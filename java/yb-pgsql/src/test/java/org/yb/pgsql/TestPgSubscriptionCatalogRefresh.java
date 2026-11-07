// Copyright (c) YugabyteDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//
package org.yb.pgsql;

import static org.yb.AssertionWrappers.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;
import org.yb.client.TestUtils;
import org.yb.util.BuildTypeUtil;

/**
 * An apply worker must see DDL that another session ran on the subscriber.
 *
 * With object locking off, a backend picks up a catalog version bump only in
 * PostgresMain's per-statement check, which a background worker never runs, so
 * the apply worker kept relation entries from before the DDL.  A replicated
 * change then wrote to a table the DDL had replaced, failed, and restarted the
 * worker; a replicated TRUNCATE (from a PostgreSQL publisher) instead hung on a
 * DocDB table the rewrite had already dropped.  Object locking is forced off
 * here so release builds, where it defaults on, still cover that mode.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionCatalogRefresh extends BasePgSubscriptionLifecycleTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestPgSubscriptionCatalogRefresh.class);

  private static final long APPLY_TIMEOUT_MS = BuildTypeUtil.adjustTimeout(120000);

  private Connection pubConn;
  private Connection subConn;

  @Override
  protected int getInitialNumTServers() {
    return 1;
  }

  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  // The validators tie these together on the master and the tserver.
  private static void disableObjectLocking(Map<String, String> flagMap) {
    flagMap.put("enable_object_locking_for_table_locks", "false");
    flagMap.put("ysql_enable_concurrent_ddl", "false");
    flagMap.put("ysql_yb_ddl_transaction_block_enabled", "false");
    flagMap.put("ysql_yb_enable_ddl_savepoint_support", "false");
  }

  @Override
  protected Map<String, String> getTServerFlags() {
    Map<String, String> flagMap = super.getTServerFlags();
    disableObjectLocking(flagMap);
    return flagMap;
  }

  @Override
  protected Map<String, String> getMasterFlags() {
    Map<String, String> flagMap = super.getMasterFlags();
    disableObjectLocking(flagMap);
    return flagMap;
  }

  @Before
  public void initPubSubDatabases() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE DATABASE " + PUB_DB);
      stmt.execute("CREATE DATABASE " + SUB_DB);
    }
    pubConn = getConnectionBuilder().withDatabase(PUB_DB).connect();
    subConn = getConnectionBuilder().withDatabase(SUB_DB).connect();
  }

  @After
  public void tearDownSubscription() throws Exception {
    try (Statement sub = subConn.createStatement()) {
      sub.execute("ALTER SUBSCRIPTION " + SUB_NAME + " DISABLE");
      sub.execute("ALTER SUBSCRIPTION " + SUB_NAME + " SET (slot_name = NONE)");
      sub.execute("DROP SUBSCRIPTION IF EXISTS " + SUB_NAME);
    } catch (Exception e) {
      LOG.warn("Failed to drop subscription during cleanup", e);
    }
    try (Statement pub = pubConn.createStatement()) {
      pub.execute("SELECT pg_drop_replication_slot(slot_name) "
                  + "FROM pg_replication_slots WHERE plugin = 'pgoutput'");
    } catch (Exception e) {
      LOG.warn("Failed to drop replication slots during cleanup", e);
    }
    pubConn.close();
    subConn.close();
  }

  /** A subscriber-local TRUNCATE rewrites the table under a new DocDB id. */
  @Test
  public void testApplyAfterLocalTableRewrite() throws Exception {
    checkApplyAfterLocalDdl("TRUNCATE t");
  }

  /** ADD COLUMN keeps the DocDB table but bumps its schema version. */
  @Test
  public void testApplyAfterLocalAlterTable() throws Exception {
    checkApplyAfterLocalDdl("ALTER TABLE t ADD COLUMN extra int");
  }

  private void checkApplyAfterLocalDdl(String localDdl) throws Exception {
    try (Statement pub = pubConn.createStatement();
         Statement sub = subConn.createStatement()) {
      pub.execute("CREATE TABLE t (k int PRIMARY KEY, v text)");
      sub.execute("CREATE TABLE t (k int PRIMARY KEY, v text)");
      // YB tables default to CHANGE, which pgoutput refuses to stream.
      pub.execute("ALTER TABLE t REPLICA IDENTITY DEFAULT");
      pub.execute("CREATE PUBLICATION " + PUB_NAME + " FOR TABLE t");
      sub.execute(createSubscriptionSql(SUB_NAME, PUB_NAME, false /* copyData */));
    }

    // Apply one row first, so the worker has the table open and cached.
    insertOnPublisherAndWait(1);
    int pid = applyWorkerPid(subConn, SUB_NAME);
    long errorsBefore = applyErrorCount();

    try (Statement sub = subConn.createStatement()) {
      sub.execute(localDdl);
    }
    // Let the tserver learn the new catalog version, so the worker's next
    // transaction can see that it moved.
    waitForTServerHeartbeat();

    insertOnPublisherAndWait(2);
    assertEquals("the apply worker must not error on its stale catalog",
                 errorsBefore, applyErrorCount());
    assertEquals("the apply worker must not have restarted",
                 pid, applyWorkerPid(subConn, SUB_NAME));
  }

  private void insertOnPublisherAndWait(int k) throws Exception {
    try (Statement pub = pubConn.createStatement()) {
      pub.execute("INSERT INTO t (k, v) VALUES (" + k + ", 'v" + k + "')");
    }
    TestUtils.waitFor(() -> {
      try (Statement sub = subConn.createStatement();
           ResultSet rs = sub.executeQuery("SELECT count(*) FROM t WHERE k = " + k)) {
        return rs.next() && rs.getInt(1) == 1;
      }
    }, APPLY_TIMEOUT_MS, POLL_INTERVAL_MS);
  }

  private long applyErrorCount() throws Exception {
    try (Statement sub = subConn.createStatement();
         ResultSet rs = sub.executeQuery(
             "SELECT apply_error_count FROM pg_stat_subscription_stats s "
             + "JOIN pg_subscription p ON p.oid = s.subid "
             + "WHERE p.subname = '" + SUB_NAME + "'")) {
      return rs.next() ? rs.getLong(1) : 0;
    }
  }
}
