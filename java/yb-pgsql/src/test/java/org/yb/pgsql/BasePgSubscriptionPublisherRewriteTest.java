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

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.TestUtils;
import org.yb.util.BuildTypeUtil;

/**
 * A YugabyteDB publisher (PUB_DB) and subscriber (SUB_DB) on one single-node
 * cluster, replicating table t (k int PRIMARY KEY, v text), for checking that
 * apply survives a table rewrite on the publisher.
 */
public abstract class BasePgSubscriptionPublisherRewriteTest
    extends BasePgSubscriptionLifecycleTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(BasePgSubscriptionPublisherRewriteTest.class);

  protected static final long APPLY_TIMEOUT_MS = BuildTypeUtil.adjustTimeout(120000);

  protected Connection pubConn;
  protected Connection subConn;

  @Override
  protected int getInitialNumTServers() {
    return 1;
  }

  @Override
  protected int getReplicationFactor() {
    return 1;
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

  /** Creates t on both sides and subscribes to it, without an initial copy. */
  protected void createReplicatedTable(String... pubSetup) throws Exception {
    try (Statement pub = pubConn.createStatement();
         Statement sub = subConn.createStatement()) {
      pub.execute("CREATE TABLE t (k int PRIMARY KEY, v text)");
      sub.execute("CREATE TABLE t (k int PRIMARY KEY, v text)");
      for (String sql : pubSetup) {
        pub.execute(sql);
      }
      pub.execute("CREATE PUBLICATION " + PUB_NAME + " FOR TABLE t");
      sub.execute(createSubscriptionSql(SUB_NAME, PUB_NAME, false /* copyData */));
    }
  }

  protected static void execute(Connection conn, String... sqls) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      for (String sql : sqls) {
        stmt.execute(sql);
      }
    }
  }

  protected void insertOnPublisherAndWait(int k) throws Exception {
    execute(pubConn, "INSERT INTO t (k, v) VALUES (" + k + ", 'v" + k + "')");
    TestUtils.waitFor(() -> subCount("k = " + k) == 1, APPLY_TIMEOUT_MS, POLL_INTERVAL_MS);
  }

  /**
   * Inserts row k on the publisher and asserts it reaches the subscriber
   * without an apply error.  Stops at the first error rather than waiting out
   * the timeout: the worker retries a failing transaction forever.
   */
  protected void assertInsertApplies(int k, long errorsBefore) throws Exception {
    execute(pubConn, "INSERT INTO t (k, v) VALUES (" + k + ", 'v" + k + "')");
    TestUtils.waitFor(
        () -> subCount("k = " + k) == 1 || applyErrorCount() > errorsBefore,
        APPLY_TIMEOUT_MS, POLL_INTERVAL_MS);
    assertEquals("the subscriber must not fail applying after a publisher-side rewrite",
                 errorsBefore, applyErrorCount());
  }

  protected int subCount(String where) throws Exception {
    try (Statement sub = subConn.createStatement();
         ResultSet rs = sub.executeQuery("SELECT count(*) FROM t WHERE " + where)) {
      return rs.next() ? rs.getInt(1) : 0;
    }
  }

  protected long applyErrorCount() throws Exception {
    try (Statement sub = subConn.createStatement();
         ResultSet rs = sub.executeQuery(
             "SELECT apply_error_count FROM pg_stat_subscription_stats s "
             + "JOIN pg_subscription p ON p.oid = s.subid "
             + "WHERE p.subname = '" + SUB_NAME + "'")) {
      return rs.next() ? rs.getLong(1) : 0;
    }
  }
}
