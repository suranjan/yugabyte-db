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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;
import org.yb.client.TestUtils;
import org.yb.util.BuildTypeUtil;

/**
 * Applying an UPDATE to a partitioned subscriber table whose partition does not
 * share the root's attribute numbers.
 *
 * A partition that has had a column dropped keeps a gap in its attribute
 * numbers, so its live columns sit at higher numbers than the root's.  The
 * apply worker builds the set of columns to write against the root, and the
 * partition it routes the row to has no range table entry of its own, so that
 * set has to be translated before it can be used against the partition.  When
 * it is not, YBCExecuteUpdate leaves the untranslated columns out of the write
 * and they silently keep their old values - no error, just stale data on the
 * subscriber.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionPartitionUpdate
    extends BasePgSubscriptionLifecycleTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestPgSubscriptionPartitionUpdate.class);

  private static final long TIMEOUT_MS = BuildTypeUtil.adjustTimeout(120000);
  private static final String TABLE = "parttest";
  private static final String LEAF = TABLE + "_1";

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
    if (subConn != null && !subConn.isClosed()) {
      try (Statement stmt = subConn.createStatement()) {
        stmt.execute("ALTER SUBSCRIPTION " + SUB_NAME + " DISABLE");
        stmt.execute(
            "ALTER SUBSCRIPTION " + SUB_NAME + " SET (slot_name = NONE)");
        stmt.execute("DROP SUBSCRIPTION IF EXISTS " + SUB_NAME);
      } catch (Exception e) {
        LOG.warn("Failed to drop subscription during cleanup", e);
      }
    }
    if (pubConn != null && !pubConn.isClosed()) {
      try (Statement stmt = pubConn.createStatement()) {
        stmt.execute(
            "SELECT pg_drop_replication_slot(slot_name) "
            + "FROM pg_replication_slots WHERE plugin = 'pgoutput'");
      } catch (Exception e) {
        LOG.warn("Failed to drop replication slots during cleanup", e);
      }
    }
  }

  private static String scalar(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      return rs.next() ? rs.getString(1) : null;
    }
  }

  private static void exec(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  /** Live columns of relation, as "name=attnum" ordered by attnum. */
  private String attnums(String relation) throws Exception {
    return scalar(subConn,
                  "SELECT string_agg(attname || '=' || attnum, ',' "
                  + "ORDER BY attnum) FROM pg_attribute "
                  + "WHERE attrelid = '" + relation + "'::regclass "
                  + "AND attnum > 0 AND NOT attisdropped");
  }

  /**
   * Waits until wantReady tables of the subscription are in state 'r'.  Counting
   * the ready ones rather than the not-ready ones matters: pg_subscription_rel is
   * empty until the subscription is registered, and "nothing is unready" is
   * trivially true of an empty table.  Applying DML before the table is ready
   * gets it skipped as already covered by the copy, so a test that starts too
   * early sees nothing replicate at all.
   */
  private void waitForTablesReady(int wantReady) throws Exception {
    TestUtils.waitFor(() -> {
      try (Statement stmt = subConn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT count(*) FROM pg_subscription_rel sr "
               + "JOIN pg_subscription s ON sr.srsubid = s.oid "
               + "WHERE s.subname = '" + SUB_NAME + "' "
               + "AND sr.srsubstate = 'r'")) {
        return rs.next() && rs.getInt(1) == wantReady;
      }
    }, TIMEOUT_MS, POLL_INTERVAL_MS);
  }

  @Test
  public void testUpdateIntoPartitionWithDroppedColumn() throws Exception {
    exec(pubConn, "CREATE TABLE " + TABLE + " (a int PRIMARY KEY, b int, c int)");
    // A YB table's replica identity defaults to CHANGE, which pgoutput refuses
    // to decode, so the subscription would never apply anything.
    exec(pubConn, "ALTER TABLE " + TABLE + " REPLICA IDENTITY DEFAULT");
    exec(pubConn, "INSERT INTO " + TABLE + " VALUES (1, 1, 1)");
    exec(pubConn, "CREATE PUBLICATION " + PUB_NAME + " FOR TABLE " + TABLE);

    exec(subConn, "CREATE TABLE " + TABLE
                  + " (a int NOT NULL, b int, c int, PRIMARY KEY (a)) "
                  + "PARTITION BY LIST (a)");
    exec(subConn, "CREATE TABLE " + LEAF + " PARTITION OF " + TABLE
                  + " DEFAULT");

    // Push the partition's c past the root's highest attribute number, while
    // leaving the visible column order identical to the root's.  Detaching
    // first is what allows the partition alone to be altered.
    exec(subConn, "ALTER TABLE " + TABLE + " DETACH PARTITION " + LEAF);
    exec(subConn, "ALTER TABLE " + LEAF + " DROP COLUMN c");
    exec(subConn, "ALTER TABLE " + LEAF + " ADD COLUMN c int");
    exec(subConn, "ALTER TABLE " + TABLE + " ATTACH PARTITION " + LEAF
                  + " DEFAULT");

    String rootAttnums = attnums(TABLE);
    String leafAttnums = attnums(LEAF);
    LOG.info("root attnums: {}", rootAttnums);
    LOG.info("leaf attnums: {}", leafAttnums);
    assertEquals("the root should be unaffected by the partition's DDL",
                 "a=1,b=2,c=3", rootAttnums);
    assertEquals("this test is only meaningful while the partition's c sits "
                 + "past the root's highest attribute number",
                 "a=1,b=2,c=4", leafAttnums);

    // Seed the row the UPDATE will target instead of copying it: copy_data
    // leaves the table in tablesync state 's' against a YB publisher, and DML
    // arriving in that state is skipped as already covered by the copy, so the
    // UPDATE under test would never be applied.
    exec(subConn, "INSERT INTO " + TABLE + " VALUES (1, 1, 1)");
    exec(subConn, createSubscriptionSql(SUB_NAME, PUB_NAME, false));
    waitForTablesReady(1);

    // One UPDATE touching a column on either side of the root's last attribute
    // number: b lands at the same number in both, c does not.
    exec(pubConn, "UPDATE " + TABLE + " SET b = 7, c = 9 WHERE a = 1");

    // b and c travel in the same UPDATE, so once b has arrived the write that
    // carried c has been applied too - waiting on b and then asserting c fails
    // fast with the actual value instead of timing out.
    TestUtils.waitFor(
        () -> "7".equals(scalar(subConn, "SELECT b::text FROM " + TABLE
                                         + " WHERE a = 1")),
        TIMEOUT_MS, POLL_INTERVAL_MS);

    assertEquals("a replicated UPDATE must apply columns whose attribute "
                 + "number differs between the partition and its root",
                 "9",
                 scalar(subConn, "SELECT c::text FROM " + TABLE + " WHERE a = 1"));

    // Setting a column to NULL is the same write path, and the case
    // 013_partition covers: the new value has to overwrite, not be skipped.
    exec(pubConn, "UPDATE " + TABLE + " SET b = 8, c = NULL WHERE a = 1");
    TestUtils.waitFor(
        () -> "8".equals(scalar(subConn, "SELECT b::text FROM " + TABLE
                                         + " WHERE a = 1")),
        TIMEOUT_MS, POLL_INTERVAL_MS);
    assertEquals("a replicated UPDATE must be able to set such a column to NULL",
                 null,
                 scalar(subConn, "SELECT c::text FROM " + TABLE + " WHERE a = 1"));
  }
}
