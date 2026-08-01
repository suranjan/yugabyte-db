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
 * A column that exists only on the subscriber must keep its stored value when
 * an UPDATE is applied.
 *
 * The apply worker builds updatedCols from the columns the publisher actually
 * sent, excluding those with no remote counterpart, and YBCExecuteUpdate writes
 * only that set - so DocDB performs a partial update and the subscriber-only
 * column is never touched.  This holds even on the write-only fast path, where
 * the local row is not read and the column therefore arrives as NULL in the
 * slot: the NULL is filtered out of the write.
 *
 * The second case adds a secondary index, which disables the fast path
 * (YbApplyCanSkipLocalFetch) and makes the worker fetch the row.  Both must
 * agree; a difference between them would mean the fast path is writing columns
 * it was not given values for.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionSubscriberOnlyColumn
    extends BasePgSubscriptionLifecycleTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestPgSubscriptionSubscriberOnlyColumn.class);

  private static final long TIMEOUT_MS = BuildTypeUtil.adjustTimeout(120000);

  /** No secondary index, so the apply takes the write-only fast path. */
  private static final String FAST = "t_fast";
  /** Secondary index on the subscriber, so the apply fetches the local row. */
  private static final String SLOW = "t_slow";

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

  /**
   * Waits until wantReady tables of the subscription are in state 'r'.  Counting
   * the ready ones rather than the not-ready ones matters: pg_subscription_rel
   * is empty until the subscription is registered, and "nothing is unready" is
   * trivially true of an empty table.
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

  private String row(String table) throws Exception {
    return scalar(subConn, "SELECT a || '/' || coalesce(b::text, 'NULL') "
                           + "|| '/' || coalesce(c::text, 'NULL') FROM "
                           + table + " ORDER BY a LIMIT 1");
  }

  @Test
  public void testUpdatePreservesSubscriberOnlyColumn() throws Exception {
    for (String table : new String[] {FAST, SLOW}) {
      exec(pubConn, "CREATE TABLE " + table + " (a int PRIMARY KEY, b int)");
      // A YB table's replica identity defaults to CHANGE, which pgoutput
      // refuses to decode.
      exec(pubConn, "ALTER TABLE " + table + " REPLICA IDENTITY DEFAULT");
      exec(pubConn, "INSERT INTO " + table + " VALUES (1, 1)");

      // c has no counterpart on the publisher.  Its default is deliberately
      // not the value stored below, so filling the default is distinguishable
      // from preserving the row.
      exec(subConn, "CREATE TABLE " + table
                    + " (a int PRIMARY KEY, b int, c int DEFAULT 0)");
      exec(subConn, "INSERT INTO " + table + " VALUES (1, 1, 42)");
    }
    // Disables the write-only fast path for SLOW only: YbApplyCanSkipLocalFetch
    // requires the relation to have no secondary indexes.
    exec(subConn, "CREATE INDEX " + SLOW + "_b_idx ON " + SLOW + " (b)");

    exec(pubConn, "CREATE PUBLICATION " + PUB_NAME + " FOR TABLE " + FAST
                  + ", " + SLOW);
    exec(subConn, createSubscriptionSql(SUB_NAME, PUB_NAME, false));
    waitForTablesReady(2);

    for (String table : new String[] {FAST, SLOW}) {
      exec(pubConn, "UPDATE " + table + " SET b = 7 WHERE a = 1");
      TestUtils.waitFor(
          () -> "7".equals(scalar(subConn, "SELECT b::text FROM " + table
                                           + " WHERE a = 1")),
          TIMEOUT_MS, POLL_INTERVAL_MS);
      LOG.info("{} after replicated UPDATE (a/b/c): {}", table, row(table));
      assertEquals("an applied UPDATE must not touch a column the publisher "
                   + "never sent (" + table + ")",
                   "42",
                   scalar(subConn, "SELECT c::text FROM " + table
                                   + " WHERE a = 1"));
    }
  }

  /**
   * The mirror of the above: a column with no remote counterpart is left alone
   * only as long as nothing local has a value for it.  A BEFORE ROW UPDATE
   * trigger does, and its assignment must reach storage - the column is absent
   * from updatedCols, so a partial write would silently drop it.
   */
  @Test
  public void testBeforeUpdateTriggerWritesSubscriberOnlyColumn()
      throws Exception {
    final String table = "t_trig";

    exec(pubConn, "CREATE TABLE " + table + " (a int PRIMARY KEY, b int)");
    exec(pubConn, "ALTER TABLE " + table + " REPLICA IDENTITY DEFAULT");
    exec(pubConn, "INSERT INTO " + table + " VALUES (1, 1)");

    exec(subConn, "CREATE TABLE " + table
                  + " (a int PRIMARY KEY, b int, c int DEFAULT 0)");
    exec(subConn, "INSERT INTO " + table + " VALUES (1, 1, 42)");
    exec(subConn, "CREATE FUNCTION set_c() RETURNS trigger LANGUAGE plpgsql "
                  + "AS $$ BEGIN NEW.c := NEW.b * 10; RETURN NEW; END $$");
    exec(subConn, "CREATE TRIGGER trg BEFORE UPDATE ON " + table
                  + " FOR EACH ROW EXECUTE FUNCTION set_c()");
    // Apply workers run with session_replication_role = replica, where only
    // REPLICA and ALWAYS triggers fire.
    exec(subConn, "ALTER TABLE " + table + " ENABLE REPLICA TRIGGER trg");

    exec(pubConn, "CREATE PUBLICATION " + PUB_NAME + " FOR TABLE " + table);
    exec(subConn, createSubscriptionSql(SUB_NAME, PUB_NAME, false));
    waitForTablesReady(1);

    exec(pubConn, "UPDATE " + table + " SET b = 7 WHERE a = 1");
    TestUtils.waitFor(
        () -> "7".equals(scalar(subConn, "SELECT b::text FROM " + table
                                         + " WHERE a = 1")),
        TIMEOUT_MS, POLL_INTERVAL_MS);

    assertEquals("a BEFORE UPDATE trigger's assignment must be written even "
                 + "though the publisher never sent that column",
                 "70",
                 scalar(subConn, "SELECT c::text FROM " + table
                                 + " WHERE a = 1"));
  }

  /**
   * Same shape, with a stored generated column instead of a trigger:
   * ExecComputeStoredGenerated recomputes it from the new row, and that value
   * has to be written for the column to stay consistent with its expression.
   */
  @Test
  public void testStoredGeneratedColumnRecomputedOnUpdate() throws Exception {
    final String table = "t_gen";

    exec(pubConn, "CREATE TABLE " + table + " (a int PRIMARY KEY, b int)");
    exec(pubConn, "ALTER TABLE " + table + " REPLICA IDENTITY DEFAULT");
    exec(pubConn, "INSERT INTO " + table + " VALUES (1, 1)");

    exec(subConn, "CREATE TABLE " + table + " (a int PRIMARY KEY, b int, "
                  + "c int GENERATED ALWAYS AS (b * 10) STORED)");
    exec(subConn, "INSERT INTO " + table + " VALUES (1, 1)");
    assertEquals("10", scalar(subConn, "SELECT c::text FROM " + table
                                       + " WHERE a = 1"));

    exec(pubConn, "CREATE PUBLICATION " + PUB_NAME + " FOR TABLE " + table);
    exec(subConn, createSubscriptionSql(SUB_NAME, PUB_NAME, false));
    waitForTablesReady(1);

    exec(pubConn, "UPDATE " + table + " SET b = 7 WHERE a = 1");
    TestUtils.waitFor(
        () -> "7".equals(scalar(subConn, "SELECT b::text FROM " + table
                                         + " WHERE a = 1")),
        TIMEOUT_MS, POLL_INTERVAL_MS);

    assertEquals("a stored generated column must be recomputed and written "
                 + "when the column it derives from is updated",
                 "70",
                 scalar(subConn, "SELECT c::text FROM " + table
                                 + " WHERE a = 1"));
  }
}
