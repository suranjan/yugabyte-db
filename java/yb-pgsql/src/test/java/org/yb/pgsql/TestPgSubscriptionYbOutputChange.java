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
 * Applying an UPDATE that omits columns, under REPLICA IDENTITY CHANGE decoded
 * by the yboutput plugin.
 *
 * This is the only configuration in which a column can reach the apply worker
 * marked unchanged ('u') from a YB publisher.  CHANGE sends just the columns an
 * UPDATE touched; the decoder records the rest in yb_is_omitted, and
 * logicalrep_write_tuple forwards those as 'u' - but only in yboutput mode
 * (pgoutput rejects CHANGE outright).  The subscriber always creates pgoutput
 * slots, so the slot is created here by hand and attached with
 * create_slot = false.
 *
 * An omitted column must keep its stored value.  The apply worker excludes it
 * from updatedCols, so YBCExecuteUpdate should leave it out of the DocDB write.
 * The two tables differ only in a secondary index, which decides whether the
 * worker reads the local row first: without one it takes the write-only fast
 * path, where the omitted column is NULL in the slot and only that exclusion
 * keeps the stored value alive.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionYbOutputChange
    extends BasePgSubscriptionLifecycleTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestPgSubscriptionYbOutputChange.class);

  private static final long TIMEOUT_MS = BuildTypeUtil.adjustTimeout(60000);
  private static final String SLOT = "yb_change_slot";

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
        // Covers the hand-made yboutput slot as well as any pgoutput slot.
        stmt.execute("SELECT pg_drop_replication_slot(slot_name) "
                     + "FROM pg_replication_slots");
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

  private String row(String table) throws Exception {
    return scalar(subConn, "SELECT a || '/' || coalesce(b::text, 'NULL') "
                           + "|| '/' || coalesce(c::text, 'NULL') FROM "
                           + table + " WHERE a = 1");
  }

  private void logState(String when) throws Exception {
    LOG.info("[{}] subenabled={} state={} fast={} slow={}", when,
             scalar(subConn, "SELECT subenabled::text FROM pg_subscription "
                             + "WHERE subname = '" + SUB_NAME + "'"),
             scalar(subConn, "SELECT string_agg(srsubstate, ',') "
                             + "FROM pg_subscription_rel sr "
                             + "JOIN pg_subscription s ON sr.srsubid = s.oid "
                             + "WHERE s.subname = '" + SUB_NAME + "'"),
             row(FAST), row(SLOW));
    LOG.info("[{}] apply_error_count={} sync_error_count={}", when,
             scalar(subConn, "SELECT apply_error_count::text "
                             + "FROM pg_stat_subscription_stats "
                             + "WHERE subname = '" + SUB_NAME + "'"),
             scalar(subConn, "SELECT sync_error_count::text "
                             + "FROM pg_stat_subscription_stats "
                             + "WHERE subname = '" + SUB_NAME + "'"));
  }

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
  public void testOmittedColumnKeepsStoredValue() throws Exception {
    for (String table : new String[] {FAST, SLOW}) {
      exec(pubConn, "CREATE TABLE " + table
                    + " (a int PRIMARY KEY, b int, c int)");
      // CHANGE is the YB default; set it explicitly so the test states what it
      // exercises.  Under it an UPDATE sends only the columns it touched.
      exec(pubConn, "ALTER TABLE " + table + " REPLICA IDENTITY CHANGE");
      assertEquals("publisher table must be under REPLICA IDENTITY CHANGE", "c",
                   scalar(pubConn, "SELECT relreplident::text FROM pg_class "
                                   + "WHERE oid = '" + table + "'::regclass"));
      exec(pubConn, "INSERT INTO " + table + " VALUES (1, 1, 42)");

      exec(subConn, "CREATE TABLE " + table
                    + " (a int PRIMARY KEY, b int, c int)");
      // Deliberately diverges from the publisher's 42, which makes the outcome
      // a three-way discriminator: 99 means c was omitted and left out of the
      // write, 42 means it was sent after all, NULL means it arrived as a NULL
      // rather than as an omission.
      exec(subConn, "INSERT INTO " + table + " VALUES (1, 1, 99)");
    }
    // Disables the write-only fast path for SLOW only: YbApplyCanSkipLocalFetch
    // requires the relation to have no secondary indexes.
    exec(subConn, "CREATE INDEX " + SLOW + "_b_idx ON " + SLOW + " (b)");

    exec(pubConn, "CREATE PUBLICATION " + PUB_NAME + " FOR TABLE " + FAST
                  + ", " + SLOW);

    // The subscriber hardcodes "LOGICAL pgoutput" when it creates a slot
    // (libpqwalreceiver.c), and pgoutput refuses to decode CHANGE, so make the
    // slot here and attach to it.
    LOG.info("created slot: {}",
             scalar(pubConn, "SELECT slot_name FROM "
                             + "pg_create_logical_replication_slot('" + SLOT
                             + "', 'yboutput')"));
    assertEquals("the slot must be using the yboutput plugin", "yboutput",
                 scalar(pubConn, "SELECT plugin FROM pg_replication_slots "
                                 + "WHERE slot_name = '" + SLOT + "'"));

    exec(subConn, String.format(
        "CREATE SUBSCRIPTION %s CONNECTION 'host=%s port=%d dbname=%s user=%s' "
        + "PUBLICATION %s WITH (copy_data = false, create_slot = false, "
        + "slot_name = '%s')",
        SUB_NAME, getPgHost(0), getPgPort(0), PUB_DB, TEST_PG_USER, PUB_NAME,
        SLOT));

    try {
      waitForTablesReady(2);
    } catch (Exception e) {
      logState("tablesync never completed");
      throw e;
    }

    // Touches b only, so c is omitted from the message rather than sent.
    for (String table : new String[] {FAST, SLOW}) {
      exec(pubConn, "UPDATE " + table + " SET b = 7 WHERE a = 1");
    }

    try {
      for (String table : new String[] {FAST, SLOW}) {
        TestUtils.waitFor(
            () -> "7".equals(scalar(subConn, "SELECT b::text FROM " + table
                                             + " WHERE a = 1")),
            TIMEOUT_MS, POLL_INTERVAL_MS);
      }
    } catch (Exception e) {
      logState("UPDATE never applied");
      throw e;
    }

    logState("after UPDATE");
    for (String table : new String[] {FAST, SLOW}) {
      assertEquals("a column omitted by REPLICA IDENTITY CHANGE must keep its "
                   + "stored value (" + table + ")",
                   "99",
                   scalar(subConn, "SELECT c::text FROM " + table
                                   + " WHERE a = 1"));
    }
  }
}
