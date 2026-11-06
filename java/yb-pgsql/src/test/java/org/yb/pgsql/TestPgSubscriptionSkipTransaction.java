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
import static org.yb.AssertionWrappers.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Tests ALTER SUBSCRIPTION ... SKIP (lsn = ...) end to end.
 *
 * The rest of the suite covers SKIP only as DDL - regress/sql/subscription.sql
 * sets subskiplsn to a made-up value and reads it back, which says nothing about
 * whether a transaction is actually skipped.  This test makes a replicated
 * transaction fail on the subscriber, skips it by the LSN the server reported,
 * and then checks the data.
 *
 * The skipped transaction carries a second row that conflicts with nothing.  If
 * SKIP discarded only the offending change, that row would still land; requiring
 * it to be absent is what makes this a test of skipping a transaction rather than
 * of swallowing an error.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionSkipTransaction
    extends BasePgSubscriptionLifecycleTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestPgSubscriptionSkipTransaction.class);

  private static final long TIMEOUT_MS = BuildTypeUtil.adjustTimeout(120000);
  private static final String TABLE = "skiptest";

  /**
   * The finish LSN of a failed transaction is only reported in the apply
   * worker's error CONTEXT, so the test has to read the server log - the same
   * thing an operator does before choosing an LSN to skip.
   */
  private static final Pattern FINISH_LSN = Pattern.compile(
      "for replication target relation \"public\\." + TABLE
      + "\" in transaction \\d+, finished at ([0-9A-Fa-f]+/[0-9A-Fa-f]+)");

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

  @Override
  protected Map<String, String> getTServerFlags() {
    Map<String, String> flagMap = super.getTServerFlags();
    // The finish LSN has to be readable from a log file.
    appendToYsqlPgConf(flagMap, "logging_collector=on");
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

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

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

  /** Absolute path of the log the subscriber node is writing to. */
  private Path subscriberLogFile() throws Exception {
    String rel = scalar(subConn, "SELECT pg_current_logfile()");
    assertNotNull("logging_collector must be on for this test to read the "
                  + "apply worker's error CONTEXT", rel);
    Path path = Paths.get(rel);
    if (!path.isAbsolute()) {
      path = Paths.get(scalar(subConn, "SHOW data_directory")).resolve(rel);
    }
    return path;
  }

  private static long fileLength(Path path) throws Exception {
    return Files.exists(path) ? Files.size(path) : 0;
  }

  /** Everything written to path at or after fromOffset. */
  private static String logSince(Path path, long fromOffset) throws Exception {
    if (!Files.exists(path)) {
      return "";
    }
    byte[] all = Files.readAllBytes(path);
    if (fromOffset >= all.length) {
      return "";
    }
    return new String(all, (int) fromOffset, (int) (all.length - fromOffset),
                      StandardCharsets.UTF_8);
  }

  private String subscriptionColumn(String column) throws Exception {
    return scalar(subConn, "SELECT " + column + "::text FROM pg_subscription "
                           + "WHERE subname = '" + SUB_NAME + "'");
  }

  private void waitForSubscriptionColumn(String column, String want)
      throws Exception {
    TestUtils.waitFor(() -> want.equals(subscriptionColumn(column)),
                      TIMEOUT_MS, POLL_INTERVAL_MS);
  }

  private void waitForAllTablesReady() throws Exception {
    TestUtils.waitFor(() -> {
      try (Statement stmt = subConn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT count(*) FROM pg_subscription_rel sr "
               + "JOIN pg_subscription s ON sr.srsubid = s.oid "
               + "WHERE s.subname = '" + SUB_NAME + "' "
               + "AND sr.srsubstate != 'r'")) {
        return rs.next() && rs.getInt(1) == 0;
      }
    }, TIMEOUT_MS, POLL_INTERVAL_MS);
  }

  // ---------------------------------------------------------------------------
  //  Test
  // ---------------------------------------------------------------------------

  @Test
  public void testSkipConflictingTransaction() throws Exception {
    exec(pubConn, "CREATE TABLE " + TABLE + " (i int PRIMARY KEY, t text)");
    // A YB table's replica identity defaults to CHANGE, which pgoutput refuses
    // to decode; without this the subscription fails on the very first message
    // and never reaches the conflict this test is about.
    exec(pubConn, "ALTER TABLE " + TABLE + " REPLICA IDENTITY DEFAULT");
    exec(subConn, "CREATE TABLE " + TABLE + " (i int PRIMARY KEY, t text)");
    exec(pubConn, "CREATE PUBLICATION " + PUB_NAME + " FOR TABLE " + TABLE);

    // disable_on_error so the failed transaction stops the worker cleanly
    // instead of being retried forever, which is what makes SKIP usable.
    exec(subConn, String.format(
        "CREATE SUBSCRIPTION %s CONNECTION 'host=%s port=%d dbname=%s user=%s' "
        + "PUBLICATION %s WITH (copy_data = false, disable_on_error = true)",
        SUB_NAME, getPgHost(0), getPgPort(0), PUB_DB, TEST_PG_USER, PUB_NAME));
    waitForAllTablesReady();

    // Seed the conflict after sync, so it is the apply path that fails and not
    // the initial copy.
    exec(subConn, "INSERT INTO " + TABLE + " VALUES (1, 'local')");

    Path log = subscriberLogFile();
    long offset = fileLength(log);
    LOG.info("Reading subscriber log {} from offset {}", log, offset);

    exec(pubConn, "BEGIN");
    exec(pubConn, "INSERT INTO " + TABLE + " VALUES (1, 'from-pub')");
    exec(pubConn, "INSERT INTO " + TABLE + " VALUES (2, 'collateral')");
    exec(pubConn, "COMMIT");

    // Wait for the reported finish LSN rather than for the subscription to go
    // disabled: any apply error disables it, so waiting on subenabled alone
    // would pass on an unrelated failure and then time out looking for an LSN
    // that was never going to appear.
    LOG.info("Waiting for the apply worker to report the failed transaction");
    TestUtils.waitFor(() -> FINISH_LSN.matcher(logSince(log, offset)).find(),
                      TIMEOUT_MS, POLL_INTERVAL_MS);
    waitForSubscriptionColumn("subenabled", "false");

    // Take the most recent one: the worker may have retried before
    // disable_on_error took effect.
    String lsn = null;
    Matcher m = FINISH_LSN.matcher(logSince(log, offset));
    while (m.find()) {
      lsn = m.group(1);
    }
    assertNotNull("apply worker did not report a finish LSN for the failed "
                  + "transaction", lsn);
    LOG.info("Skipping transaction that finished at {}", lsn);

    exec(subConn,
         "ALTER SUBSCRIPTION " + SUB_NAME + " SKIP (lsn = '" + lsn + "')");
    assertEquals("subskiplsn should hold the LSN we asked to skip", lsn,
                 subscriptionColumn("subskiplsn"));

    exec(subConn, "ALTER SUBSCRIPTION " + SUB_NAME + " ENABLE");

    // The worker clears subskiplsn once it has skipped that transaction.
    LOG.info("Waiting for the worker to clear subskiplsn");
    waitForSubscriptionColumn("subskiplsn", "0/0");

    // Replication must resume after the skip.
    exec(pubConn, "INSERT INTO " + TABLE + " VALUES (3, 'after-skip')");
    TestUtils.waitFor(
        () -> "1".equals(scalar(subConn, "SELECT count(*) FROM " + TABLE
                                         + " WHERE i = 3")),
        TIMEOUT_MS, POLL_INTERVAL_MS);

    assertEquals("the conflicting row must keep the subscriber's value",
                 "local", scalar(subConn,
                                 "SELECT t FROM " + TABLE + " WHERE i = 1"));
    assertEquals("the skipped transaction's other row must not be applied "
                 + "either - SKIP discards the whole transaction",
                 "0", scalar(subConn, "SELECT count(*) FROM " + TABLE
                                      + " WHERE i = 2"));
    assertEquals("a transaction after the skipped one must replicate",
                 "after-skip",
                 scalar(subConn, "SELECT t FROM " + TABLE + " WHERE i = 3"));
    assertEquals("the subscription must still be enabled", "true",
                 subscriptionColumn("subenabled"));

    LOG.info("publisher:  {}",
             scalar(pubConn, "SELECT string_agg(i || '=' || t, ' ' ORDER BY i) "
                             + "FROM " + TABLE));
    LOG.info("subscriber: {}",
             scalar(subConn, "SELECT string_agg(i || '=' || t, ' ' ORDER BY i) "
                             + "FROM " + TABLE));
  }
}
