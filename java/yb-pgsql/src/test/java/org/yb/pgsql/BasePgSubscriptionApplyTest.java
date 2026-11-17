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
import static org.yb.AssertionWrappers.assertTrue;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.TestUtils;
import org.yb.minicluster.LogErrorListener;
import org.yb.minicluster.MiniYBDaemon;
import org.yb.util.BuildTypeUtil;

/**
 * Harness shared by the tests that exercise the apply worker's DML path: a
 * publisher database and a subscriber database on one single-node cluster, the
 * publication and subscription between them, and the polling helpers the
 * assertions need.
 *
 * The cluster is recreated for every test method, which caps how many tests one
 * class can hold inside the per-class fork timeout; hence several small classes
 * over this base.
 */
public abstract class BasePgSubscriptionApplyTest extends BasePgSubscriptionLifecycleTest {
  private static final Logger LOG = LoggerFactory.getLogger(BasePgSubscriptionApplyTest.class);

  protected static final long TIMEOUT_MS = BuildTypeUtil.adjustTimeout(120000);

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

  // ---------------------------------------------------------------------------
  //  Setup / Teardown helpers
  // ---------------------------------------------------------------------------

  @Before
  public void initPubSubDatabases() throws Exception {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("CREATE DATABASE " + PUB_DB);
      stmt.execute("CREATE DATABASE " + SUB_DB);
    }
    pubConn = getConnectionBuilder().withDatabase(PUB_DB).connect();
    subConn = getConnectionBuilder().withDatabase(SUB_DB).connect();
  }

  /**
   * Stops the workers and frees the slots.  Nothing else needs cleaning up: the
   * cluster is recreated for the next test method.
   */
  @After
  public void tearDownSubscription() throws Exception {
    if (pubConn == null || subConn == null) {
      return;
    }
    try {
      exec(subConn, "ALTER SUBSCRIPTION " + SUB_NAME + " DISABLE");
      exec(subConn, "ALTER SUBSCRIPTION " + SUB_NAME + " SET (slot_name = NONE)");
      exec(subConn, "DROP SUBSCRIPTION " + SUB_NAME);
    } catch (Exception e) {
      LOG.warn("Failed to drop subscription during cleanup", e);
    }
    try {
      // Every slot, so that one a test made by hand goes too.
      exec(pubConn, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots");
    } catch (Exception e) {
      LOG.warn("Failed to drop replication slots during cleanup", e);
    }
    pubConn.close();
    subConn.close();
  }

  // ---------------------------------------------------------------------------
  //  Reusable table-creation helpers
  // ---------------------------------------------------------------------------

  /** Functional interface for creating a table on a given connection. */
  @FunctionalInterface
  protected interface TableCreator {
    void create(Statement stmt) throws Exception;
  }

  /**
   * Creates identical tables on both publisher and subscriber, then wires up
   * publication + subscription.
   *
   * @param tableName  unqualified table name
   * @param creator    callback that issues CREATE TABLE (and optional CREATE
   *                   INDEX) on the given Statement
   */
  protected void createReplicatedTable(String tableName, TableCreator creator) throws Exception {
    // YB defaults to REPLICA IDENTITY CHANGE which pgoutput doesn't support.
    // Use FULL so the publisher sends all columns in old tuples for
    // UPDATEs/DELETEs, avoiding replica identity column mismatch issues.
    createReplicatedTable(tableName, "FULL", creator);
  }

  /**
   * As above, but with an explicit REPLICA IDENTITY. Under DEFAULT the
   * publisher omits the old tuple for an UPDATE that leaves the key alone, so
   * the apply worker has only the new tuple and the changed-column bitmap to
   * work from.
   */
  protected void createReplicatedTable(String tableName, String replicaIdentity,
                                     TableCreator creator) throws Exception {
    try (Statement pub = pubConn.createStatement();
         Statement sub = subConn.createStatement()) {
      creator.create(pub);
      creator.create(sub);
      pub.execute("ALTER TABLE " + tableName + " REPLICA IDENTITY " + replicaIdentity);
      sub.execute("ALTER TABLE " + tableName + " REPLICA IDENTITY " + replicaIdentity);
    }
    createPubSub(tableName);
  }

  /**
   * Wires up the publication and subscription over tables that already exist.
   * Split out of createReplicatedTable for the tests that need the two sides to
   * differ - a different replica identity, or different rows.
   */
  protected void createPubSub(String... tables) throws Exception {
    createPubSubWith("copy_data = false", tables);
  }

  /** As above, with a caller-supplied WITH (...) option list for the subscription. */
  protected void createPubSubWith(String withOptions, String... tables) throws Exception {
    exec(pubConn, "CREATE PUBLICATION " + PUB_NAME + " FOR TABLE " + String.join(", ", tables));
    exec(subConn, createSubscriptionSql(SUB_NAME, PUB_NAME, withOptions));
    waitForTablesReady(tables.length);
  }

  /**
   * Waits until wantReady tables of the subscription are in state 'r'.  Counting
   * the ready ones rather than the not-ready ones matters: pg_subscription_rel
   * is empty until the subscription is registered, and "nothing is unready" is
   * trivially true of an empty table.  DML applied before a table is ready is
   * skipped as already covered by the copy.
   */
  protected void waitForTablesReady(int wantReady) throws Exception {
    waitForSubscriberValue(
        "SELECT count(*) FROM pg_subscription_rel sr "
        + "JOIN pg_subscription s ON sr.srsubid = s.oid "
        + "WHERE s.subname = '" + SUB_NAME + "' AND sr.srsubstate = 'r'",
        String.valueOf(wantReady));
  }

  // ---------------------------------------------------------------------------
  //  Assertion / polling helpers
  // ---------------------------------------------------------------------------

  /** A single row represented as a list of column-value strings. */
  protected static List<String> row(String... cols) {
    return Arrays.asList(cols);
  }

  /**
   * Waits until a query on the subscriber returns exactly the expected rows
   * (order-insensitive).
   */
  protected void waitForRows(String query, List<List<String>> expectedRows) throws Exception {
    List<List<String>> sortedExpected = sortRows(expectedRows);
    TestUtils.waitFor(() -> {
      List<List<String>> actual = fetchRows(subConn, query);
      return sortRows(actual).equals(sortedExpected);
    }, TIMEOUT_MS, POLL_INTERVAL_MS);

    List<List<String>> actual = fetchRows(subConn, query);
    assertEquals("Rows on subscriber do not match expected",
                 sortedExpected, sortRows(actual));
  }

  /** Waits until tableName has expectedCount rows on the subscriber. */
  protected void waitForRowCount(String tableName, int expectedCount) throws Exception {
    waitForSubscriberValue("SELECT count(*) FROM " + tableName, String.valueOf(expectedCount));
  }

  /**
   * Waits until a single-value query on the subscriber returns want, as text;
   * null stands for a NULL or for no row.
   */
  protected void waitForSubscriberValue(String query, String want) throws Exception {
    TestUtils.waitFor(() -> Objects.equals(want, scalar(subConn, query)),
                      TIMEOUT_MS, POLL_INTERVAL_MS);
  }

  /**
   * Collect apply worker log lines containing 'needle'.
   *
   * The apply worker logs to the tserver's stderr, which the harness is already
   * tailing, so attach to that rather than locating the log file.  Registered
   * per test because the cluster is recreated for each one.
   */
  protected List<String> collectPgLogLines(String needle) throws Exception {
    List<String> matched = Collections.synchronizedList(new ArrayList<>());

    for (MiniYBDaemon tserver : miniCluster.getTabletServers().values()) {
      tserver.getLogPrinter().addErrorListener(new LogErrorListener() {
        @Override
        public void handleLine(String line) {
          if (line.contains(needle))
            matched.add(line);
        }

        @Override
        public void reportErrorsAtEnd() {
          // Nothing: the lines are the result, not a failure.
        }
      });
    }
    return matched;
  }

  /** Waits until a line collected by collectPgLogLines shows up. */
  protected void waitForLogLine(List<String> collected, String what) throws Exception {
    TestUtils.waitFor(() -> !collected.isEmpty(),
                      TIMEOUT_MS, POLL_INTERVAL_MS);
    assertTrue("Expected " + what + " in the apply worker log", !collected.isEmpty());
  }

  /**
   * Wait until the subscription has recorded at least one apply failure.
   *
   * The worker retries a failing transaction forever, so the count only has to
   * become non-zero; it keeps climbing afterwards.
   */
  protected void waitForApplyError() throws Exception {
    TestUtils.waitFor(() -> applyErrorCount() > 0, TIMEOUT_MS, POLL_INTERVAL_MS);
  }

  /** Apply failures the subscription has recorded so far. */
  protected long applyErrorCount() throws Exception {
    String count = scalar(subConn, "SELECT apply_error_count FROM pg_stat_subscription_stats "
                                   + "WHERE subname = '" + SUB_NAME + "'");
    return count == null ? 0 : Long.parseLong(count);
  }

  /** Fetches all rows from a query as lists of strings. */
  protected List<List<String>> fetchRows(Connection conn, String query) throws Exception {
    List<List<String>> rows = new ArrayList<>();
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      int cols = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        List<String> row = new ArrayList<>();
        for (int i = 1; i <= cols; i++) {
          row.add(rs.getString(i));
        }
        rows.add(row);
      }
    }
    return rows;
  }

  /** Sorts rows lexicographically for order-independent comparison. */
  private List<List<String>> sortRows(List<List<String>> rows) {
    List<List<String>> sorted = new ArrayList<>(rows);
    sorted.sort((a, b) -> {
      for (int i = 0; i < Math.min(a.size(), b.size()); i++) {
        String av = a.get(i) == null ? "" : a.get(i);
        String bv = b.get(i) == null ? "" : b.get(i);
        int cmp = av.compareTo(bv);
        if (cmp != 0) return cmp;
      }
      return Integer.compare(a.size(), b.size());
    });
    return sorted;
  }

  /** Executes a statement on the publisher connection. */
  protected void execOnPublisher(String sql) throws Exception {
    exec(pubConn, sql);
  }

  protected static void exec(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  /** First column of the first row as text, or null for a NULL or no row. */
  protected static String scalar(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      return rs.next() ? rs.getString(1) : null;
    }
  }
}
