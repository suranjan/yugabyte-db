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
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.TestUtils;
import org.yb.util.BuildTypeUtil;

/**
 * Harness shared by the tests that exercise the apply worker's DML path: a
 * publisher database and a subscriber database on one cluster, wired together by
 * a publication and a subscription, and the polling helpers the assertions need.
 *
 * Split out of TestPgSubscription so that class stays inside the per-class fork
 * timeout - it recreates the cluster for every test method, which puts a hard
 * ceiling on how many tests one class can hold.
 */
public abstract class BasePgSubscriptionApplyTest extends BasePgSQLTest {
  private static final Logger LOG = LoggerFactory.getLogger(BasePgSubscriptionApplyTest.class);

  protected static final String PUB_DB = "pub_db";
  protected static final String SUB_DB = "sub_db";
  protected static final long REPLICATION_TIMEOUT_MS =
      BuildTypeUtil.adjustTimeout(60000);
  protected static final int REPLICATION_POLL_INTERVAL_MS = 500;

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

  @Override
  public int getTestMethodTimeoutSec() {
    return 300;
  }

  @Override
  protected Map<String, String> getTServerFlags() {
    Map<String, String> flagMap = super.getTServerFlags();
    appendToYsqlPgConf(flagMap, "yb_enable_pg_subscription=true");
    flagMap.put("ysql_cdc_active_replication_slot_window_ms", "0");
    flagMap.put("cdc_send_null_before_image_if_not_exists", "true");
    flagMap.put("TEST_dcheck_for_missing_schema_packing", "false");
    return flagMap;
  }

  @Override
  protected Map<String, String> getMasterFlags() {
    Map<String, String> flagMap = super.getMasterFlags();
    flagMap.put("TEST_dcheck_for_missing_schema_packing", "false");
    return flagMap;
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

  @After
  public void tearDownSubscription() throws Exception {
    // Tear down subscription first.
    if (subConn != null && !subConn.isClosed()) {
      try (Statement stmt = subConn.createStatement()) {
        stmt.execute("ALTER SUBSCRIPTION test_sub DISABLE");
        stmt.execute("ALTER SUBSCRIPTION test_sub SET (slot_name = NONE)");
        stmt.execute("DROP SUBSCRIPTION IF EXISTS test_sub");
      } catch (Exception e) {
        LOG.warn("Failed to drop subscription during cleanup", e);
      }
    }
    // Drop replication slots on the publisher.
    if (pubConn != null && !pubConn.isClosed()) {
      try (Statement stmt = pubConn.createStatement()) {
        stmt.execute(
            "SELECT pg_drop_replication_slot(slot_name) "
            + "FROM pg_replication_slots WHERE plugin = 'pgoutput'");
      } catch (Exception e) {
        LOG.warn("Failed to drop replication slots during cleanup", e);
      }
    }
    // Let the apply worker fully exit before dropping objects it may reference.
    Thread.sleep(2000);
    // Drop tables and publication (cheaper than dropping entire databases).
    if (pubConn != null && !pubConn.isClosed()) {
      try (Statement stmt = pubConn.createStatement()) {
        stmt.execute("DROP PUBLICATION IF EXISTS test_pub");
        stmt.execute("DROP TABLE IF EXISTS t");
      } catch (Exception e) {
        LOG.warn("Failed to drop publisher objects during cleanup", e);
      }
      pubConn.close();
    }
    if (subConn != null && !subConn.isClosed()) {
      try (Statement stmt = subConn.createStatement()) {
        stmt.execute("DROP TABLE IF EXISTS t");
      } catch (Exception e) {
        LOG.warn("Failed to drop subscriber table during cleanup", e);
      }
      subConn.close();
    }
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
  protected void createPubSub(String tableName) throws Exception {
    try (Statement pub = pubConn.createStatement();
         Statement sub = subConn.createStatement()) {
      pub.execute("CREATE PUBLICATION test_pub FOR TABLE " + tableName);
      sub.execute(String.format(
          "CREATE SUBSCRIPTION test_sub CONNECTION 'host=%s port=%d dbname=%s user=%s' "
          + "PUBLICATION test_pub WITH (copy_data = false)",
          getPgHost(0), getPgPort(0), PUB_DB, ConnectionBuilder.TEST_PG_USER));
    }
    // Give the launcher time to detect the new subscription and start the
    // apply worker before we begin issuing DMLs.
    Thread.sleep(2000);
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
    }, REPLICATION_TIMEOUT_MS, REPLICATION_POLL_INTERVAL_MS);

    List<List<String>> actual = fetchRows(subConn, query);
    assertEquals("Rows on subscriber do not match expected",
                 sortedExpected, sortRows(actual));
  }

  /**
   * Waits until a scalar count query on the subscriber returns the expected
   * value.
   */
  protected void waitForRowCount(String tableName, int expectedCount) throws Exception {
    String query = "SELECT count(*) FROM " + tableName;
    TestUtils.waitFor(() -> {
      try (Statement stmt = subConn.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        return rs.next() && rs.getInt(1) == expectedCount;
      }
    }, REPLICATION_TIMEOUT_MS, REPLICATION_POLL_INTERVAL_MS);

    try (Statement stmt = subConn.createStatement();
         ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next());
      assertEquals(expectedCount, rs.getInt(1));
    }
  }

  /** Fetches all rows from a query as lists of strings. */
  /**
   * Wait until the subscription has recorded at least one apply failure.
   *
   * The worker retries a failing transaction forever, so the count only has to
   * become non-zero; it keeps climbing afterwards.
   */
  protected void waitForApplyError() throws Exception {
    TestUtils.waitFor(() -> {
      try (Statement stmt = subConn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT apply_error_count FROM pg_stat_subscription_stats "
               + "WHERE subname = 'test_sub'")) {
        return rs.next() && rs.getLong(1) > 0;
      }
    }, REPLICATION_TIMEOUT_MS, REPLICATION_POLL_INTERVAL_MS);
  }

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

  /** Executes a DML statement on the publisher connection. */
  protected void execOnPublisher(String sql) throws Exception {
    try (Statement stmt = pubConn.createStatement()) {
      stmt.execute(sql);
    }
  }
}
