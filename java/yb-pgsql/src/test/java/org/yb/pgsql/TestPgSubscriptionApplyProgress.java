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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;
import org.yb.client.TestUtils;

/**
 * The apply worker records what it applied in the same transaction, so a transaction the
 * publisher resends after a worker restart is skipped instead of applied twice: the restarted
 * worker asks the walsender to start after it, and skips by commit time anything still sent.
 *
 * Feedback is withheld (yb_test_subscription_skip_feedback), so the publisher never learns what
 * was applied and every restart is resent everything since the slot was created.  That is the
 * window a crash leaves open between a commit and the next feedback message, made permanent.
 *
 * Progress is kept in yb_system.pg_yb_subscription_progress, which the master creates when
 * ysql_yb_enable_pg_subscription is set.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionApplyProgress extends BasePgSubscriptionApplyTest {
  private static final String SKIP_LOG = "applied before the worker restarted";
  private static final String FILTER_LOG = "YBDecodeCommit: Ignoring txn";
  private static final String PROGRESS_TABLE = "pg_yb_subscription_progress";

  @Override
  protected Map<String, String> getMasterFlags() {
    Map<String, String> flagMap = super.getMasterFlags();
    flagMap.put("ysql_yb_enable_pg_subscription", "true");
    return flagMap;
  }

  @Override
  protected Map<String, String> getTServerFlags() {
    Map<String, String> flagMap = super.getTServerFlags();
    flagMap.put("ysql_yb_enable_pg_subscription", "true");
    // Commit hybrid times are what identify a resent transaction from a YB publisher.
    flagMap.put("ysql_yb_use_commit_time_ht_in_replication", "true");
    appendToYsqlPgConf(flagMap, "yb_test_subscription_skip_feedback=true");
    return flagMap;
  }

  private Connection ybSystemConn() throws Exception {
    return getConnectionBuilder().withDatabase("yb_system").connect();
  }

  private long progressRows() throws Exception {
    try (Connection conn = ybSystemConn();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + PROGRESS_TABLE)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  // The master creates the table in the background; a worker that starts before it exists records
  // nothing for its whole lifetime, so wait for it before any subscription is created.
  @Before
  public void waitForProgressTable() throws Exception {
    TestUtils.waitFor(() -> {
      try {
        progressRows();
        return true;
      } catch (Exception e) {
        return false;
      }
    }, TIMEOUT_MS, POLL_INTERVAL_MS);
  }

  private void restartApplyWorker() throws Exception {
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("ALTER SUBSCRIPTION test_sub DISABLE");
    }
    TestUtils.waitFor(() -> {
      try (Statement stmt = subConn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT count(*) FROM pg_stat_subscription "
               + "WHERE subname = 'test_sub' AND pid IS NOT NULL")) {
        return rs.next() && rs.getLong(1) == 0;
      }
    }, TIMEOUT_MS, POLL_INTERVAL_MS);
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("ALTER SUBSCRIPTION test_sub ENABLE");
    }
  }

  /**
   * Resent INSERTs would fail on a duplicate key and wedge the worker, so a fresh change applying
   * after the restart shows the resent ones were not applied again.  The restarted worker starts
   * from the end_lsn in the progress table, so the walsender filters them before they are sent
   * and the worker's own skip never fires.
   */
  @Test
  public void testResentTransactionsAreSkipped() throws Exception {
    // The walsender reports what it filters at DEBUG1, and reads this when the restart reconnects.
    try (Statement stmt = pubConn.createStatement()) {
      stmt.execute("ALTER DATABASE " + PUB_DB + " SET log_min_messages = 'debug1'");
    }
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });
    execOnPublisher("INSERT INTO t VALUES (1, 'a')");
    execOnPublisher("INSERT INTO t VALUES (2, 'b')");
    execOnPublisher("UPDATE t SET val = 'c' WHERE id = 1");
    execOnPublisher("DELETE FROM t WHERE id = 2");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "c")));

    List<String> filtered = collectPgLogLines(FILTER_LOG);
    List<String> skipped = collectPgLogLines(SKIP_LOG);
    restartApplyWorker();
    execOnPublisher("INSERT INTO t VALUES (3, 'd')");

    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "c"), row("3", "d")));
    waitForLogLine(filtered, "the walsender filtering a resent transaction");
    // Every resent transaction precedes id 3, so all of them have been handled by now.
    assertTrue("resent transactions should be filtered by the walsender, not reach the worker: "
               + skipped, skipped.isEmpty());
    assertEquals(0L, applyErrorCount());
  }

  /**
   * Progress recorded after one restart must survive the next: the second restart resends both
   * the transactions from before the first and the one applied between them.
   */
  @Test
  public void testProgressSurvivesRepeatedRestarts() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });
    execOnPublisher("INSERT INTO t VALUES (1, 'a')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "a")));

    restartApplyWorker();
    execOnPublisher("INSERT INTO t VALUES (2, 'b')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "a"), row("2", "b")));

    restartApplyWorker();
    execOnPublisher("INSERT INTO t VALUES (3, 'c')");
    waitForRows("SELECT id, val FROM t",
                Arrays.asList(row("1", "a"), row("2", "b"), row("3", "c")));
    assertEquals(0L, applyErrorCount());
  }

  /** DROP SUBSCRIPTION removes the subscription's progress, so nothing is left in yb_system. */
  @Test
  public void testDropSubscriptionRemovesProgress() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });
    execOnPublisher("INSERT INTO t VALUES (1, 'a')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "a")));
    assertEquals(1L, progressRows());

    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("ALTER SUBSCRIPTION test_sub DISABLE");
      stmt.execute("ALTER SUBSCRIPTION test_sub SET (slot_name = NONE)");
      stmt.execute("DROP SUBSCRIPTION test_sub");
    }
    assertEquals(0L, progressRows());
  }
}
