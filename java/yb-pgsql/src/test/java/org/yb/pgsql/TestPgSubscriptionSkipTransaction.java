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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;
import org.yb.client.TestUtils;

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
    extends BasePgSubscriptionApplyTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestPgSubscriptionSkipTransaction.class);

  private static final String TABLE = "skiptest";

  /**
   * The finish LSN of a failed transaction is only reported in the apply
   * worker's error CONTEXT, so the test has to read the server log - the same
   * thing an operator does before choosing an LSN to skip.
   */
  private static final Pattern FINISH_LSN = Pattern.compile(
      "for replication target relation \"public\\." + TABLE
      + "\" in transaction \\d+, finished at ([0-9A-Fa-f]+/[0-9A-Fa-f]+)");

  /** The finish LSN in the most recent of the collected CONTEXT lines, or null. */
  private static String finishLsn(List<String> lines) {
    String lsn = null;
    synchronized (lines) {
      for (String line : lines) {
        Matcher m = FINISH_LSN.matcher(line);
        if (m.find()) {
          lsn = m.group(1);
        }
      }
    }
    return lsn;
  }

  private String subscriptionColumn(String column) throws Exception {
    return scalar(subConn, "SELECT " + column + "::text FROM pg_subscription "
                           + "WHERE subname = '" + SUB_NAME + "'");
  }

  private void waitForSubscriptionColumn(String column, String want)
      throws Exception {
    waitForSubscriberValue("SELECT " + column + "::text FROM pg_subscription "
                           + "WHERE subname = '" + SUB_NAME + "'", want);
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
    // disable_on_error so the failed transaction stops the worker cleanly
    // instead of being retried forever, which is what makes SKIP usable.
    createPubSubWith("copy_data = false, disable_on_error = true", TABLE);

    // Seed the conflict after sync, so it is the apply path that fails and not
    // the initial copy.
    exec(subConn, "INSERT INTO " + TABLE + " VALUES (1, 'local')");

    List<String> contexts = collectPgLogLines("finished at");

    exec(pubConn, "BEGIN");
    exec(pubConn, "INSERT INTO " + TABLE + " VALUES (1, 'from-pub')");
    exec(pubConn, "INSERT INTO " + TABLE + " VALUES (2, 'collateral')");
    exec(pubConn, "COMMIT");

    // Wait for the reported finish LSN rather than for the subscription to go
    // disabled: any apply error disables it, so waiting on subenabled alone
    // would pass on an unrelated failure and then time out looking for an LSN
    // that was never going to appear.
    LOG.info("Waiting for the apply worker to report the failed transaction");
    TestUtils.waitFor(() -> finishLsn(contexts) != null, TIMEOUT_MS, POLL_INTERVAL_MS);
    waitForSubscriptionColumn("subenabled", "false");

    // Take the most recent one: the worker may have retried before
    // disable_on_error took effect.
    String lsn = finishLsn(contexts);
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
    waitForSubscriberValue("SELECT count(*) FROM " + TABLE + " WHERE i = 3", "1");

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
