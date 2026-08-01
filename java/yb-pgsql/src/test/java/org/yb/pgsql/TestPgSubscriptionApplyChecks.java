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
import java.sql.Statement;
import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;

/**
 * Apply-path cases that turn on the shape of the target relation rather than on
 * the DML itself: whether a conflicting INSERT is absorbed by the upsert or
 * reported, the privilege needed to look the row up at all, and the missing-row
 * report the rows-affected count serves.
 *
 * Separate from TestPgSubscription because that class already runs close to the
 * per-class fork timeout.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionApplyChecks extends BasePgSubscriptionApplyTest {

  /**
   * A replicated INSERT is an upsert where a blind write is safe, which is what
   * makes crash replay of an already-applied change idempotent: the row the
   * publisher sends wins over whatever is there.  No secondary index, no
   * trigger and no rule, so nothing needs the replaced row's old values.
   */
  @Test
  public void testUpsertOverConflictOnSafeTable() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    // Diverge the subscriber: the row the publisher is about to send is already
    // here with a different value.
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("INSERT INTO t VALUES (1, 'local')");
    }

    execOnPublisher("INSERT INTO t VALUES (1, 'from_pub')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "from_pub")));
  }

  /**
   * On a relation where a blind write is unsafe - here, one with secondary
   * indexes - the upsert is not used, so a replicated INSERT that collides with
   * a diverged local row reports the duplicate key instead of overwriting it.
   * That is the only way the divergence becomes visible; the indexes must come
   * through it consistent, since a half-applied upsert is what would corrupt
   * them.
   */
  @Test
  public void testConflictingInsertOnIndexedTableIsReported() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT, tag TEXT)");
      stmt.execute("CREATE INDEX idx_val ON t (val)");
      stmt.execute("CREATE UNIQUE INDEX idx_tag ON t (tag)");
    });

    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("INSERT INTO t VALUES (1, 'local', 'local_tag')");
    }

    execOnPublisher("INSERT INTO t VALUES (1, 'from_pub', 'pub_tag')");

    // The apply worker cannot get past the duplicate key, so it retries and
    // records the failure rather than silently replacing the row.
    waitForApplyError();

    assertEquals(Arrays.asList(row("1", "local", "local_tag")),
                 fetchRows(subConn, "SELECT id, val, tag FROM t"));

    // An orphaned entry is invisible to a query that filters on it, so compare
    // each index against the table itself.
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("SELECT yb_index_check('idx_val'::regclass::oid)");
      stmt.execute("SELECT yb_index_check('idx_tag'::regclass::oid)");
    }
  }

  /**
   * Locating the local row is a read, so it needs SELECT on the target - and
   * the check has to happen whether or not the row is addressed by a
   * key-derived ybctid.  UPDATE is granted and SELECT is not, so a permission
   * failure here can only come from the read.
   */
  @Test
  public void testApplyRequiresSelectPrivilegeOnTarget() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'seed')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "seed")));

    // A subscription can only be owned by a superuser, so grant it, hand over
    // the subscription, and take it away again - the worker then applies as an
    // unprivileged role.
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("CREATE ROLE sub_owner SUPERUSER LOGIN");
      stmt.execute("ALTER SUBSCRIPTION test_sub OWNER TO sub_owner");
      stmt.execute("ALTER ROLE sub_owner NOSUPERUSER");
      stmt.execute("GRANT UPDATE ON t TO sub_owner");
      // Restart the worker so it reconnects as the new owner.
      stmt.execute("ALTER SUBSCRIPTION test_sub DISABLE");
      stmt.execute("ALTER SUBSCRIPTION test_sub ENABLE");
    }
    Thread.sleep(2000);

    execOnPublisher("UPDATE t SET val = 'must_not_apply' WHERE id = 1");

    // The apply must fail rather than proceed; the worker retries, so the error
    // count keeps climbing while the row stays put.
    waitForApplyError();

    assertEquals(Arrays.asList(row("1", "seed")),
                 fetchRows(subConn, "SELECT id, val FROM t"));
  }

  /**
   * As above, with the apply worker's DEBUG1 missing-row report enabled.  The
   * write-only delete only asks DocDB for a rows-affected count when that
   * report can be emitted - the count costs a synchronous round trip - so this
   * covers the other side of that branch, which must reach the same state.
   */
  @Test
  public void testDeleteNonExistentRowWithDebugLogging() throws Exception {
    // Set before the subscription exists, so the apply worker picks it up when
    // it connects.
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("ALTER DATABASE " + SUB_DB + " SET log_min_messages = 'debug1'");
    }
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    List<String> reports =
        collectPgLogLines("logical replication did not find row to be deleted");

    execOnPublisher("INSERT INTO t VALUES (1, 'keep'), (2, 'remove')");
    waitForRowCount("t", 2);

    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("DELETE FROM t WHERE id = 2");
    }
    execOnPublisher("DELETE FROM t WHERE id = 2");
    waitForLogLine(reports, "the missing-row report for DELETE");

    execOnPublisher("INSERT INTO t VALUES (3, 'sentinel')");
    waitForRows("SELECT id, val FROM t ORDER BY id",
                Arrays.asList(row("1", "keep"), row("3", "sentinel")));
  }
}
