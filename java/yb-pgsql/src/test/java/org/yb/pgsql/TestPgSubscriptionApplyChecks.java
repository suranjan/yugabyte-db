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
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;

/**
 * Apply-path cases that turn on the shape of the target relation rather than on
 * the DML itself: that a conflicting INSERT is reported, the privilege needed
 * to look the row up at all, and the missing-row
 * report the rows-affected count serves.
 *
 * Separate from TestPgSubscription because that class already runs close to the
 * per-class fork timeout.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionApplyChecks extends BasePgSubscriptionApplyTest {

  /**
   * A replicated INSERT that collides with a diverged local row reports the
   * duplicate key, as on PG, rather than overwriting the row.
   */
  @Test
  public void testConflictingInsertIsReported() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    // Diverge the subscriber: the row the publisher is about to send is already
    // here with a different value.
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("INSERT INTO t VALUES (1, 'local')");
    }

    execOnPublisher("INSERT INTO t VALUES (1, 'from_pub')");

    waitForApplyError();
    assertEquals(Arrays.asList(row("1", "local")),
                 fetchRows(subConn, "SELECT id, val FROM t"));
  }

  /**
   * The same conflict on a relation with secondary indexes: the indexes must
   * come through the rejected insert consistent.
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
   * The missing-row report has to survive the write-only fast path for UPDATE
   * too, not just for DELETE.  Nothing about this table needs the local row -
   * no index, no trigger, no constraint - so the apply would otherwise answer
   * "found" without reading and report nothing at all.
   *
   * Unlike DELETE, UPDATE cannot recover the answer after the fact: apply
   * passes canSetTag false with no RETURNING clause, so YBCExecuteUpdate always
   * batches and never asks DocDB for a rows-affected count.  So the fast path
   * is given up while this report is reachable, and that is what this asserts.
   */
  @Test
  public void testUpdateOfMissingRowIsReportedWithDebugLogging() throws Exception {
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("ALTER DATABASE " + SUB_DB + " SET log_min_messages = 'debug1'");
    }
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    List<String> reports =
        collectPgLogLines("logical replication did not find row to be updated");

    execOnPublisher("INSERT INTO t VALUES (1, 'seed'), (2, 'gone')");
    waitForRowCount("t", 2);

    // Diverge: the row the publisher is about to update is not here.
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("DELETE FROM t WHERE id = 2");
    }

    execOnPublisher("UPDATE t SET val = 'updated' WHERE id = 2");
    waitForLogLine(reports, "the missing-row report for UPDATE");

    // The update of the absent row is a no-op, and the stream keeps flowing.
    execOnPublisher("INSERT INTO t VALUES (3, 'sentinel')");
    waitForRows("SELECT id, val FROM t ORDER BY id",
                Arrays.asList(row("1", "seed"), row("3", "sentinel")));
  }

  /**
   * A relation whose only trigger fires on INSERT needs nothing from the old
   * row when an UPDATE or a DELETE is applied, so both still take the
   * write-only path.  The predicate is asked per operation rather than "does
   * this relation have any trigger at all", and this is the case that
   * distinguishes the two.
   */
  @Test
  public void testApplyOnTableWithInsertOnlyTrigger() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
      stmt.execute("CREATE TABLE t_audit (id INT, val TEXT)");
      // Schema-qualified: the apply worker runs with an empty search_path, so an
      // unqualified name in the trigger body would not resolve.
      stmt.execute("CREATE FUNCTION t_audit_fn() RETURNS TRIGGER AS $$ "
                   + "BEGIN INSERT INTO public.t_audit VALUES (NEW.id, NEW.val); "
                   + "RETURN NEW; END; $$ LANGUAGE plpgsql");
      stmt.execute("CREATE TRIGGER t_ins AFTER INSERT ON t "
                   + "FOR EACH ROW EXECUTE FUNCTION t_audit_fn()");
      // Replicated writes only fire triggers on a subscriber that enables them.
      stmt.execute("ALTER TABLE t ENABLE ALWAYS TRIGGER t_ins");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    waitForRows("SELECT id, val FROM t ORDER BY id",
                Arrays.asList(row("1", "a"), row("2", "b")));

    execOnPublisher("UPDATE t SET val = 'a2' WHERE id = 1");
    execOnPublisher("DELETE FROM t WHERE id = 2");
    waitForRows("SELECT id, val FROM t ORDER BY id", Arrays.asList(row("1", "a2")));

    // The INSERT trigger fired for the two inserts, and for nothing else.
    assertEquals(Arrays.asList(row("1", "a"), row("2", "b")),
                 fetchRows(subConn, "SELECT id, val FROM t_audit ORDER BY id"));
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
