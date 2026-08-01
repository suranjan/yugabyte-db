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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;

/**
 * Secondary index maintenance on the apply path.
 *
 * A DocDB row write does not touch index entries, so the apply worker deletes
 * the old entries and inserts the new ones itself; these tests check the pair
 * from the outside, by looking rows up through the index.
 *
 * Split out of {@link TestPgSubscription} for runtime: every test method
 * recreates the mini-cluster, so a single class holding all of them runs up
 * against the per-class fork timeout.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionSecondaryIndex extends BasePgSubscriptionApplyTest {

  @Test
  public void testInsertWithSecondaryIndex() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
      stmt.execute("CREATE INDEX idx_val ON t (val)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')");
    waitForRowCount("t", 2);

    // Verify the index is usable on the subscriber via index scan.
    List<List<String>> rows = fetchRows(subConn,
        "SELECT id FROM t WHERE val = 'alpha'");
    assertEquals(1, rows.size());
    assertEquals("1", rows.get(0).get(0));
  }

  @Test
  public void testUpdateWithSecondaryIndex() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
      stmt.execute("CREATE INDEX idx_val ON t (val)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'old_val')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "old_val")));

    execOnPublisher("UPDATE t SET val = 'new_val' WHERE id = 1");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "new_val")));

    // Old index entry must be gone, new one present.
    List<List<String>> oldLookup = fetchRows(subConn,
        "SELECT id FROM t WHERE val = 'old_val'");
    assertEquals(0, oldLookup.size());

    List<List<String>> newLookup = fetchRows(subConn,
        "SELECT id FROM t WHERE val = 'new_val'");
    assertEquals(1, newLookup.size());
    assertEquals("1", newLookup.get(0).get(0));
  }

  @Test
  public void testDeleteWithSecondaryIndex() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
      stmt.execute("CREATE INDEX idx_val ON t (val)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'to_delete')");
    waitForRowCount("t", 1);

    execOnPublisher("DELETE FROM t WHERE id = 1");
    waitForRowCount("t", 0);

    List<List<String>> lookup = fetchRows(subConn,
        "SELECT id FROM t WHERE val = 'to_delete'");
    assertEquals(0, lookup.size());
  }

  @Test
  public void testMultipleSecondaryIndexes() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)");
      stmt.execute("CREATE INDEX idx_a ON t (a)");
      stmt.execute("CREATE INDEX idx_b ON t (b)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'foo', 100)");
    execOnPublisher("INSERT INTO t VALUES (2, 'bar', 200)");
    waitForRowCount("t", 2);

    execOnPublisher("UPDATE t SET a = 'baz', b = 300 WHERE id = 1");
    waitForRows("SELECT id, a, b FROM t WHERE id = 1",
                Arrays.asList(row("1", "baz", "300")));

    // Verify both indexes reflect the update.
    assertEquals(1, fetchRows(subConn, "SELECT id FROM t WHERE a = 'baz'").size());
    assertEquals(0, fetchRows(subConn, "SELECT id FROM t WHERE a = 'foo'").size());
    assertEquals(1, fetchRows(subConn, "SELECT id FROM t WHERE b = 300").size());
    assertEquals(0, fetchRows(subConn, "SELECT id FROM t WHERE b = 100").size());

    execOnPublisher("DELETE FROM t WHERE id = 2");
    waitForRowCount("t", 1);
    assertEquals(0, fetchRows(subConn, "SELECT id FROM t WHERE a = 'bar'").size());
    assertEquals(0, fetchRows(subConn, "SELECT id FROM t WHERE b = 200").size());
  }

  @Test
  public void testUniqueSecondaryIndex() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'alice@example.com')");
    execOnPublisher("INSERT INTO t VALUES (2, 'bob@example.com')");
    waitForRowCount("t", 2);

    // Verify unique index lookup works.
    List<List<String>> lookup = fetchRows(subConn,
        "SELECT id FROM t WHERE email = 'alice@example.com'");
    assertEquals(1, lookup.size());
    assertEquals("1", lookup.get(0).get(0));
  }

  /**
   * Under REPLICA IDENTITY DEFAULT the publisher omits the old tuple, so the
   * apply worker must fetch the local row to learn the old values of indexed
   * columns; deleting the old index entries with values taken from the new
   * tuple leaves the real old entries behind (stale index entries).
   */
  @Test
  public void testSecondaryIndexWithReplicaIdentityDefault() throws Exception {
    createReplicatedTable("t", "DEFAULT", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
      stmt.execute("CREATE INDEX idx_val ON t (val)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'before')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "before")));

    execOnPublisher("UPDATE t SET val = 'after' WHERE id = 1");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "after")));

    // The old index entry must be gone and the new one present.
    assertEquals(0, fetchRows(subConn,
        "SELECT id FROM t WHERE val = 'before'").size());
    List<List<String>> lookup = fetchRows(subConn,
        "SELECT id FROM t WHERE val = 'after'");
    assertEquals(1, lookup.size());
    assertEquals("1", lookup.get(0).get(0));

    execOnPublisher("DELETE FROM t WHERE id = 1");
    waitForRowCount("t", 0);
    assertEquals(0, fetchRows(subConn,
        "SELECT id FROM t WHERE val = 'after'").size());
  }

  /**
   * A publisher-side UPDATE that changes the primary key must converge on the
   * subscriber: the old key gone, the new key present, secondary indexes
   * consistent.  (A YB publisher streams this as DELETE+INSERT; the in-place
   * replace path in YbExecSimpleRelationUpdate is exercised by PostgreSQL
   * publishers, which send a single UPDATE message.)
   */
  @Test
  public void testPrimaryKeyUpdateReplication() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
      stmt.execute("CREATE INDEX idx_val ON t (val)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'movable')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "movable")));

    execOnPublisher("UPDATE t SET id = 2 WHERE id = 1");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("2", "movable")));

    // The secondary index must resolve to the new row only.
    List<List<String>> lookup = fetchRows(subConn,
        "SELECT id FROM t WHERE val = 'movable'");
    assertEquals(1, lookup.size());
    assertEquals("2", lookup.get(0).get(0));
  }

  /**
   * The counterpart of the cases above: with no secondary index there is no
   * index maintenance to do, and the whole INSERT/UPDATE/DELETE sequence goes
   * through the write-only path.
   */
  @Test
  public void testTableWithNoSecondaryIndex() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'a')");
    execOnPublisher("UPDATE t SET val = 'b' WHERE id = 1");
    execOnPublisher("DELETE FROM t WHERE id = 1");

    waitForRowCount("t", 0);
  }
}
