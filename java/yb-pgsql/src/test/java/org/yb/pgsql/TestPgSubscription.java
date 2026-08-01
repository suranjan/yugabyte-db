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
 * End-to-end tests for YugabyteDB logical replication (pg_subscription).
 *
 * Tests the YB-native DML execution path in the apply worker:
 *   - YBCExecuteInsert (with ONCONFLICT_YB_REPLACE)
 *   - YBCExecuteUpdate (with an empty ModifyTable plan, yb_repl_mt_plan)
 *   - YBCExecuteDelete
 *
 * Index maintenance lives in {@link TestPgSubscriptionSecondaryIndex} and the
 * cases that turn on the shape of the target relation in
 * {@link TestPgSubscriptionApplyChecks}; every test method here recreates the
 * mini-cluster, so one class holding all of them would exceed the per-class
 * fork timeout.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscription extends BasePgSubscriptionApplyTest {
  // ===================================================================
  //  Tests
  // ===================================================================

  // ----- Group 1: Basic DML replication -----

  @Test
  public void testInsertReplication() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'hello')");
    execOnPublisher("INSERT INTO t VALUES (2, 'world')");

    waitForRows("SELECT id, val FROM t ORDER BY id",
                Arrays.asList(row("1", "hello"), row("2", "world")));
  }

  @Test
  public void testUpdateReplication() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'before')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "before")));

    execOnPublisher("UPDATE t SET val = 'after' WHERE id = 1");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "after")));
  }

  @Test
  public void testDeleteReplication() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'gone')");
    waitForRowCount("t", 1);

    execOnPublisher("DELETE FROM t WHERE id = 1");
    waitForRowCount("t", 0);
  }

  @Test
  public void testMixedDmlReplication() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    execOnPublisher("UPDATE t SET val = 'B' WHERE id = 2");
    execOnPublisher("DELETE FROM t WHERE id = 3");

    waitForRows("SELECT id, val FROM t ORDER BY id",
                Arrays.asList(row("1", "a"), row("2", "B")));
  }

  // ----- Group 2: UPDATE-specific logic -----

  @Test
  public void testUpdateSingleColumn() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'a1', 'b1', 'c1')");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "a1", "b1", "c1")));

    execOnPublisher("UPDATE t SET b = 'b2' WHERE id = 1");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "a1", "b2", "c1")));
  }

  @Test
  public void testUpdateAllColumns() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'old', 10)");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "old", "10")));

    execOnPublisher("UPDATE t SET a = 'new', b = 20 WHERE id = 1");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "new", "20")));
  }

  @Test
  public void testUpdateWithNulls() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'notnull')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "notnull")));

    // Set to NULL.
    execOnPublisher("UPDATE t SET val = NULL WHERE id = 1");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", null)));

    // Set back from NULL.
    execOnPublisher("UPDATE t SET val = 'restored' WHERE id = 1");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "restored")));
  }

  /**
   * Under REPLICA IDENTITY DEFAULT the publisher sends no old tuple for an
   * UPDATE that leaves the key alone, so the apply worker binds columns from
   * the changed-column bitmap that apply_handle_update builds.  That bitmap is
   * indexed from the relation's own attribute base, so a table wide enough for
   * an off-by-one to land on a real neighbour catches a mis-based bitmap:
   * the wrong column would be written and the intended one left stale.
   */
  @Test
  public void testUpdateWithReplicaIdentityDefault() throws Exception {
    createReplicatedTable("t", "DEFAULT", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, "
                   + "a TEXT, b TEXT, c TEXT, d TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'a0', 'b0', 'c0', 'd0')");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "a0", "b0", "c0", "d0")));

    // Update each column in turn; every other column must survive untouched.
    execOnPublisher("UPDATE t SET a = 'a1' WHERE id = 1");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "a1", "b0", "c0", "d0")));

    execOnPublisher("UPDATE t SET c = 'c1' WHERE id = 1");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "a1", "b0", "c1", "d0")));

    execOnPublisher("UPDATE t SET d = 'd1' WHERE id = 1");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "a1", "b0", "c1", "d1")));

    // Multi-column update in one statement.
    execOnPublisher("UPDATE t SET b = 'b2', d = 'd2' WHERE id = 1");
    waitForRows("SELECT * FROM t", Arrays.asList(row("1", "a1", "b2", "c1", "d2")));
  }

  /**
   * Under REPLICA IDENTITY DEFAULT a DELETE carries only the key columns, so
   * the apply worker has to derive the row key from a partially populated
   * tuple.
   */
  @Test
  public void testDeleteWithReplicaIdentityDefault() throws Exception {
    createReplicatedTable("t", "DEFAULT", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'x', 'y'), (2, 'p', 'q')");
    waitForRowCount("t", 2);

    execOnPublisher("DELETE FROM t WHERE id = 1");
    waitForRows("SELECT * FROM t", Arrays.asList(row("2", "p", "q")));
  }

  /**
   * Tables without a primary key cannot be addressed by a ybctid derived from
   * the replicated tuple; the apply worker locates the row with a plan-based
   * scan and uses the storage ybctid it returns.
   */
  @Test
  public void testTableWithoutPrimaryKey() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (a INT, b TEXT)");
    });

    execOnPublisher(
        "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'y'), (4, 'w'), (5, 'v')");
    waitForRowCount("t", 5);

    // Several changes in a row, each needing its own lookup: every one restarts
    // the underlying DocDB scan, and a scan that instead resumed where the
    // previous one stopped would fail to find the later targets.  Duplicate
    // values in b keep the match dependent on comparing the whole tuple.
    execOnPublisher("UPDATE t SET b = 'z' WHERE a = 2");
    waitForRows("SELECT a, b FROM t ORDER BY a",
                Arrays.asList(row("1", "x"), row("2", "z"), row("3", "y"),
                              row("4", "w"), row("5", "v")));

    execOnPublisher("UPDATE t SET b = 'V' WHERE a = 5");
    waitForRows("SELECT a, b FROM t ORDER BY a",
                Arrays.asList(row("1", "x"), row("2", "z"), row("3", "y"),
                              row("4", "w"), row("5", "V")));

    execOnPublisher("DELETE FROM t WHERE a = 1");
    execOnPublisher("DELETE FROM t WHERE a = 4");
    waitForRows("SELECT a, b FROM t ORDER BY a",
                Arrays.asList(row("2", "z"), row("3", "y"), row("5", "V")));
  }

  // ----- Group 3: Primary key variations -----

  @Test
  public void testCompositePrimaryKey() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (a INT, b INT, val TEXT, PRIMARY KEY (a, b))");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 10, 'x')");
    execOnPublisher("INSERT INTO t VALUES (1, 20, 'y')");
    execOnPublisher("INSERT INTO t VALUES (2, 10, 'z')");
    waitForRowCount("t", 3);

    execOnPublisher("UPDATE t SET val = 'X' WHERE a = 1 AND b = 10");
    waitForRows("SELECT a, b, val FROM t WHERE a = 1 AND b = 10",
                Arrays.asList(row("1", "10", "X")));

    execOnPublisher("DELETE FROM t WHERE a = 1 AND b = 20");
    waitForRowCount("t", 2);
  }

  // ----- Group 4: Data type coverage -----

  @Test
  public void testVariousDataTypes() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t ("
          + "id INT PRIMARY KEY, "
          + "col_bigint BIGINT, "
          + "col_text TEXT, "
          + "col_bool BOOLEAN, "
          + "col_numeric NUMERIC(10,2), "
          + "col_bytea BYTEA)");
    });

    execOnPublisher(
        "INSERT INTO t VALUES (1, 9999999999, 'hello world', true, 123.45, E'\\\\xDEADBEEF')");
    waitForRowCount("t", 1);

    List<List<String>> rows = fetchRows(subConn,
        "SELECT id, col_bigint, col_text, col_bool, col_numeric FROM t");
    assertEquals(1, rows.size());
    assertEquals("1", rows.get(0).get(0));
    assertEquals("9999999999", rows.get(0).get(1));
    assertEquals("hello world", rows.get(0).get(2));
    assertEquals("t", rows.get(0).get(3));
    assertEquals("123.45", rows.get(0).get(4));

    execOnPublisher("UPDATE t SET col_text = 'updated', col_bool = false WHERE id = 1");
    waitForRows("SELECT id, col_text, col_bool FROM t",
                Arrays.asList(row("1", "updated", "f")));

    execOnPublisher("DELETE FROM t WHERE id = 1");
    waitForRowCount("t", 0);
  }

  // ----- Group 5: Crash safety / upsert -----

  @Test
  public void testInsertIdempotency() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    // Seed a conflicting row directly on the subscriber, so the replicated
    // plain INSERT below collides with it on the apply side.  A publisher-side
    // INSERT ... ON CONFLICT would not do: it executes there as an UPDATE and
    // replicates as an UPDATE message, never reaching the insert-upsert path.
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("INSERT INTO t VALUES (1, 'local')");
    }

    // The apply worker must upsert over the conflicting row
    // (ONCONFLICT_YB_REPLACE) instead of wedging on a duplicate-key error --
    // the same semantics that make crash replay of an already-applied INSERT
    // idempotent.
    execOnPublisher("INSERT INTO t VALUES (1, 'from-pub')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "from-pub")));
  }


  // ----- Group 6: Bulk operations -----

  @Test
  public void testBulkInsert() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    int rowCount = 500;
    try (Statement stmt = pubConn.createStatement()) {
      stmt.execute("INSERT INTO t SELECT g, 'val_' || g FROM generate_series(1, "
                   + rowCount + ") g");
    }

    waitForRowCount("t", rowCount);
  }

  // ----- Group 7: Constraints -----

  @Test
  public void testNotNullConstraint() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT NOT NULL)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'present')");
    waitForRows("SELECT id, val FROM t", Arrays.asList(row("1", "present")));
  }

  // ----- Group 8: Edge cases -----

  @Test
  public void testDeleteNonExistentRow() throws Exception {
    createReplicatedTable("t", stmt -> {
      stmt.execute("CREATE TABLE t (id INT PRIMARY KEY, val TEXT)");
    });

    execOnPublisher("INSERT INTO t VALUES (1, 'keep')");
    execOnPublisher("INSERT INTO t VALUES (2, 'remove')");
    waitForRowCount("t", 2);

    // Delete on subscriber directly to create divergence.
    try (Statement stmt = subConn.createStatement()) {
      stmt.execute("DELETE FROM t WHERE id = 2");
    }

    // Publisher also deletes the row - the apply worker will try to delete
    // a non-existent row.  This should not crash.
    execOnPublisher("DELETE FROM t WHERE id = 2");

    // A sentinel write after the missing-row DELETE: it can only replicate
    // once the worker has processed that DELETE, so its arrival proves the
    // worker advanced past it (a wedged retry loop would time out here)
    // rather than asserting a state that was already true before the DELETE.
    execOnPublisher("INSERT INTO t VALUES (3, 'sentinel')");
    waitForRows("SELECT id, val FROM t ORDER BY id",
                Arrays.asList(row("1", "keep"), row("3", "sentinel")));
  }
}
