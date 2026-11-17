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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.YBTestRunner;

/**
 * Applying an UPDATE to a partitioned subscriber table whose partition does not
 * share the root's attribute numbers.
 *
 * A partition that has had a column dropped keeps a gap in its attribute
 * numbers, so its live columns sit at higher numbers than the root's.  The
 * apply worker builds the set of columns to write against the root, and the
 * partition it routes the row to has no range table entry of its own, so that
 * set has to be translated before it can be used against the partition.  When
 * it is not, YBCExecuteUpdate leaves the untranslated columns out of the write
 * and they silently keep their old values - no error, just stale data on the
 * subscriber.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionPartitionUpdate
    extends BasePgSubscriptionApplyTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestPgSubscriptionPartitionUpdate.class);

  private static final String TABLE = "parttest";
  private static final String LEAF = TABLE + "_1";

  /** Live columns of relation, as "name=attnum" ordered by attnum. */
  private String attnums(String relation) throws Exception {
    return scalar(subConn,
                  "SELECT string_agg(attname || '=' || attnum, ',' "
                  + "ORDER BY attnum) FROM pg_attribute "
                  + "WHERE attrelid = '" + relation + "'::regclass "
                  + "AND attnum > 0 AND NOT attisdropped");
  }

  @Test
  public void testUpdateIntoPartitionWithDroppedColumn() throws Exception {
    exec(pubConn, "CREATE TABLE " + TABLE + " (a int PRIMARY KEY, b int, c int)");
    // A YB table's replica identity defaults to CHANGE, which pgoutput refuses
    // to decode, so the subscription would never apply anything.
    exec(pubConn, "ALTER TABLE " + TABLE + " REPLICA IDENTITY DEFAULT");
    exec(pubConn, "INSERT INTO " + TABLE + " VALUES (1, 1, 1)");

    exec(subConn, "CREATE TABLE " + TABLE
                  + " (a int NOT NULL, b int, c int, PRIMARY KEY (a)) "
                  + "PARTITION BY LIST (a)");
    exec(subConn, "CREATE TABLE " + LEAF + " PARTITION OF " + TABLE
                  + " DEFAULT");

    // Push the partition's c past the root's highest attribute number, while
    // leaving the visible column order identical to the root's.  Detaching
    // first is what allows the partition alone to be altered.
    exec(subConn, "ALTER TABLE " + TABLE + " DETACH PARTITION " + LEAF);
    exec(subConn, "ALTER TABLE " + LEAF + " DROP COLUMN c");
    exec(subConn, "ALTER TABLE " + LEAF + " ADD COLUMN c int");
    exec(subConn, "ALTER TABLE " + TABLE + " ATTACH PARTITION " + LEAF
                  + " DEFAULT");

    String rootAttnums = attnums(TABLE);
    String leafAttnums = attnums(LEAF);
    LOG.info("root attnums: {}", rootAttnums);
    LOG.info("leaf attnums: {}", leafAttnums);
    assertEquals("the root should be unaffected by the partition's DDL",
                 "a=1,b=2,c=3", rootAttnums);
    assertEquals("this test is only meaningful while the partition's c sits "
                 + "past the root's highest attribute number",
                 "a=1,b=2,c=4", leafAttnums);

    // Seed the row the UPDATE will target instead of copying it: copy_data
    // leaves the table in tablesync state 's' against a YB publisher, and DML
    // arriving in that state is skipped as already covered by the copy, so the
    // UPDATE under test would never be applied.
    exec(subConn, "INSERT INTO " + TABLE + " VALUES (1, 1, 1)");
    createPubSub(TABLE);

    // One UPDATE touching a column on either side of the root's last attribute
    // number: b lands at the same number in both, c does not.
    exec(pubConn, "UPDATE " + TABLE + " SET b = 7, c = 9 WHERE a = 1");

    // b and c travel in the same UPDATE, so once b has arrived the write that
    // carried c has been applied too - waiting on b and then asserting c fails
    // fast with the actual value instead of timing out.
    waitForSubscriberValue("SELECT b::text FROM " + TABLE + " WHERE a = 1", "7");

    assertEquals("a replicated UPDATE must apply columns whose attribute "
                 + "number differs between the partition and its root",
                 "9",
                 scalar(subConn, "SELECT c::text FROM " + TABLE + " WHERE a = 1"));

    // Setting a column to NULL is the same write path, and the case
    // 013_partition covers: the new value has to overwrite, not be skipped.
    exec(pubConn, "UPDATE " + TABLE + " SET b = 8, c = NULL WHERE a = 1");
    waitForSubscriberValue("SELECT b::text FROM " + TABLE + " WHERE a = 1", "8");
    assertEquals("a replicated UPDATE must be able to set such a column to NULL",
                 null,
                 scalar(subConn, "SELECT c::text FROM " + TABLE + " WHERE a = 1"));
  }
}
