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

import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;

/**
 * A table rewrite on a YugabyteDB publisher must not re-send the table's rows.
 *
 * PG's decoding skips the rows a rewrite copies into the new heap.  A YB
 * publisher streams them again as INSERTs (see
 * TestPgReplicationSlot#testWithTableRewrite), so a subscriber that already
 * has those rows fails on a duplicate key, restarts, and retries the same
 * transaction forever.  TAP 031_column_list hangs this way on tab6.
 */
@Ignore("A YugabyteDB publisher re-sends the rows a table rewrite copies")
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionPublisherRewrite extends BasePgSubscriptionPublisherRewriteTest {
  // Keeps the replica identity DEFAULT across the rewrite, which a per-table
  // ALTER would not (TestPgSubscriptionPublisherRewriteReplicaIdentity).
  @Override
  protected Map<String, String> getTServerFlags() {
    Map<String, String> flagMap = super.getTServerFlags();
    flagMap.put("ysql_yb_default_replica_identity", "DEFAULT");
    appendToYsqlPgConf(flagMap, "yb_default_replica_identity=DEFAULT");
    return flagMap;
  }

  /** Rewrite without a key change: the re-sent rows collide on the same key. */
  @Test
  public void testColumnTypeChange() throws Exception {
    checkApplyAfterPublisherRewrite(
        new String[] {"ALTER TABLE t ALTER COLUMN v TYPE varchar(10)"},
        new String[] {});
  }

  /** 031_column_list's tab6: the primary key moves to another column on both sides. */
  @Test
  public void testPrimaryKeyChange() throws Exception {
    String[] movePk = {"ALTER TABLE t DROP CONSTRAINT t_pkey",
                       "ALTER TABLE t ADD PRIMARY KEY (v)"};
    checkApplyAfterPublisherRewrite(movePk, movePk);
  }

  private void checkApplyAfterPublisherRewrite(String[] pubDdl, String[] subDdl)
      throws Exception {
    createReplicatedTable();
    insertOnPublisherAndWait(1);
    insertOnPublisherAndWait(2);
    long errorsBefore = applyErrorCount();

    execute(pubConn, pubDdl);
    execute(subConn, subDdl);

    assertInsertApplies(3, errorsBefore);
    assertEquals("rows on the subscriber", 3, subCount("true"));
  }
}
