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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;

/**
 * An apply worker must see DDL that another session ran on the subscriber.
 *
 * With object locking off, a backend picks up a catalog version bump only in
 * PostgresMain's per-statement check, which a background worker never runs, so
 * the apply worker kept relation entries from before the DDL.  A replicated
 * change then wrote to a table the DDL had replaced, failed, and restarted the
 * worker; a replicated TRUNCATE (from a PostgreSQL publisher) instead hung on a
 * DocDB table the rewrite had already dropped.  Object locking is forced off
 * here so release builds, where it defaults on, still cover that mode.
 */
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionCatalogRefresh extends BasePgSubscriptionApplyTest {
  // The validators tie these together on the master and the tserver.
  private static void disableObjectLocking(Map<String, String> flagMap) {
    flagMap.put("enable_object_locking_for_table_locks", "false");
    flagMap.put("ysql_enable_concurrent_ddl", "false");
    flagMap.put("ysql_yb_ddl_transaction_block_enabled", "false");
    flagMap.put("ysql_yb_enable_ddl_savepoint_support", "false");
  }

  @Override
  protected Map<String, String> getTServerFlags() {
    Map<String, String> flagMap = super.getTServerFlags();
    disableObjectLocking(flagMap);
    return flagMap;
  }

  @Override
  protected Map<String, String> getMasterFlags() {
    Map<String, String> flagMap = super.getMasterFlags();
    disableObjectLocking(flagMap);
    return flagMap;
  }

  /** A subscriber-local TRUNCATE rewrites the table under a new DocDB id. */
  @Test
  public void testApplyAfterLocalTableRewrite() throws Exception {
    checkApplyAfterLocalDdl("TRUNCATE t");
  }

  /** ADD COLUMN keeps the DocDB table but bumps its schema version. */
  @Test
  public void testApplyAfterLocalAlterTable() throws Exception {
    checkApplyAfterLocalDdl("ALTER TABLE t ADD COLUMN extra int");
  }

  private void checkApplyAfterLocalDdl(String localDdl) throws Exception {
    // YB tables default to CHANGE, which pgoutput refuses to stream.
    createReplicatedTable("t", "DEFAULT", stmt -> {
      stmt.execute("CREATE TABLE t (k int PRIMARY KEY, v text)");
    });

    // Apply one row first, so the worker has the table open and cached.
    insertOnPublisherAndWait(1);
    int pid = applyWorkerPid(subConn, SUB_NAME);
    long errorsBefore = applyErrorCount();

    exec(subConn, localDdl);
    // Let the tserver learn the new catalog version, so the worker's next
    // transaction can see that it moved.
    waitForTServerHeartbeat();

    insertOnPublisherAndWait(2);
    assertEquals("the apply worker must not error on its stale catalog",
                 errorsBefore, applyErrorCount());
    assertEquals("the apply worker must not have restarted",
                 pid, applyWorkerPid(subConn, SUB_NAME));
  }

  private void insertOnPublisherAndWait(int k) throws Exception {
    execOnPublisher("INSERT INTO t (k, v) VALUES (" + k + ", 'v" + k + "')");
    waitForSubscriberValue("SELECT count(*) FROM t WHERE k = " + k, "1");
  }
}
