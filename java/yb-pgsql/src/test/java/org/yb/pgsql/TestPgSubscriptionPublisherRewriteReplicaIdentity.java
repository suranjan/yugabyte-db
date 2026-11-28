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

import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.yb.YBTestRunner;
import org.yb.client.TestUtils;

/**
 * A table rewrite on a YugabyteDB publisher must keep the table's replica
 * identity.
 *
 * The rewrite's new DocDB table takes its replica identity from
 * ysql_yb_default_replica_identity (CHANGE by default) instead of the table's
 * own, though pg_class still has the table's.  The walsender reads the DocDB
 * one, so after the rewrite pgoutput rejects the table and the subscriber's
 * worker fails on every reconnect.  The table is empty when rewritten, so the
 * rewrite has no rows to re-send (TestPgSubscriptionPublisherRewrite).
 */
@Ignore("A table rewrite on a YugabyteDB publisher resets the table's replica identity")
@RunWith(value = YBTestRunner.class)
public class TestPgSubscriptionPublisherRewriteReplicaIdentity
    extends BasePgSubscriptionPublisherRewriteTest {
  @Test
  public void testReplicaIdentityKeptAcrossRewrite() throws Exception {
    // YB tables default to CHANGE, which pgoutput refuses to stream.
    createReplicatedTable("ALTER TABLE t REPLICA IDENTITY DEFAULT");
    insertOnPublisherAndWait(1);
    execute(pubConn, "DELETE FROM t");
    TestUtils.waitFor(() -> subCount("true") == 0, APPLY_TIMEOUT_MS, POLL_INTERVAL_MS);
    long errorsBefore = applyErrorCount();

    execute(pubConn, "ALTER TABLE t ALTER COLUMN v TYPE varchar(10)");
    try (Statement pub = pubConn.createStatement();
         ResultSet rs = pub.executeQuery(
             "SELECT relreplident FROM pg_class WHERE oid = 't'::regclass")) {
      rs.next();
      assertEquals("pg_class keeps the replica identity", "d", rs.getString(1));
    }

    assertInsertApplies(2, errorsBefore);
  }
}
