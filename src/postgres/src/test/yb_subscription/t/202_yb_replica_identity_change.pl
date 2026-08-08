# Copyright (c) YugabyteDB, Inc.

# CHANGE is YugabyteDB's own replica identity and its default for new tables, but
# it cannot be streamed to a PG-style subscription: pgoutput rejects it
# (ValidateReplicaIdentities in yb_virtual_wal_client.c), and a subscription's
# slot is always created with "LOGICAL pgoutput"
# (libpqwalreceiver.c), so yboutput - the plugin that does support it - is not
# reachable through CREATE SUBSCRIPTION at all.
#
# The failure is not clean, which is what this test pins down.  The initial COPY
# succeeds, because copying rows needs no replica identity, and then streaming
# never starts: the relation stays in state 'f' forever while the worker errors
# and is restarted every few seconds.  Nothing raises an error to the client, so
# the only in-band evidence is the error counters in
# pg_stat_subscription_stats.
#
# This needs a YugabyteDB publisher; CHANGE does not exist on PostgreSQL.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

if (!($node_publisher->can('is_yb') && $node_publisher->is_yb))
{
	plan skip_all => 'replica identity CHANGE requires a YugabyteDB publisher';
}

$node_publisher->safe_psql('postgres', qq{
	CREATE TABLE tab_change (a int PRIMARY KEY, b text);
	ALTER TABLE tab_change REPLICA IDENTITY CHANGE;
	INSERT INTO tab_change VALUES (1, 'copied');
});

$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_change (a int PRIMARY KEY, b text)");

is($node_publisher->safe_psql('postgres',
		"SELECT relreplident FROM pg_class WHERE relname = 'tab_change'"),
	'c', 'publisher table has replica identity CHANGE');

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_change FOR TABLE tab_change");

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_change CONNECTION '$publisher_connstr' PUBLICATION tap_pub_change"
);

# The COPY needs no replica identity, so this much does work.  Deliberately not
# wait_for_subscription_sync: the relation never reaches 'r', so that would only
# time out.
$node_subscriber->poll_query_until('postgres',
	"SELECT count(*) = 1 FROM tab_change")
  or die "initial copy of a CHANGE table never arrived";
pass('initial copy of a CHANGE table reaches the subscriber');

# The error is invisible to the client, so the counters are the contract.
$node_subscriber->poll_query_until('postgres',
	"SELECT sum(apply_error_count + sync_error_count) > 0
	   FROM pg_stat_subscription_stats st
	   JOIN pg_subscription s ON s.oid = st.subid
	  WHERE s.subname = 'tap_sub_change'")
  or die "no error was recorded for a CHANGE table subscription";
pass('pg_stat_subscription_stats records the rejected replica identity');

# Streaming never starts, so a change made after the copy is not replicated and
# the relation does not reach 'r'.  Give the apply worker room to have done it.
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_change VALUES (2, 'streamed')");
sleep 15;

is($node_subscriber->safe_psql('postgres',
		"SELECT count(*) FROM tab_change"),
	1, 'changes after the copy are not streamed from a CHANGE table');

isnt($node_subscriber->safe_psql('postgres',
		"SELECT DISTINCT srsubstate FROM pg_subscription_rel r
		   JOIN pg_subscription s ON s.oid = r.srsubid
		  WHERE s.subname = 'tap_sub_change'"),
	'r', 'a CHANGE table never becomes ready');

# Leave nothing behind for the next test: the slot cannot be dropped by a
# subscription that never caught up, so release it first.
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub_change DISABLE");
$node_subscriber->safe_psql('postgres',
	"ALTER SUBSCRIPTION tap_sub_change SET (slot_name = NONE)");
$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_change");
$node_publisher->safe_psql('postgres',
	"SELECT pg_drop_replication_slot('tap_sub_change')");

done_testing();
