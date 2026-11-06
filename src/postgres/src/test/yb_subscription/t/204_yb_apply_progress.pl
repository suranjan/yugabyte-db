# Copyright (c) YugabyteDB, Inc.

# Transactions the publisher resends after an apply worker restart are skipped,
# not applied twice.  YB does not persist replication origin progress with the
# commit, so the worker records what it applied in the subscription progress
# table, in the same transaction, and skips anything at or below it.
#
# Feedback is withheld (yb_test_subscription_skip_feedback) so the publisher
# never learns what was applied and resends everything after a restart.  The
# replication origin is also rewound, as a subscriber restart would leave it:
# a PostgreSQL walsender otherwise honours the in-memory origin as the start
# point and resends nothing.  A resent INSERT would hit a duplicate key and
# wedge the worker, so a later change applying shows the resend was skipped.
#
# wait_for_catchup is not used: against a PostgreSQL publisher it waits for
# feedback, which this test withholds.  The subscriber's rows are polled
# instead.
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

if ($node_subscriber->safe_psql('postgres',
		"SELECT coalesce(current_setting('yb_test_subscription_skip_feedback', true), 'missing')")
	eq 'missing')
{
	plan skip_all => 'subscriber was not built with yb_test_subscription_skip_feedback';
}

# The master creates the progress table in the background, and only with
# ysql_yb_enable_pg_subscription; start_yb_clusters.sh sets it.
if ($node_subscriber->safe_psql('postgres',
		"SELECT count(*) FROM pg_database WHERE datname = 'yb_system'") ne '1')
{
	plan skip_all => 'no yb_system database: start the subscriber with ysql_yb_enable_pg_subscription';
}
$node_subscriber->poll_query_until('yb_system',
	"SELECT to_regclass('pg_yb_subscription_progress') IS NOT NULL")
  or die "yb_system.pg_yb_subscription_progress was never created";

my $sub = 'tap_sub_prog';

sub restart_worker
{
	my ($rewind_origin) = @_;
	$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION $sub DISABLE");
	$node_subscriber->poll_query_until('postgres',
		"SELECT count(*) = 0 FROM pg_stat_subscription WHERE subname = '$sub' AND pid IS NOT NULL")
	  or die "the apply worker did not exit";
	if ($rewind_origin)
	{
		$node_subscriber->safe_psql('postgres',
			"SELECT pg_replication_origin_advance('pg_' || oid, '0/0') "
			  . "FROM pg_subscription WHERE subname = '$sub'");
	}
	$node_subscriber->safe_psql('postgres', "ALTER SUBSCRIPTION $sub ENABLE");
}

sub confirmed_flush
{
	return $node_publisher->safe_psql('postgres',
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '$sub'");
}

$node_publisher->safe_psql('postgres', q{
	CREATE TABLE tab_prog (a int PRIMARY KEY, b text);
	CREATE PUBLICATION tap_pub_prog FOR TABLE tab_prog;
});
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tab_prog (a int PRIMARY KEY, b text)");

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_subscriber->safe_psql('postgres', qq{
	CREATE SUBSCRIPTION $sub CONNECTION '$publisher_connstr'
	  PUBLICATION tap_pub_prog WITH (copy_data = false);
});

# The worker reads database settings when it starts.
$node_subscriber->safe_psql('postgres',
	"ALTER DATABASE postgres SET yb_test_subscription_skip_feedback = on");
restart_worker(0);
my $flush_before = confirmed_flush();

$node_publisher->safe_psql('postgres', "INSERT INTO tab_prog VALUES (1, 'a')");
$node_publisher->safe_psql('postgres', "INSERT INTO tab_prog VALUES (2, 'b')");
$node_publisher->safe_psql('postgres', "UPDATE tab_prog SET b = 'c' WHERE a = 1");
$node_publisher->safe_psql('postgres', "DELETE FROM tab_prog WHERE a = 2");
$node_subscriber->poll_query_until('postgres',
	"SELECT string_agg(a || '/' || b, ',' ORDER BY a) = '1/c' FROM tab_prog")
  or die "the first transactions were not applied";
is(confirmed_flush(), $flush_before,
	'no feedback reached the publisher, so a restart resends these transactions');

restart_worker(1);
$node_publisher->safe_psql('postgres', "INSERT INTO tab_prog VALUES (3, 'd')");
ok( $node_subscriber->poll_query_until('postgres',
		"SELECT string_agg(a || '/' || b, ',' ORDER BY a) = '1/c,3/d' FROM tab_prog"),
	'a change after the restart applies, so the resent ones were skipped');

# Progress recorded since the first restart has to hold across a second one.
restart_worker(1);
$node_publisher->safe_psql('postgres', "INSERT INTO tab_prog VALUES (4, 'e')");
ok( $node_subscriber->poll_query_until('postgres',
		"SELECT string_agg(a || '/' || b, ',' ORDER BY a) = '1/c,3/d,4/e' FROM tab_prog"),
	'progress recorded after one restart holds across the next');

is($node_subscriber->safe_psql('postgres',
		"SELECT apply_error_count FROM pg_stat_subscription_stats WHERE subname = '$sub'"),
	'0', 'no resent transaction was applied a second time');

$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION $sub");
$node_subscriber->safe_psql('postgres',
	"ALTER DATABASE postgres RESET yb_test_subscription_skip_feedback");

done_testing();
