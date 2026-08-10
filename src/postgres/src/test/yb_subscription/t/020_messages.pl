
# Copyright (c) 2021-2022, PostgreSQL Global Development Group
# Copyright (c) YugabyteDB, Inc.

# Tests that logical decoding messages
#
# YugabyteDB copy.  There is no storage for the PG WAL, so nothing
# pg_logical_emit_message() writes can reach a replication slot, and every
# assertion upstream makes about a message arriving on one is unreachable.  Both
# forms are therefore rejected, and what is left to test is that the rejection is
# an error rather than the crash the transactional form used to be:
# LogLogicalMessage() assigned an xid without marking the transaction as one that
# runs PostgreSQL's commit path, so the backend died on
# Assert(!TransactionIdIsValid(proc->xid)) in ProcArrayEndTransaction() and the
# postmaster died reaping it, restarting the whole instance.  In a release build
# the xid was instead left in the procarray after the transaction ended, so the
# backend_xid check below is the one that still means something without
# assertions.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

if (!($node_publisher->can('is_yb') && $node_publisher->is_yb))
{
	plan skip_all => 'needs a YugabyteDB publisher; see the comment at the top';
}

my $start_time =
  $node_publisher->safe_psql('postgres', 'SELECT pg_postmaster_start_time()');

my ($ret, $stdout, $stderr) = $node_publisher->psql(
	'postgres', qq(
	SELECT pg_logical_emit_message(false, 'pgoutput',
		'a non-transactional message')
	),
	on_error_stop => 1);

isnt($ret, 0, 'non-transactional message is rejected');

($ret, $stdout, $stderr) = $node_publisher->psql(
	'postgres', qq(
	SELECT pg_logical_emit_message(true, 'pgoutput', 'a transactional message')
	),
	on_error_stop => 1);

isnt($ret, 0, 'transactional message is rejected');
like(
	$stderr,
	qr/logical decoding messages are not supported/,
	'rejection names the unsupported feature');

# The session survives the error, and leaves no xid behind in the procarray.
# Both are checked from one connection that outlives the failed statement:
# before the fix there was no connection left to check from.
my $h = $node_publisher->background_psql('postgres', on_error_stop => 0);

$h->query(
	"SELECT pg_logical_emit_message(true, 'pgoutput', 'a transactional message')"
);
$h->{stderr} = '';

my $backend_xid = $h->query(
	'SELECT backend_xid IS NULL FROM pg_stat_activity WHERE pid = pg_backend_pid()'
);
is($h->{stderr}, '', 'session is still usable after the rejection');
is($backend_xid, 't', 'no xid is left in the procarray');

$h->quit;

is( $node_publisher->safe_psql(
		'postgres', 'SELECT pg_postmaster_start_time()'),
	$start_time,
	'the instance did not restart');

$node_publisher->stop;

done_testing();
