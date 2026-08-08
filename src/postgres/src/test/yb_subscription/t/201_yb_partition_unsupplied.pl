
# Copyright (c) YugabyteDB, Inc.

# Two shapes around applying an UPDATE to a partition, both needing a column to
# arrive unsupplied.  A PostgreSQL publisher produces that only for an unchanged
# out-of-line TOAST value - it sends every other column - which is why case 1
# uses one.  A YugabyteDB publisher omits every column an UPDATE did not touch
# under its default replica identity CHANGE, so it reaches these shapes far more
# easily; the Java suite cannot, because the harness has to set REPLICA IDENTITY
# FULL for pgoutput.
#
# 1. A leaf partition published directly, with an out-of-line column the UPDATE
#    leaves alone.  The target is a partition rather than the partitioned root,
#    which is the case the root-level analysis of what the change needs cannot
#    speak for: a partition validates the row against its own bound, and may
#    carry constraints its parent does not.  Both must hold with the column
#    missing from the change.
#
# 2. A partitioned table with a text primary key, published through the root so
#    the apply routes the tuple.  Routing hands the worker a virtual slot while
#    the located row is a heap tuple, so the same key value can differ byte for
#    byte between them - a short varlena picks up a one-byte header once it has
#    been through heap_form_tuple.  Comparing payloads instead of raw datums
#    keeps that from reading as a changed key, which would apply the update as a
#    delete plus an insert.
#
# Neither case fails on the code these tests were written against: the columns
# left out of a partial write survive on their own, and a needless delete plus
# insert still lands the same row.  They are here to hold that ground - the first
# against a future widening of the write-only fast path, the second against the
# key comparison drifting back to raw datums.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# 6400 characters, deterministic so both sides can seed the same value, and
# poorly compressible so it is stored out of line rather than inline.
my $big = q{(SELECT string_agg(md5(g::text), '') FROM generate_series(1,200) g)};

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->start;

# The premise above needs a PostgreSQL publisher, so this is not a YugabyteDB
# limitation to work around: on a YugabyteDB publisher the message shape under
# test is never produced, and pg_toast does not exist to confirm the setup.
if ($node_publisher->can('is_yb') && $node_publisher->is_yb)
{
	plan skip_all => 'needs a PostgreSQL publisher; see the comment at the top';
}

# part_leaf carries nothing that would force the local row to be read on its own
# account - no secondary index, no trigger, no check constraint, and the column
# left out of the update is nullable - so the only reason to read it is that the
# target is a partition.  part_textpk is keyed by text.
my $ddl = q{
	CREATE TABLE part_leaf (id int PRIMARY KEY, v int, big text)
	  PARTITION BY RANGE (id);
	CREATE TABLE part_leaf_1 PARTITION OF part_leaf
	  FOR VALUES FROM (1) TO (100);
	CREATE TABLE part_textpk (k text PRIMARY KEY, v int, big text)
	  PARTITION BY RANGE (k);
	CREATE TABLE part_textpk_1 PARTITION OF part_textpk
	  FOR VALUES FROM ('a') TO ('z');
};

$node_publisher->safe_psql('postgres', $ddl);
$node_publisher->safe_psql('postgres', qq{
	ALTER TABLE part_leaf ALTER COLUMN big SET STORAGE EXTERNAL;
	ALTER TABLE part_textpk ALTER COLUMN big SET STORAGE EXTERNAL;
	INSERT INTO part_leaf VALUES (1, 10, $big);
	INSERT INTO part_textpk VALUES ('alpha', 1, $big);
});

# STORAGE EXTERNAL only takes effect above the tuple threshold, and both cases
# evaporate if the value ends up inline, so check it.
my $chunks = $node_publisher->safe_psql('postgres', q{
	SELECT count(*) FROM pg_toast.pg_toast_} . $node_publisher->safe_psql(
		'postgres', q{SELECT 'part_leaf_1'::regclass::oid}));
cmp_ok($chunks, '>', 0,
	'part_leaf_1.big is stored out of line, so it is sent as unchanged');

# The leaf is published directly, so the apply target is the partition itself;
# the root is published via partition root, so that apply routes the tuple.
$node_publisher->safe_psql('postgres', q{
	CREATE PUBLICATION tap_pub_leaf FOR TABLE part_leaf_1;
	CREATE PUBLICATION tap_pub_root FOR TABLE part_textpk
	  WITH (publish_via_partition_root = true);
});

# Seeded on both sides so the subscription needs no copy.  c has no counterpart
# on the publisher: filling in its default is distinguishable from keeping the
# row.
$node_subscriber->safe_psql('postgres', $ddl);
$node_subscriber->safe_psql('postgres', qq{
	ALTER TABLE part_textpk ADD COLUMN c int DEFAULT 0;
	INSERT INTO part_leaf VALUES (1, 10, $big);
	INSERT INTO part_textpk VALUES ('alpha', 1, $big, 99);
});

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_leaf CONNECTION '$publisher_connstr' PUBLICATION tap_pub_leaf WITH (copy_data = false)"
);
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_root CONNECTION '$publisher_connstr' PUBLICATION tap_pub_root WITH (copy_data = false)"
);
$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_leaf');
$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_root');

my $md5_leaf = $node_publisher->safe_psql('postgres',
	"SELECT md5(big) FROM part_leaf WHERE id = 1");
my $md5_text = $node_publisher->safe_psql('postgres',
	"SELECT md5(big) FROM part_textpk WHERE k = 'alpha'");

# ---------------------------------------------------------------------------
# An unsupplied column, applied to a leaf partition.
# ---------------------------------------------------------------------------
$node_publisher->safe_psql('postgres',
	"UPDATE part_leaf_1 SET v = 20 WHERE id = 1");

# Poll rather than wait_for_catchup: a row the partition rejects fails the apply
# and the worker retries instead of advancing, which would hang a catchup wait
# rather than report anything.
my $applied = $node_subscriber->poll_query_until('postgres',
	"SELECT v = 20 FROM part_leaf_1 WHERE id = 1", 't');
ok($applied, 'an UPDATE with an unsupplied column applies to a leaf partition');

is($node_subscriber->safe_psql('postgres',
		"SELECT v || '/' || coalesce(md5(big), 'NULL') FROM part_leaf_1 WHERE id = 1"),
	qq(20/$md5_leaf),
	'the unsupplied column keeps its stored value on the partition');

# ---------------------------------------------------------------------------
# A text primary key, reached through tuple routing.
# ---------------------------------------------------------------------------
$node_publisher->safe_psql('postgres',
	"UPDATE part_textpk SET v = 2 WHERE k = 'alpha'");
$node_publisher->wait_for_catchup('tap_sub_root');

is($node_subscriber->safe_psql('postgres',
		"SELECT k || '/' || v || '/' || coalesce(md5(big), 'NULL') || '/'
		        || coalesce(c::text, 'NULL') FROM part_textpk"),
	qq(alpha/2/$md5_text/99),
	'a non-key UPDATE on a text-keyed partition keeps every other column');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
