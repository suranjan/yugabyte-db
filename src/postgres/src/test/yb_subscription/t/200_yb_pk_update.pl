
# Copyright (c) YugabyteDB, Inc.

# A primary-key-changing UPDATE cannot be applied in place when the row's key is
# its storage key, so it is applied as a delete plus an insert - and an insert
# writes every column of the row, where an ordinary update writes only the
# columns the publisher sent.  Columns the publisher does not send must survive
# it: one that exists only on the subscriber, and an unchanged out-of-line TOAST
# value, which the protocol sends as "unchanged" rather than as data.
#
# This needs a PostgreSQL publisher.  A YugabyteDB publisher streams a key
# change as DELETE + INSERT, so the single UPDATE message exercised here never
# reaches the apply worker and no Java test can cover it.
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

# tab_extra_col has a column the publication cannot carry; tab_toast_col has one
# the publisher deliberately does not resend.  tab_inplace repeats both against
# an UPDATE that leaves the key alone, which is the case that must keep taking
# the write-only path.  tab_all_cols has neither, so the key change writes the
# whole row from the message alone and reads nothing.
$node_publisher->safe_psql('postgres', qq{
	CREATE TABLE tab_extra_col (a int PRIMARY KEY, b int);
	CREATE TABLE tab_toast_col (a int PRIMARY KEY, b int, big text);
	CREATE TABLE tab_inplace (a int PRIMARY KEY, b int, big text);
	CREATE TABLE tab_all_cols (a int PRIMARY KEY, b int, v text);
	ALTER TABLE tab_toast_col ALTER COLUMN big SET STORAGE EXTERNAL;
	ALTER TABLE tab_inplace ALTER COLUMN big SET STORAGE EXTERNAL;
	INSERT INTO tab_extra_col VALUES (1, 1);
	INSERT INTO tab_toast_col VALUES (1, 1, $big);
	INSERT INTO tab_inplace VALUES (1, 1, $big);
	INSERT INTO tab_all_cols VALUES (1, 1, 'keep');
});

# STORAGE EXTERNAL only takes effect above the tuple threshold, and the whole
# point of the test evaporates if the value ends up inline, so check it.
my $chunks = $node_publisher->safe_psql('postgres', q{
	SELECT count(*) FROM pg_toast.pg_toast_} . $node_publisher->safe_psql(
		'postgres', q{SELECT 'tab_toast_col'::regclass::oid}));
cmp_ok($chunks, '>', 0,
	'tab_toast_col.big is stored out of line, so it is sent as unchanged');

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE tab_extra_col, tab_toast_col, tab_inplace, tab_all_cols"
);

# c is not in the publication at all.  Its default differs from the value stored
# here, so "the default was filled in" is distinguishable from "the row was
# kept".  Every row is seeded on both sides so the subscription needs no copy.
$node_subscriber->safe_psql('postgres', qq{
	CREATE TABLE tab_extra_col (a int PRIMARY KEY, b int, c int DEFAULT 0);
	CREATE TABLE tab_toast_col (a int PRIMARY KEY, b int, big text);
	CREATE TABLE tab_inplace (a int PRIMARY KEY, b int, big text, c int DEFAULT 0);
	CREATE TABLE tab_all_cols (a int PRIMARY KEY, b int, v text);
	INSERT INTO tab_extra_col VALUES (1, 1, 99);
	INSERT INTO tab_toast_col VALUES (1, 1, $big);
	INSERT INTO tab_inplace VALUES (1, 1, $big, 99);
	INSERT INTO tab_all_cols VALUES (1, 1, 'keep');
});

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub CONNECTION '$publisher_connstr' PUBLICATION tap_pub WITH (copy_data = false)"
);
$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub');

my $md5 = $node_publisher->safe_psql('postgres',
	"SELECT md5(big) FROM tab_toast_col WHERE a = 1");

# ---------------------------------------------------------------------------
# The key change.
# ---------------------------------------------------------------------------
$node_publisher->safe_psql('postgres', q{
	UPDATE tab_extra_col SET a = 2 WHERE a = 1;
	UPDATE tab_toast_col SET a = 2 WHERE a = 1;
	UPDATE tab_all_cols SET a = 2 WHERE a = 1;
});
$node_publisher->wait_for_catchup('tap_sub');

is($node_subscriber->safe_psql('postgres',
		"SELECT a || '/' || b || '/' || coalesce(c::text, 'NULL') FROM tab_extra_col"),
	qq(2/1/99),
	'a key-changing UPDATE keeps a column that exists only on the subscriber');

is($node_subscriber->safe_psql('postgres',
		"SELECT a || '/' || b || '/' || coalesce(md5(big), 'NULL') FROM tab_toast_col"),
	qq(2/1/$md5),
	'a key-changing UPDATE keeps an unchanged TOASTed value');

# With nothing unsupplied the row is rebuilt from the message alone, so this is
# the key change that must come out right without reading the local row at all.
is($node_subscriber->safe_psql('postgres',
		"SELECT a || '/' || b || '/' || coalesce(v, 'NULL') FROM tab_all_cols"),
	qq(2/1/keep),
	'a key-changing UPDATE with every column supplied needs no local read');

# ---------------------------------------------------------------------------
# The same two columns, but the key stays put.
# ---------------------------------------------------------------------------
$node_publisher->safe_psql('postgres', "UPDATE tab_inplace SET b = 7 WHERE a = 1");
$node_publisher->wait_for_catchup('tap_sub');

is($node_subscriber->safe_psql('postgres',
		"SELECT a || '/' || b || '/' || coalesce(md5(big), 'NULL') || '/'
		        || coalesce(c::text, 'NULL') FROM tab_inplace"),
	qq(1/7/$md5/99),
	'an in-place UPDATE keeps both, without reading the local row');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
