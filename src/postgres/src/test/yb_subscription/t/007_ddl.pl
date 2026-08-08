
# Copyright (c) 2021-2022, PostgreSQL Global Development Group

# Test some logical replication DDL behavior
#
# YugabyteDB copy.  PostgreSQL resolves a subscription's publications lazily, in
# get_rel_sync_entry, so a missing one is only noticed when a change for some
# relation is actually about to be sent.  YugabyteDB resolves them eagerly when
# the virtual WAL is initialised, so START_REPLICATION fails outright and the
# apply worker never gets far enough to sync a table.  Upstream's first check
# leaves non_existent_pub missing and then waits for sync, which therefore waits
# forever.  Creating the publication once the warning has been observed lets the
# stream start while keeping the assertion intact.  The later checks need no
# such treatment: nothing waits on them.
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

my $yb_publisher = $node_publisher->can('is_yb') && $node_publisher->is_yb;

my $ddl = "CREATE TABLE test1 (a int, b text);";
$node_publisher->safe_psql('postgres', $ddl);
$node_subscriber->safe_psql('postgres', $ddl);

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION mypub FOR ALL TABLES;");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION mysub CONNECTION '$publisher_connstr' PUBLICATION mypub;"
);

$node_publisher->wait_for_catchup('mysub');

$node_subscriber->safe_psql(
	'postgres', q{
BEGIN;
ALTER SUBSCRIPTION mysub DISABLE;
ALTER SUBSCRIPTION mysub SET (slot_name = NONE);
DROP SUBSCRIPTION mysub;
COMMIT;
});

pass "subscription disable and drop in same transaction did not hang";

# One of the specified publications exists.
my ($ret, $stdout, $stderr) = $node_subscriber->psql('postgres',
	"CREATE SUBSCRIPTION mysub1 CONNECTION '$publisher_connstr' PUBLICATION mypub, non_existent_pub"
);
ok( $stderr =~
	  m/WARNING:  publication "non_existent_pub" does not exist on the publisher/,
	"Create subscription throws warning for non-existent publication");

# The warning above has already been observed, so the publication has served its
# purpose as a missing one.  Create it now: a YugabyteDB publisher refuses to
# start the stream while any of the subscription's publications is missing, and
# the sync below would never complete.
$node_publisher->safe_psql('postgres', "CREATE PUBLICATION non_existent_pub")
  if $yb_publisher;

# Wait for initial table sync to finish.
$node_subscriber->wait_for_subscription_sync($node_publisher, 'mysub1');

# Specifying non-existent publication along with add publication.
($ret, $stdout, $stderr) = $node_subscriber->psql('postgres',
	"ALTER SUBSCRIPTION mysub1 ADD PUBLICATION non_existent_pub1, non_existent_pub2"
);
ok( $stderr =~
	  m/WARNING:  publications "non_existent_pub1", "non_existent_pub2" do not exist on the publisher/,
	"Alter subscription add publication throws warning for non-existent publications"
);

# Specifying non-existent publication along with set publication.  This one uses
# a name of its own because non_existent_pub now exists.
($ret, $stdout, $stderr) = $node_subscriber->psql('postgres',
	"ALTER SUBSCRIPTION mysub1 SET PUBLICATION non_existent_pub3");
ok( $stderr =~
	  m/WARNING:  publication "non_existent_pub3" does not exist on the publisher/,
	"Alter subscription set publication throws warning for non-existent publication"
);

$node_subscriber->stop;
$node_publisher->stop;

done_testing();
