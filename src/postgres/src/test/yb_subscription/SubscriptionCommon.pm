# Copyright (c) 2021-2022, PostgreSQL Global Development Group
# Copyright (c) 2025, Yugabyte, Inc.
#
# When YB_SUBSCRIBER_CONNSTR is set, subscription TAP tests use an existing
# YugabyteDB (or other Postgres-compatible) cluster as the subscriber
# instead of starting one with initdb/pg_ctl.
# When PG_PUBLISHER_CONNSTR is set, the publisher is an existing Postgres
# (e.g. started by setup_pg_publisher.sh or a C++ test) instead of initdb/pg_ctl.
# This module patches Cluster->new() to return ExternalNode for those names.

package SubscriptionCommon;

use strict;
use warnings;

use IPC::Run ();

# Tests that use several nodes of the same role (013_partition has two
# subscribers, 100_bugs several publishers) cannot each get their own cluster
# here, so each node gets its own database on the one external cluster instead:
# a subscription and a publication both live in a single database, so the nodes
# stay independent.  '' keeps the database the tests actually name ('postgres').
my %SUB_SUFFIX = (
	'subscriber'  => '',
	'subscriber1' => '_s1',
	'subscriber2' => '_s2',
	'subscriber3' => '_s3');
my %PUB_SUFFIX = (
	'publisher'  => '',
	'publisher2' => '_p2',
	'publisher3' => '_p3');

# Database the tests connect to, and therefore the one we reset.
my $TEST_DB = 'postgres';

sub _psql_path
{
	my ($role) = @_;
	return $role eq 'pub'
	  ? ($ENV{PG_PUBLISHER_PSQL_PATH} || 'psql')
	  : ($ENV{YB_YSQLSH_PATH} || 'ysqlsh');
}

sub _base_connstr
{
	my ($role) = @_;
	return $role eq 'pub'
	  ? $ENV{PG_PUBLISHER_CONNSTR}
	  : $ENV{YB_SUBSCRIBER_CONNSTR};
}

# Database used to run DROP/CREATE DATABASE, which cannot run inside the
# database being dropped.
sub _admin_db
{
	my ($role) = @_;
	return 'template1' if $role eq 'pub';

	my $conn = $ENV{YB_SUBSCRIBER_CONNSTR} || '';
	my $db = ($conn =~ /\bdbname=(\S+)/) ? $1 : 'yugabyte';
	# The connstr may already point at the database we are about to drop.
	$db = 'yugabyte' if $db eq $TEST_DB;
	return $db;
}

sub _conn_with_db
{
	my ($conn, $db) = @_;
	$conn = '' unless defined $conn;
	$conn =~ s/\bdbname=\S+//g;
	$conn =~ s/^\s+|\s+$//g;
	return ($conn ne '' ? "$conn " : '') . "dbname=$db";
}

# Errors are deliberately ignored: these run against whatever a previous test
# left behind, so "database does not exist" and friends are expected.
sub _run_sql
{
	my ($role, $db, $sql) = @_;
	my $psql = _psql_path($role);
	my $conn = _conn_with_db(_base_connstr($role), $db);
	my ($out, $err);
	eval {
		IPC::Run::run([ $psql, '-XAtq', '-d', $conn, '-f', '-' ],
			'<', \$sql, '>', \$out, '2>', \$err);
	};
	return defined($out) ? $out : '';
}

sub _existing_test_dbs
{
	my ($role) = @_;
	my $out = _run_sql($role, _admin_db($role),
		"SELECT datname FROM pg_database WHERE datname LIKE '$TEST_DB%'");
	return grep { $_ ne '' } split /\n/, $out;
}

# Subscriptions must go before anything else: YugabyteDB refuses DROP DATABASE
# while the database still has one, and the publisher's slot has to be released
# (slot_name = NONE) because the subscription cannot drop a slot it can no
# longer reach.
sub _drop_subscriptions
{
	my ($role, $db) = @_;
	my $out = _run_sql($role, $db, 'SELECT subname FROM pg_subscription');
	for my $sub (grep { $_ ne '' } split /\n/, $out)
	{
		_run_sql($role, $db, qq{ALTER SUBSCRIPTION "$sub" DISABLE;});
		_run_sql($role, $db, qq{ALTER SUBSCRIPTION "$sub" SET (slot_name = NONE);});
		_run_sql($role, $db, qq{DROP SUBSCRIPTION "$sub";});
	}
	return;
}

sub _reset_db
{
	my ($role, $db) = @_;

	_drop_subscriptions($role, $db);

	# Scoped to this database on purpose: the publisher may be shared, and
	# dropping unrelated slots would break whoever owns them.
	_run_sql($role, _admin_db($role),
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '$db'"
	);

	_run_sql($role, _admin_db($role),
		qq{DROP DATABASE IF EXISTS "$db";\nCREATE DATABASE "$db";\n});
	return;
}

# Roles live in the cluster, not in a database, so recreating the database
# leaves them behind and the next run of a test that creates them fails with
# "role already exists" (027_nosuperuser).  Drop them after the databases, once
# the objects they owned are gone.
sub _drop_test_roles
{
	my ($role) = @_;
	my $out = _run_sql($role, _admin_db($role),
		"SELECT rolname FROM pg_roles WHERE rolname LIKE 'regress\\_%'");
	my @roles = grep { $_ ne '' } split /\n/, $out;
	return unless @roles;

	my @dbs = _existing_test_dbs($role);
	for my $r (@roles)
	{
		_run_sql($role, $_, qq{DROP OWNED BY "$r" CASCADE;}) for @dbs;
		_run_sql($role, _admin_db($role), qq{DROP ROLE IF EXISTS "$r";});
	}
	return;
}

# Discard settings appended by whichever test ran last.  A test that dies between
# an append_conf and its reset would otherwise hand its settings to every test
# after it - and the ones tests reach for are exactly the ones that change apply
# behaviour, so the damage is silent.
sub _reset_conf
{
	my ($role) = @_;
	my $connstr =
	  $role eq 'pub' ? $ENV{PG_PUBLISHER_CONNSTR} : $ENV{YB_SUBSCRIBER_CONNSTR};
	return unless $connstr;

	my $node = PostgreSQL::Test::ExternalNode->new($connstr, "${role}_confreset",
		psql_path => _psql_path($role));
	$node->reset_conf;
	return;
}

# Order matters across nodes: every subscription anywhere has to be gone before
# the publisher's slots can be dropped, otherwise the slots are still active.
if ($ENV{YB_SUBSCRIBER_CONNSTR})
{
	_drop_subscriptions('sub', $_) for _existing_test_dbs('sub');
}
_reset_db('pub', $TEST_DB) if $ENV{PG_PUBLISHER_CONNSTR};
_reset_db('sub', $TEST_DB) if $ENV{YB_SUBSCRIBER_CONNSTR};
_drop_test_roles('pub')    if $ENV{PG_PUBLISHER_CONNSTR};
_drop_test_roles('sub')    if $ENV{YB_SUBSCRIBER_CONNSTR};
_reset_conf('pub')         if $ENV{PG_PUBLISHER_CONNSTR};
_reset_conf('sub')         if $ENV{YB_SUBSCRIBER_CONNSTR};

# Load Cluster first so we can override its new()
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ExternalNode;

my $orig_cluster_new = \&PostgreSQL::Test::Cluster::new;

no warnings 'redefine';
*PostgreSQL::Test::Cluster::new = sub {
	my ($class, $name, @rest) = @_;

	if (defined($name) && exists($PUB_SUFFIX{$name}) && $ENV{PG_PUBLISHER_CONNSTR})
	{
		my $suffix = $PUB_SUFFIX{$name};
		# The extra databases are only reset when a test actually asks for the
		# node, so single-node tests do not pay for them.
		_reset_db('pub', $TEST_DB . $suffix) if $suffix ne '';
		return PostgreSQL::Test::ExternalNode->new(
			$ENV{PG_PUBLISHER_CONNSTR}, $name,
			psql_path => $ENV{PG_PUBLISHER_PSQL_PATH} || 'psql',
			db_suffix => $suffix);
	}
	if (defined($name) && exists($SUB_SUFFIX{$name}) && $ENV{YB_SUBSCRIBER_CONNSTR})
	{
		my $suffix = $SUB_SUFFIX{$name};
		_reset_db('sub', $TEST_DB . $suffix) if $suffix ne '';
		return PostgreSQL::Test::ExternalNode->new(
			$ENV{YB_SUBSCRIBER_CONNSTR}, $name,
			db_suffix => $suffix);
	}
	return $orig_cluster_new->($class, $name, @rest);
};

1;
