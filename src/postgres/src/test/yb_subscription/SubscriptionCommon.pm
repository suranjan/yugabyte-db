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
	# pg_subscription is a shared catalog, but DROP SUBSCRIPTION only reaches
	# the current database, so an unscoped list would try to drop other
	# databases' subscriptions and leave this one's behind.
	my $out = _run_sql($role, $db,
		'SELECT subname FROM pg_subscription
		 WHERE subdbid = (SELECT oid FROM pg_database
		                  WHERE datname = current_database())');
	for my $sub (grep { $_ ne '' } split /\n/, $out)
	{
		_run_sql($role, $db, qq{ALTER SUBSCRIPTION "$sub" DISABLE;});
		_run_sql($role, $db, qq{ALTER SUBSCRIPTION "$sub" SET (slot_name = NONE);});
		_run_sql($role, $db, qq{DROP SUBSCRIPTION "$sub";});
	}
	return;
}

# A transaction left prepared by a test that died holds its xid for as long as
# the cluster lives, and that blocks DROP DATABASE as well as every later
# CREATE_REPLICATION_SLOT, which waits for all running transactions to finish.
# One dead two-phase test would otherwise hang the whole rest of the suite.
sub _rollback_prepared
{
	my ($role, $db) = @_;
	my $out = _run_sql($role, $db,
		'SELECT gid FROM pg_prepared_xacts WHERE database = current_database()');
	for my $gid (grep { $_ ne '' } split /\n/, $out)
	{
		$gid =~ s/'/''/g;
		_run_sql($role, $db, qq{ROLLBACK PREPARED '$gid';});
	}
	return;
}

# Empty a database instead of recreating it, for a cluster that is not ours: a
# publisher we did not start may hold state we should not destroy wholesale, and
# DROP DATABASE fails there whenever any other session is still connected - a
# failure that is invisible, because errors are ignored here by design.  The
# recreated public schema is given PostgreSQL's default owner and ACL so that
# permission tests (027_nosuperuser) see what a fresh database would give them.
sub _clean_db_contents
{
	my ($role, $db) = @_;

	_run_sql($role, $db, <<"SQL");
DO \$\$
DECLARE
	obj text;
BEGIN
	FOR obj IN SELECT pubname FROM pg_publication LOOP
		EXECUTE format('DROP PUBLICATION %I', obj);
	END LOOP;
	FOR obj IN SELECT nspname FROM pg_namespace
	           WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema'
	LOOP
		EXECUTE format('DROP SCHEMA %I CASCADE', obj);
	END LOOP;
END \$\$;
CREATE SCHEMA public;
ALTER SCHEMA public OWNER TO pg_database_owner;
GRANT USAGE ON SCHEMA public TO PUBLIC;
SQL
	return;
}

# Dropping a subscription leaves nothing behind normally, but one that was cut
# off from its database (a crash mid-DROP, or DROP DATABASE that got through)
# leaves its origins in pg_replication_origin forever.  That catalog is shared,
# so 001_rep_changes' "SELECT count(*) FROM pg_replication_origin" sees them from
# any database and fails.  Origins can be dropped by name from anywhere, unlike
# the subscription itself, so clear the ones with no subscription left.
sub _drop_orphaned_origins
{
	my ($role, $db) = @_;
	_run_sql($role, $db,
		"SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin
		  WHERE roname ~ '^pg_[0-9]+'
		    AND split_part(roname, '_', 2)::oid
		        NOT IN (SELECT oid FROM pg_subscription)");
	return;
}

# pg_drop_replication_slot refuses a slot that is still active, and _run_sql
# swallows the error, so the old one-shot drop silently did nothing whenever a
# test died with its walsender still attached - and _drop_subscriptions has
# already set slot_name = NONE by then, so nothing else will ever reclaim it.
# Every later test that wants the same slot name then fails at
# CREATE_REPLICATION_SLOT with "already exists", which is how one slow test
# takes unrelated ones down with it.
#
# So detach the walsender first, and retry: the slot stays active until that
# backend actually exits.  Still scoped to this database, because the publisher
# may be shared and dropping someone else's slots would break them.
sub _drop_slots
{
	my ($role, $db) = @_;
	my $admin = _admin_db($role);

	for my $attempt (1 .. 10)
	{
		my $out = _run_sql($role, $admin,
			"SELECT slot_name FROM pg_replication_slots WHERE database = '$db'");
		return unless grep { $_ ne '' } split /\n/, $out;

		_run_sql($role, $admin,
			"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots
			 WHERE database = '$db' AND active_pid IS NOT NULL");
		# Attempt every slot rather than only the ones currently inactive: the
		# flag clears a moment after the backend goes, and errors here are
		# ignored anyway, so filtering only loses races.
		_run_sql($role, $admin,
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
			 WHERE database = '$db'");

		sleep 1;
	}

	# Say so rather than leaving the next test to fail on it mysteriously.
	my $left = _run_sql($role, $admin,
		"SELECT string_agg(slot_name, ', ') FROM pg_replication_slots WHERE database = '$db'");
	$left =~ s/\s+$//;
	print STDERR "warning: slots still on $role/$db after reset: $left\n"
	  if $left ne '';
	return;
}

sub _reset_db
{
	my ($role, $db) = @_;

	_drop_subscriptions($role, $db);
	_drop_orphaned_origins($role, $db);
	_rollback_prepared($role, $db);

	_drop_slots($role, $db);

	# The extra databases (_s1, _p2, ...) exist only for the tests, so those are
	# still recreated; only the database the tests actually name is emptied.
	if ($role eq 'pub' && $db eq $TEST_DB)
	{
		_clean_db_contents($role, $db);
		return;
	}

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

# Tests built around a feature the YugabyteDB subscriber does not have, so they
# cannot pass whatever the publisher is.  Report them as skipped, before the
# reset below touches either cluster, rather than let them die part-way.  Only
# whole-file gaps belong here; a test that merely touches one gets a copy in t/
# with just that part skipped (029_on_error).
my %YB_UNSUPPORTED = (
	'002_types.pl'           => 'a PRIMARY KEY over an array type',
	'005_encoding.pl'        => 'a non-UTF8 database encoding',
	'014_binary.pl'          => 'a PRIMARY KEY over an array type',
	'021_twophase.pl'        => 'PREPARE TRANSACTION',
	'023_twophase_stream.pl' => 'PREPARE TRANSACTION');

# The same for features a YugabyteDB publisher does not have.  A test that only
# partly depends on one skips just that part in its copy in t/ (013, 028).
my %YB_PUBLISHER_UNSUPPORTED = (
	'010_truncate.pl'               => 'replicating TRUNCATE (#29674)',
	'025_rep_changes_for_schema.pl' => 'publications FOR TABLES IN SCHEMA');

{
	my ($test) = $0 =~ m{([^/]+)$};
	my $reason;
	if (!defined $test)
	{
	}
	elsif ($ENV{YB_SUBSCRIBER_CONNSTR} && exists $YB_UNSUPPORTED{$test})
	{
		$reason = "the YugabyteDB subscriber does not support $YB_UNSUPPORTED{$test}";
	}
	elsif ($ENV{PG_PUBLISHER_CONNSTR}
		&& exists $YB_PUBLISHER_UNSUPPORTED{$test}
		&& _run_sql('pub', $TEST_DB, 'SELECT version()') =~ /-YB-/)
	{
		$reason =
		  "a YugabyteDB publisher does not support $YB_PUBLISHER_UNSUPPORTED{$test}";
	}
	if (defined $reason)
	{
		require Test::More;
		Test::More::plan(skip_all => $reason);
	}
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
