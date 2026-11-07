# Copyright (c) 2021-2022, PostgreSQL Global Development Group
# Copyright (c) 2025, Yugabyte, Inc.
#
# ExternalNode - node interface for an existing DB (e.g. YugabyteDB subscriber)
# Used when subscription TAP tests run with YB_SUBSCRIBER_CONNSTR instead of
# starting a second Postgres cluster with initdb/pg_ctl.

package PostgreSQL::Test::ExternalNode;

use strict;
use warnings;

use Carp;
use File::Temp;
use IPC::Run;
use Time::HiRes qw(usleep);

use PostgreSQL::Test::Utils;

=head1 NAME

PostgreSQL::Test::ExternalNode - connection handle for an existing database node

=head1 SYNOPSIS

  use PostgreSQL::Test::ExternalNode;

  # When YB_SUBSCRIBER_CONNSTR is set, subscription tests use this instead
  # of starting a subscriber with initdb/pg_ctl:
  my $node = PostgreSQL::Test::ExternalNode->new(
    $ENV{YB_SUBSCRIBER_CONNSTR}, 'subscriber');

  $node->init();   # no-op
  $node->start();  # no-op
  $node->safe_psql('postgres', 'SELECT 1');
  $node->wait_for_subscription_sync($node_publisher, 'tap_sub');

=head1 DESCRIPTION

Implements the subset of PostgreSQL::Test::Cluster interface needed for
a logical replication subscriber when the subscriber is an existing
database (e.g. YugabyteDB) rather than a cluster started by the test.
Callers can use init(), start(), append_conf() as no-ops so the same
test code works with either Cluster or ExternalNode.

=cut

# Every external node, so a YugabyteDB publisher's wait_for_catchup can reach
# the subscriber: apply progress against such a publisher is only observable
# there, and wait_for_catchup is a method on the publisher.
my @ALL_NODES;

sub new
{
	my ($class, $connstr, $name, %opts) = @_;
	croak 'connstr required' unless defined($connstr) && $connstr ne '';
	$name = 'subscriber' unless defined($name);
	my $self = {
		_connstr => $connstr,
		_name    => $name,
		_psql    => $opts{psql_path} || $ENV{YB_YSQLSH_PATH} || 'psql',
		# Tests with more than one subscriber (013_partition) or publisher get
		# one database per node on the single external cluster.  A subscription
		# is per-database, so that keeps the nodes independent.
		_db_suffix => defined($opts{db_suffix}) ? $opts{db_suffix} : '',
	};
	bless $self, $class;
	$self->_arm_log_baseline;
	push @ALL_NODES, $self;
	return $self;
}

sub name
{
	my ($self) = @_;
	return $self->{_name};
}

sub init
{
	my ($self, %params) = @_;
	# No-op: node already exists
	return;
}

sub start
{
	my ($self) = @_;
	# No-op: node already running
	return;
}

sub stop
{
	my ($self, $mode) = @_;
	# No-op: we do not control the node
	return;
}

# Append to the server's own config file when we can reach it, so settings the
# tests rely on actually take effect.  $filename is ignored: the server is asked
# where it reads its configuration (on YugabyteDB that is ysql_pg.conf, not
# postgresql.conf).  Only settings that can be applied by SIGHUP will take hold;
# a PGC_POSTMASTER setting still needs a restart we do not control, so callers
# depending on one (005_encoding's encoding, 021_twophase's
# max_prepared_transactions) continue to get the running value.
sub _config_file
{
	my ($self) = @_;

	my $cfg = $self->{_configfile};
	if (!defined $cfg)
	{
		$cfg = eval { $self->safe_psql('postgres', 'SHOW config_file') };
		$cfg = '' unless defined $cfg;
		$self->{_configfile} = $cfg;
	}
	return $cfg;
}

# Appends land in a side file included from the main config rather than in the
# main config itself, so reset_conf below can drop a whole test's settings by
# truncating one file.  Appending straight to the main config would be a one-way
# door: a test that dies between setting something and resetting it leaves that
# setting behind for every test that follows.
sub yb_overrides_file
{
	my ($self) = @_;

	my $cfg = $self->_config_file;
	return '' if $cfg eq '';
	(my $dir = $cfg) =~ s{/[^/]*$}{};
	return "$dir/yb_tap_overrides.conf";
}

sub append_conf
{
	my ($self, $filename, $line) = @_;

	my $cfg = $self->_config_file;
	my $ovr = $self->yb_overrides_file;

	if ($cfg eq '' || !-w $cfg)
	{
		carp 'append_conf ignored on external node "' . $self->name
		  . "\" (no writable config file): $line";
		return;
	}

	# Wire up the include once.  Re-checked on every call because the server may
	# regenerate its config (yugabyted rewrites ysql_pg.conf on start).
	my $include = "include_if_exists 'yb_tap_overrides.conf'";
	my $wired   = 0;
	if (open(my $in, '<', $cfg))
	{
		while (<$in>)
		{
			if (index($_, $include) >= 0) { $wired = 1; last; }
		}
		close $in;
	}
	if (!$wired)
	{
		open(my $fh, '>>', $cfg)
		  or do { carp "could not append to $cfg: $!"; return; };
		print $fh "$include\n";
		close $fh;
	}

	open(my $fh, '>>', $ovr)
	  or do { carp "could not append to $ovr: $!"; return; };
	print $fh "$line\n";
	close $fh;

	# Upstream scripts append before start() and never reload, because for a
	# cluster they own that is enough - even a postmaster-context setting is in
	# effect once it starts.  Here the server is already running and start() is
	# a no-op, so apply it now, and say so when we cannot: a test silently
	# running with settings it asked for and did not get is worse than a failure
	# (015-019 spent a whole baseline run not actually streaming).
	$self->reload;
	$self->_warn_settings_not_applied($line);
	return;
}

# Complain about any setting in $line that the running server cannot adopt.
sub _warn_settings_not_applied
{
	my ($self, $line) = @_;

	for my $l (split /\n/, $line)
	{
		next if $l =~ /^\s*(#|$)/;
		my ($name) = $l =~ /^\s*([A-Za-z_][A-Za-z0-9_.]*)\s*=/ or next;

		my $ctx = eval {
			$self->safe_psql('postgres',
				"SELECT coalesce(max(context), '') FROM pg_settings WHERE name = '$name'");
		};
		next unless defined $ctx;

		if ($ctx eq '')
		{
			carp 'unknown setting "' . $name . '" on external node "'
			  . $self->name . '"';
		}
		elsif ($ctx eq 'postmaster' || $ctx eq 'internal')
		{
			carp 'setting "' . $name . '" needs a restart (' . $ctx
			  . ' context) and this node is not ours to restart, so "' . $l
			  . '" has no effect on "' . $self->name . '"';
		}
	}
	return;
}

# Drop every setting previous tests appended.  SubscriptionCommon calls this at
# load time, next to the database and role reset, so each test starts from the
# cluster's own configuration however the last one ended.
sub reset_conf
{
	my ($self) = @_;

	my $ovr = $self->yb_overrides_file;
	return if $ovr eq '';
	return unless -e $ovr;

	open(my $fh, '>', $ovr)
	  or do { carp "could not truncate $ovr: $!"; return; };
	close $fh;
	$self->reload;
	return;
}

# Ask the running server to re-read its configuration.  This is what makes an
# append_conf above visible to backends that are already running, including the
# apply worker (013_partition raises log_min_messages to debug1 mid-test and then
# greps the log for what the apply worker wrote).
sub reload
{
	my ($self) = @_;
	eval { $self->safe_psql('postgres', 'SELECT pg_reload_conf()') };
	return;
}

# Resolved once and cached: wait_for_log tracks byte offsets, which only hold
# if every call reads the same file.  A server that rotates its log mid-test
# will therefore go unnoticed.
# Where the server says it logs, or '' if it will not say.  Quiet, so arming the
# mirror at construction does not warn about nodes whose log the test never reads.
sub _resolve_server_logfile
{
	my ($self) = @_;

	# Both YugabyteDB and Postgres answer when the logging collector is on.
	my $path = eval {
		$self->safe_psql('postgres',
			"SELECT coalesce(pg_current_logfile(), '')");
	};
	return '' unless defined($path) && $path ne '';

	if ($path !~ m{^/})
	{
		my $datadir = eval { $self->safe_psql('postgres', 'SHOW data_directory') };
		$path = "$datadir/$path" if defined($datadir) && $datadir ne '';
	}
	return -r $path ? $path : '';
}

# Fix the point the mirror starts from.  Called at construction: a test reads the
# log after the event it cares about, so waiting until the first read would put
# the baseline past the very output the test is looking for.
sub _arm_log_baseline
{
	my ($self) = @_;
	return if defined $self->{_logsnap};

	my $src = $self->_resolve_server_logfile;
	return if $src eq '';
	$self->{_logfile} = $src;

	my $dir = PostgreSQL::Test::Utils::tempdir();
	my $snap = "$dir/" . $self->name . '_server.log';
	open(my $out, '>', $snap) or return;
	close $out;

	$self->{_logsnap}  = $snap;
	$self->{_logtaken} = (-s $src) || 0;
	return;
}

# The server's real log file.  Resolved once: log offsets need a stable path.
sub _server_logfile
{
	my ($self) = @_;
	return $self->{_logfile} if defined $self->{_logfile};

	my $path = $self->_resolve_server_logfile;
	if ($path ne '')
	{
		$self->{_logfile} = $path;
		return $path;
	}

	# Fall back to an empty temp file so tests that merely open the log still
	# run; anything grepping it will not match.
	my ($fh, $tmp) = File::Temp::tempfile(UNLINK => 0, SUFFIX => '.log');
	close $fh;
	carp 'no server log available for external node "' . $self->name
	  . '"; using empty ' . $tmp;
	$self->{_logfile} = $tmp;
	return $tmp;
}

# The log the tests are allowed to see: everything this node has written since
# the first time the running test asked for it.
#
# Upstream scripts start from offset 0 of a log their own initdb created moments
# earlier, so offset 0 means "the beginning of my output".  An external server's
# log is one file shared by every test and every previous run, and reading it
# from 0 hands a test somebody else's output - 029_on_error picked up an error
# LSN from an earlier run and skipped the wrong transaction.  Mirroring the
# per-test log into a file that begins where the test did restores what those
# offsets are supposed to mean.
sub logfile
{
	my ($self) = @_;

	# Normally armed at construction; do it now if that did not get a log path
	# (a node whose logging collector was off when the test started).
	$self->_arm_log_baseline;
	my $snap = $self->{_logsnap};
	return $self->_server_logfile if !defined $snap;

	my $src = $self->_server_logfile;

	my $size = (-s $src) || 0;
	# A rotated or truncated log restarts the mirror rather than seeking past
	# the end of the new file.
	$self->{_logtaken} = 0 if $size < $self->{_logtaken};

	if ($size > $self->{_logtaken})
	{
		if (open(my $in, '<', $src))
		{
			if (open(my $out, '>>', $snap))
			{
				binmode $in;
				binmode $out;
				seek($in, $self->{_logtaken}, 0);
				my $buf;
				while (read($in, $buf, 1 << 16)) { print $out $buf; }
				close $out;
				$self->{_logtaken} = $size;
			}
			close $in;
		}
	}

	return $snap;
}

sub log_content
{
	my ($self, $offset) = @_;
	$offset = 0 unless defined $offset;
	return PostgreSQL::Test::Utils::slurp_file($self->logfile, $offset);
}

sub wait_for_log
{
	my ($self, $regexp, $offset) = @_;
	$offset = 0 unless defined $offset;

	my $max_attempts = 10 * $PostgreSQL::Test::Utils::timeout_default;
	my $attempts     = 0;

	while ($attempts < $max_attempts)
	{
		my $log =
		  PostgreSQL::Test::Utils::slurp_file($self->logfile, $offset);

		return $offset + length($log) if ($log =~ m/$regexp/);

		# Wait 0.1 second before retrying.
		usleep(100_000);

		$attempts++;
	}

	croak "timed out waiting for match: $regexp";
}

sub connstr
{
	my ($self, $dbname) = @_;
	my $connstr = $self->{_connstr};
	if (defined($dbname) && $dbname ne '')
	{
		# Single funnel for every query this node runs, so mapping the name
		# here is what gives each node its own database.
		$dbname .= $self->{_db_suffix};
		$dbname =~ s#\\#\\\\#g;
		$dbname =~ s#\'#\\\'#g;
		$connstr = $connstr . " dbname='$dbname'";
	}
	return $connstr;
}

sub safe_psql
{
	my ($self, $dbname, $sql, %params) = @_;
	$sql = $self->_yb_disable_before_drop($sql);
	my $connstr = $self->connstr($dbname);
	my @cmd = ($self->{_psql}, '-XAtq', '-d', $connstr, '-v', 'ON_ERROR_STOP=1', '-f', '-');
	my ($stdout, $stderr);
	my $ret = IPC::Run::run \@cmd, '<', \$sql, '>', \$stdout, '2>', \$stderr;
	if (!$ret || $?)
	{
		croak "psql failed: stderr=$stderr stdout=$stdout";
	}
	if ($stderr ne '')
	{
		print "#### Begin standard error\n";
		print $stderr;
		print "\n#### End standard error\n";
	}
	# Normalize trailing newline so callers get the same shape as with Cluster (tests expect no trailing \n).
	chomp($stdout) if defined($stdout);
	return $stdout;
}

sub psql
{
	my ($self, $dbname, $sql, %params) = @_;
	$sql = $self->_yb_disable_before_drop($sql);
	my $connstr = $self->connstr($dbname);
	my @cmd = ($self->{_psql}, '-XAtq', '-d', $connstr, '-f', '-');
	push @cmd, '-v', 'ON_ERROR_STOP=1' if $params{on_error_stop};
	my ($stdout_ref, $stderr_ref) = (\my $out, \my $err);
	$stdout_ref = $params{stdout} if defined $params{stdout};
	$stderr_ref = $params{stderr} if defined $params{stderr};
	my $ret = IPC::Run::run \@cmd, '<', \$sql, '>', $stdout_ref, '2>', $stderr_ref;
	my $exit_code = $? >> 8;
	if (wantarray)
	{
		return ($exit_code, $$stdout_ref, $$stderr_ref);
	}
	return $exit_code;
}

sub background_psql
{
	my ($self, $dbname, %params) = @_;

	require PostgreSQL::Test::BackgroundPsql;

	my $replication = $params{replication};
	my $timeout     = $params{timeout};

	my @psql_params = (
		$self->{_psql},
		'-XAtq',
		'-d',
		$self->connstr($dbname)
		  . (defined $replication ? " replication=$replication" : ''),
		'-f',
		'-');

	$params{on_error_stop} = 1 unless defined $params{on_error_stop};
	push @psql_params, '-v', 'ON_ERROR_STOP=1' if $params{on_error_stop};
	push @psql_params, @{ $params{extra_params} }
	  if defined $params{extra_params};

	return PostgreSQL::Test::ExternalNode::PsqlFactory->make(\@psql_params,
		$timeout);
}

sub poll_query_until
{
	my ($self, $dbname, $query, $expected) = @_;
	$expected = 't' unless defined($expected);
	my $max_attempts = 10 * $PostgreSQL::Test::Utils::timeout_default;
	my $attempts     = 0;
	my ($stdout, $stderr);

	while ($attempts < $max_attempts)
	{
		my $connstr = $self->connstr($dbname);
		my @cmd = ($self->{_psql}, '-XAt', '-d', $connstr);
		my $result = IPC::Run::run \@cmd, '<', \$query, '>', \$stdout, '2>', \$stderr;
		chomp($stdout);
		chomp($stderr);
		if ($result && $stdout eq $expected && $stderr eq '')
		{
			return 1;
		}
		usleep(100_000);
		$attempts++;
	}
	return 0;
}

sub lsn
{
	my ($self, $mode) = @_;
	my %modes = (
		'insert'  => 'pg_current_wal_insert_lsn()',
		'flush'   => 'pg_current_wal_flush_lsn()',
		'write'   => 'pg_current_wal_lsn()',
		'receive' => 'pg_last_wal_receive_lsn()',
		'replay'  => 'pg_last_wal_replay_lsn()');
	croak "unknown mode for 'lsn': '$mode'" unless defined($modes{$mode});
	my $result = $self->safe_psql('postgres', "SELECT $modes{$mode}");
	chomp($result);
	return $result eq '' ? undef : $result;
}

# Public form, for tests that must adapt to a YugabyteDB publisher.  Call it as
# $node->can('is_yb') && $node->is_yb, so the same test still runs against a real
# PostgreSQL::Test::Cluster publisher, which has no such method.
sub is_yb
{
	my ($self) = @_;
	return $self->_is_yb;
}

# Same test the apply worker applies to its own publisher (worker.c).
sub _is_yb
{
	my ($self) = @_;
	unless (exists $self->{_is_yb})
	{
		my $version = $self->safe_psql('postgres', 'SELECT version()');
		$self->{_is_yb} = (index($version, '-YB-') >= 0) ? 1 : 0;
	}
	return $self->{_is_yb};
}

sub _quote_literal
{
	my ($str) = @_;
	$str =~ s#\'#\'\'#g;
	return "'$str'";
}

# YugabyteDB refuses DROP SUBSCRIPTION while the subscription is enabled
# (subscriptioncmds.c), and the upstream tests drop enabled ones.  Disable it
# first, in its own statement so the DISABLE commits before the drop.  The DO
# block keeps DROP SUBSCRIPTION IF EXISTS working for a missing subscription.
sub _yb_disable_before_drop
{
	my ($self, $sql) = @_;
	return $sql unless defined $sql && $sql =~ /\bDROP\s+SUBSCRIPTION\b/i;
	return $sql unless $self->_is_yb;
	$sql =~ s{(\bDROP\s+SUBSCRIPTION\s+(?:IF\s+EXISTS\s+)?("(?:[^"]|"")+"|\w+))}{
		my ($stmt, $ident) = ($1, $2);
		my $name = lc($ident);
		($name = substr($ident, 1, -1)) =~ s/""/"/g if $ident =~ /^"/;
		my $lit = _quote_literal($name);
		(my $quoted = $name) =~ s/"/""/g;
		qq{DO \$yb\$ BEGIN IF EXISTS (SELECT 1 FROM pg_subscription WHERE subname = $lit }
		  . qq{AND subenabled AND subdbid = (SELECT oid FROM pg_database WHERE datname = current_database())) }
		  . qq{THEN ALTER SUBSCRIPTION "$quoted" DISABLE; END IF; END \$yb\$;\n$stmt}
	}gie;
	return $sql;
}

# Identify the subscription the way the upstream path does, by application_name:
# it is not always the subscription name (021_twophase gives subscription
# 'tap_sub_copy' the application_name 'appname_copy').  Anchored on the spaces
# separating conninfo parameters so one name cannot match another it prefixes.
sub _yb_find_downstream
{
	my ($self, $appname) = @_;
	my $lit = _quote_literal($appname);
	my $query = qq[SELECT subname FROM pg_catalog.pg_subscription
	     WHERE subconninfo ~ ('(^|[ ])application_name=' || $lit || '([ ]|\$)')
	        OR subname = $lit
	     ORDER BY subname LIMIT 1];

	foreach my $node (@ALL_NODES)
	{
		next if $node == $self;
		my $subname = $node->safe_psql('postgres', $query);
		return ($node, $subname) if defined($subname) && $subname ne '';
	}
	croak "no external node has a subscription with application_name '$appname'";
}

# A YugabyteDB publisher has no WAL position to compare: pg_current_wal_lsn() is
# unsupported, and the LSNs in pg_stat_replication are virtual WAL positions.
# Hybrid time is the real ordering.  The publisher's safe_time passes T only once
# every commit at or before T has been streamed, so an applied hybrid time at or
# past a target read now proves every earlier commit was applied.  $mode has no
# analogue - there is one notion of apply progress.
sub _yb_wait_for_catchup
{
	my ($self, $appname) = @_;
	my ($downstream, $subname) = $self->_yb_find_downstream($appname);
	my $target = $self->safe_psql('postgres', 'SELECT yb_get_current_hybrid_time_lsn()');
	croak 'publisher ' . $self->name . ' returned no hybrid time'
	  unless defined($target) && $target =~ /^\d+$/;

	print "Waiting for subscription \"$subname\" on " . $downstream->name
	  . " to apply past hybrid time $target\n";

	# relid IS NULL selects the apply worker; a tablesync worker's progress says
	# nothing about the subscription as a whole.  pid IS NOT NULL keeps only
	# subscriptions that actually have a running worker: pg_stat_subscription
	# left-joins pg_subscription, which is a shared catalog, so a same-named
	# subscription in another database contributes an all-NULL row that would
	# make bool_and false forever.  count(*) guards the empty case, where
	# bool_and is NULL and the subscription has not started applying at all.
	my $query = qq[SELECT count(*) > 0
	                  AND bool_and(COALESCE(st.yb_last_commit_ht >= $target, false))
	     FROM pg_catalog.pg_subscription s
	     JOIN pg_catalog.pg_stat_subscription st ON st.subid = s.oid
	    WHERE s.subname = ] . _quote_literal($subname) . qq[
	      AND st.relid IS NULL AND st.pid IS NOT NULL];

	# Query once directly first: safe_psql croaks with the server's own message,
	# so a subscriber whose pg_stat_subscription has no yb_last_commit_ht fails
	# immediately instead of after the poll timeout.
	if ($downstream->safe_psql('postgres', $query) ne 't')
	{
		$downstream->poll_query_until('postgres', $query)
		  or croak "timed out waiting for \"$subname\" to apply past hybrid time $target";
	}
	print "done\n";
	return;
}

sub wait_for_catchup
{
	my ($self, $standby_name, $mode, $target_lsn) = @_;
	$mode = defined($mode) ? $mode : 'replay';
	my %valid_modes = ('sent' => 1, 'write' => 1, 'flush' => 1, 'replay' => 1);
	croak "unknown mode $mode for 'wait_for_catchup'" unless exists($valid_modes{$mode});
	$standby_name = $standby_name->name if ref($standby_name) && $standby_name->can('name');

	# An explicit LSN target has no hybrid-time equivalent, so leave it to the
	# upstream path rather than silently waiting for something else.
	return $self->_yb_wait_for_catchup($standby_name)
	  if !defined($target_lsn) && $self->_is_yb;

	$target_lsn = $self->lsn('write') unless defined($target_lsn);
	print "Waiting for replication conn ${standby_name}'s ${mode}_lsn to pass $target_lsn on " . $self->name . "\n";
	my $query = qq[SELECT '$target_lsn' <= ${mode}_lsn AND state = 'streaming'
         FROM pg_catalog.pg_stat_replication
         WHERE application_name IN ('$standby_name', 'walreceiver')];
	$self->poll_query_until('postgres', $query) or croak "timed out waiting for catchup";
	print "done\n";
	return;
}

sub wait_for_subscription_sync
{
	my ($self, $publisher, $subname, $dbname) = @_;
	$dbname = defined($dbname) ? $dbname : 'postgres';
	my $name = $self->name;

	print "Waiting for all subscriptions in \"$name\" to synchronize data\n";
	my $query =
	    qq[SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r', 's');];
	$self->poll_query_until($dbname, $query)
	  or croak "timed out waiting for subscriber to synchronize data";

	if (defined($publisher))
	{
		croak 'subscription name must be specified' unless defined($subname);
		$publisher->wait_for_catchup($subname);
	}
	print "done\n";
	return;
}

package PostgreSQL::Test::ExternalNode::PsqlFactory;

# BackgroundPsql->new() refuses any caller that is not a Cluster subclass.  This
# shim exists only to satisfy that check - it holds no state and adds no
# behaviour, so BackgroundPsql itself stays unmodified.
our @ISA = ('PostgreSQL::Test::Cluster');

sub make
{
	my ($class, $psql_params, $timeout) = @_;
	return PostgreSQL::Test::BackgroundPsql->new(0, $psql_params, $timeout);
}

1;
