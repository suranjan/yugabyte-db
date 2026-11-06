# Copyright (c) YugabyteDB, Inc.

# Replicating a table whose replica identity is CHANGE, which is only reachable
# with yb_subscription_use_yboutput enabled: pgoutput rejects CHANGE, and the
# plugin named at slot creation is what the publisher later decodes with.  202
# covers the default-off behaviour, where such a subscription stalls silently.
#
# What CHANGE buys is that an UPDATE carries only the columns it touched.  The
# rest arrive as unchanged-toast markers, so the apply worker keeps whatever the
# subscriber already had.  Seeding c differently on the two sides is what makes
# that observable: if the column were really being sent, the publisher's value
# would overwrite the local one.
#
# The first part uses copy_data = false deliberately.  The GUC is read wherever a
# slot is created, and with copy_data = false the only slot is the one
# CREATE SUBSCRIPTION makes in this backend, so a session SET is enough.  With
# copy_data = true each tablesync worker creates its own slot and does not
# inherit a session setting, so that path needs the GUC in the server
# configuration - which is a cluster restart, hence the second part is skipped
# unless the cluster already has it on.
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

# The second argument makes an unknown name return NULL instead of raising, so
# this reads as "not built with the GUC" rather than failing the test.
if ($node_subscriber->safe_psql('postgres',
		"SELECT coalesce(current_setting('yb_subscription_use_yboutput', true), 'missing')")
	eq 'missing')
{
	plan skip_all => 'subscriber was not built with yb_subscription_use_yboutput';
}

# ---------------------------------------------------------------------------
# A session SET, with no tablesync worker to leave behind on pgoutput.
# ---------------------------------------------------------------------------
$node_publisher->safe_psql('postgres', q{
	CREATE TABLE tab_ybout (a int PRIMARY KEY, b text, c text);
	ALTER TABLE tab_ybout REPLICA IDENTITY CHANGE;
	INSERT INTO tab_ybout VALUES (1, 'b1', 'c1');
	CREATE PUBLICATION tap_pub_ybout FOR TABLE tab_ybout;
});

# c diverges on purpose, so "kept the local value" is distinguishable from
# "received the publisher's value".
$node_subscriber->safe_psql('postgres', q{
	CREATE TABLE tab_ybout (a int PRIMARY KEY, b text, c text);
	INSERT INTO tab_ybout VALUES (1, 'b1', 'local-c');
});

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_subscriber->safe_psql('postgres', qq{
	SET yb_subscription_use_yboutput = on;
	CREATE SUBSCRIPTION tap_sub_ybout CONNECTION '$publisher_connstr'
	  PUBLICATION tap_pub_ybout WITH (copy_data = false);
});

is($node_publisher->safe_psql('postgres',
		"SELECT plugin FROM pg_replication_slots WHERE slot_name = 'tap_sub_ybout'"),
	'yboutput', 'the subscription slot was created with the yboutput plugin');

# An UPDATE that leaves c alone.  Under CHANGE the publisher omits it, so the
# subscriber's own value has to survive.
$node_publisher->safe_psql('postgres',
	"UPDATE tab_ybout SET b = 'b2' WHERE a = 1");
$node_publisher->wait_for_catchup('tap_sub_ybout');

is($node_subscriber->safe_psql('postgres',
		"SELECT a || '/' || b || '/' || c FROM tab_ybout WHERE a = 1"),
	'1/b2/local-c',
	'an UPDATE under CHANGE applies the touched column and keeps the rest');

# An INSERT carries every column, so nothing is omitted here.
$node_publisher->safe_psql('postgres',
	"INSERT INTO tab_ybout VALUES (2, 'b2', 'c2')");
$node_publisher->wait_for_catchup('tap_sub_ybout');

is($node_subscriber->safe_psql('postgres',
		"SELECT a || '/' || b || '/' || c FROM tab_ybout WHERE a = 2"),
	'2/b2/c2', 'an INSERT under CHANGE replicates every column');

# A DELETE has to locate the row from the key alone.
$node_publisher->safe_psql('postgres', "DELETE FROM tab_ybout WHERE a = 1");
$node_publisher->wait_for_catchup('tap_sub_ybout');

is($node_subscriber->safe_psql('postgres',
		"SELECT string_agg(a::text, ',' ORDER BY a) FROM tab_ybout"),
	'2', 'a DELETE under CHANGE removes the row identified by its key');

$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_ybout");
$node_publisher->poll_query_until('postgres',
	"SELECT count(*) = 0 FROM pg_replication_slots WHERE slot_name = 'tap_sub_ybout'")
  or die "the slot outlived DROP SUBSCRIPTION";
pass('DROP SUBSCRIPTION dropped the yboutput slot on the publisher');

# ---------------------------------------------------------------------------
# The tablesync path, which only works when the GUC is in the configuration.
# ---------------------------------------------------------------------------
my $conf_setting =
  $node_subscriber->safe_psql('postgres', "SHOW yb_subscription_use_yboutput");

SKIP:
{
	skip 'tablesync under CHANGE needs yb_subscription_use_yboutput in the '
	  . 'server configuration; the cluster has it off', 2
	  unless $conf_setting eq 'on';

	$node_publisher->safe_psql('postgres', q{
		CREATE TABLE tab_ybout_copy (a int PRIMARY KEY, b text, c text);
		ALTER TABLE tab_ybout_copy REPLICA IDENTITY CHANGE;
		INSERT INTO tab_ybout_copy VALUES (1, 'b1', 'c1');
		CREATE PUBLICATION tap_pub_ybout_copy FOR TABLE tab_ybout_copy;
	});
	$node_subscriber->safe_psql('postgres',
		"CREATE TABLE tab_ybout_copy (a int PRIMARY KEY, b text, c text)");
	$node_subscriber->safe_psql('postgres', qq{
		CREATE SUBSCRIPTION tap_sub_ybout_copy CONNECTION '$publisher_connstr'
		  PUBLICATION tap_pub_ybout_copy;
	});
	$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_ybout_copy');

	is($node_subscriber->safe_psql('postgres',
			"SELECT a || '/' || b || '/' || c FROM tab_ybout_copy"),
		'1/b1/c1', 'the initial copy of a CHANGE table completes');

	$node_publisher->safe_psql('postgres',
		"UPDATE tab_ybout_copy SET b = 'b2' WHERE a = 1");
	$node_publisher->wait_for_catchup('tap_sub_ybout_copy');

	is($node_subscriber->safe_psql('postgres',
			"SELECT a || '/' || b || '/' || c FROM tab_ybout_copy"),
		'1/b2/c1', 'streaming continues after the copy under CHANGE');

	$node_subscriber->safe_psql('postgres', "DROP SUBSCRIPTION tap_sub_ybout_copy");
}

done_testing();
