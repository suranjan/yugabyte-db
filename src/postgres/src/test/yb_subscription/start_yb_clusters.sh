#!/bin/bash
# Start the two YugabyteDB clusters the harness replicates between, in one of the
# two configurations the suite is characterised under.  Both need a restart to
# change, since every setting here is either a gflag or PGC_POSTMASTER.
#
#   ./start_yb_clusters.sh pgoutput   # publisher tables default to DEFAULT
#   ./start_yb_clusters.sh yboutput   # publisher defaults to CHANGE, subscriber
#                                     # creates its slots with yboutput
#
# pgoutput is the configuration a PostgreSQL subscriber could also reach.
# yboutput is YugabyteDB's own default replica identity end to end, and needs
# the yb_subscription_use_yboutput build.  See the README for why each flag is
# here; every one of them fails as a hang rather than an error.
set -e

MODE="${1:-pgoutput}"
case "$MODE" in
  pgoutput|yboutput) ;;
  *) echo "usage: $0 [pgoutput|yboutput]" >&2; exit 1 ;;
esac

YB_SRC="${YB_SRC_ROOT:-$(cd "$(dirname "$0")/../../../../.." && pwd)}"
YUGABYTED="$YB_SRC/bin/yugabyted"
PUB_DIR="${PUB_BASE_DIR:-/tmp/yb_subscription_tap/ybpub}"
SUB_DIR="${SUB_BASE_DIR:-/tmp/yb_subscription_tap/ybsub}"
PUB_ADDR=127.0.0.1
SUB_ADDR=127.0.0.2

# The publisher's replica identity is the variable under test.  It must be the
# gflag, not only the GUC: the master builds the stream's replica-identity map
# from the DocDB schema, which comes from the gflag, while pg_class comes from
# the GUC.  Setting only the GUC leaves pg_class disagreeing with the stream.
if [ "$MODE" = yboutput ]; then
  PUB_RI=CHANGE
  SUB_EXTRA=",yb_subscription_use_yboutput=on"
else
  PUB_RI=DEFAULT
  SUB_EXTRA=""
fi

# cdc_enable_intra_transactional_before_image lets a publisher build the before
# image of a row its own uncommitted transaction wrote; without it 015_stream and
# 016_stream_subxact never get past "Failed to get the beforeimage".
#
# DDL inside a transaction block is on by default in release builds and off in
# debug ones.  Turn it on so a debug build runs the mode that ships: with it off,
# 017_stream_ddl and 019_stream_subxact_ddl_abort abort on the publisher before
# anything is replicated.  It comes with object locking for table locks and
# concurrent DDL, which the flag validators tie to it on the master as well as
# the tserver, so these are release's defaults as one set.
ddl_flags="ysql_yb_ddl_transaction_block_enabled=true,\
enable_object_locking_for_table_locks=true,\
ysql_enable_concurrent_ddl=true,\
ysql_yb_enable_ddl_savepoint_support=true"
#
# max_logical_replication_workers must be >= 6 for 010_truncate.  wal_sender_timeout
# is what paces wait_for_catchup, which waits on a keepalive.
common_conf="yb_enable_pg_subscription=on,max_logical_replication_workers=6,\
yb_enable_replication_slot_exclusive_lock=on,wal_sender_timeout=6s,yb_enable_cbo=on"

start_one()
{
	local base_dir=$1 addr=$2 ri=$3 extra=$4
	"$YUGABYTED" start \
		--base_dir="$base_dir" \
		--advertise_address="$addr" \
		--master_flags "master_auto_run_initdb=true,TEST_dcheck_for_missing_schema_packing=false,\
ysql_yb_enable_pg_subscription=true,$ddl_flags" \
		--tserver_flags "ysql_yb_enable_replication_commands=true,\
ysql_yb_enable_pg_subscription=true,\
ysql_yb_use_commit_time_ht_in_replication=true,\
ysql_yb_default_replica_identity=$ri,\
TEST_dcheck_for_missing_schema_packing=false,\
cdc_enable_intra_transactional_before_image=true,\
$ddl_flags,\
ysql_num_shards_per_tserver=1,\
ysql_pg_conf_csv={$common_conf,yb_default_replica_identity=$ri$extra}"
}

# "yugabyted stop" cannot be trusted here: it dies with
#   TypeError: wait_until_stop() missing 1 required positional argument: 'pid'
# and returns having killed nothing, or having killed asynchronously.  Either
# way the next start loses the port race against the processes still shutting
# down and reports "default port(s) 5433 already in use", leaving one cluster
# up and one down.  So ask politely, then wait for the port to actually free,
# then insist.
stop_one()
{
	local base_dir=$1 addr=$2 i

	"$YUGABYTED" stop --base_dir="$base_dir" >/dev/null 2>&1 || true

	for i in $(seq 1 30); do
		ss -ltn 2>/dev/null | grep -q "$addr:5433" || return 0
		sleep 1
	done

	# Still listening after 30s: kill what belongs to this base_dir only.
	echo "  $addr:5433 still held after yugabyted stop; killing $base_dir processes"
	pkill -f "fs_data_dirs=$base_dir/data" 2>/dev/null || true
	for i in $(seq 1 30); do
		ss -ltn 2>/dev/null | grep -q "$addr:5433" || return 0
		sleep 1
	done
	echo "ERROR: $addr:5433 is still in use; not starting on top of it" >&2
	exit 1
}

echo "Stopping any running clusters..."
stop_one "$PUB_DIR" "$PUB_ADDR"
stop_one "$SUB_DIR" "$SUB_ADDR"

echo "Starting publisher ($PUB_ADDR), replica identity $PUB_RI..."
start_one "$PUB_DIR" "$PUB_ADDR" "$PUB_RI" ""

# The subscriber's own tables' replica identity does not affect apply, so it is
# held at DEFAULT in both modes to keep the publisher the only variable.
echo "Starting subscriber ($SUB_ADDR)${SUB_EXTRA:+, $SUB_EXTRA}..."
start_one "$SUB_DIR" "$SUB_ADDR" DEFAULT "$SUB_EXTRA"

echo
YSQLSH="$YB_SRC/build/latest/postgres/bin/ysqlsh"

# yugabyted returns while YSQL is still "Not Ready", so verifying straight away
# reports a connection refused that means nothing.  Wait for both to answer.
wait_ready()
{
	local addr=$1 i
	for i in $(seq 1 60); do
		if "$YSQLSH" "host=$addr port=5433 user=yugabyte dbname=yugabyte" \
			-t -A -c "SELECT 1" >/dev/null 2>&1; then
			return 0
		fi
		sleep 5
	done
	echo "ERROR: $addr:5433 never accepted connections" >&2
	exit 1
}

echo "Waiting for YSQL on both clusters..."
wait_ready "$PUB_ADDR"
wait_ready "$SUB_ADDR"

echo "Verifying the settings actually took effect:"
"$YSQLSH" "host=$PUB_ADDR port=5433 user=yugabyte dbname=postgres" -t -A \
	-c "SELECT 'publisher yb_default_replica_identity=' || current_setting('yb_default_replica_identity')"
"$YSQLSH" "host=$SUB_ADDR port=5433 user=yugabyte dbname=postgres" -t -A \
	-c "SELECT 'subscriber yb_enable_pg_subscription=' || current_setting('yb_enable_pg_subscription')"
if [ "$MODE" = yboutput ]; then
	"$YSQLSH" "host=$SUB_ADDR port=5433 user=yugabyte dbname=postgres" -t -A \
		-c "SELECT 'subscriber yb_subscription_use_yboutput=' || current_setting('yb_subscription_use_yboutput')"
fi
echo "Done."
