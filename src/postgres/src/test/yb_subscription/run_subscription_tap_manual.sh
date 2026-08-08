#!/bin/bash
# Manual run: execute subscription TAP test with error output visible.
# Use this to see the real failure (prove hides it; exit 29 = no TAP plan = script died early).
#
# Usage:
#   ./run_subscription_tap_manual.sh              # run 001 (normal)
#   ./run_subscription_tap_manual.sh verify       # only verify publisher + subscriber connections
#   ./run_subscription_tap_manual.sh debug        # run 001 with DIE handler (shows errors; may show "Too late INIT" warning)
#   ./run_subscription_tap_manual.sh t/002_types.pl

set -e
YB_SRC="${YB_SRC_ROOT:-$(cd "$(dirname "$0")/../../../../.." && pwd)}"
YB_TEST_DIR="$YB_SRC/src/postgres/src/test/yb_subscription"
PG_TEST_DIR="$YB_SRC/src/postgres/src/test/subscription"
PERL_LIB="$YB_SRC/src/postgres/src/test/perl"
PG_PSQL="${PG_PUBLISHER_PSQL_PATH:-psql}"
PG_CONN="${PG_PUBLISHER_CONNSTR:-host=127.0.0.1 port=5432 user=$USER}"
YB_CONN="${YB_SUBSCRIBER_CONNSTR:-host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte}"

export PATH="${PG_PUBLISHER_PSQL_PATH%/*}:${YB_YSQLSH_PATH%/*}:$PATH"
export PG_PUBLISHER_CONNSTR="$PG_CONN"
export PG_PUBLISHER_PSQL_PATH="$PG_PSQL"
export YB_SUBSCRIBER_CONNSTR="$YB_CONN"
export YB_YSQLSH_PATH="${YB_YSQLSH_PATH:-$YB_SRC/build/latest/postgres/bin/ysqlsh}"
export TESTDIR="${TESTDIR:-/tmp/subscription_tap_check}"
mkdir -p "$TESTDIR"

if [ "${1:-}" = "verify" ]; then
  echo "=== 1. Verify publisher (Postgres) ==="
  PG_CONN_DB="$PG_CONN"; [[ "$PG_CONN_DB" != *"dbname="* ]] && PG_CONN_DB="$PG_CONN_DB dbname=postgres"
  $PG_PSQL "$PG_CONN_DB" -c "SELECT 1 AS publisher_ok;" || { echo "FAIL: cannot connect to publisher"; exit 1; }
  echo "Publisher OK."
  echo ""
  echo "=== Publisher replica identity (public schema) ==="
  echo "  d=DEFAULT(PK), n=NOTHING, f=FULL, i=INDEX"
  $PG_PSQL "$PG_CONN_DB" -c "
    SELECT c.relname AS table_name,
           c.relreplident::text AS ri,
           CASE c.relreplident
             WHEN 'd' THEN 'DEFAULT (primary key)'
             WHEN 'n' THEN 'NOTHING'
             WHEN 'f' THEN 'FULL'
             WHEN 'i' THEN 'INDEX'
             ELSE '?'
           END AS replica_identity
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
    ORDER BY c.relname;" 2>/dev/null || true
  echo ""
  echo "=== 2. Verify subscriber (YugabyteDB) ==="
  "$YB_YSQLSH_PATH" "$YB_CONN" -c "SELECT 1 AS subscriber_ok;" || { echo "FAIL: cannot connect to subscriber. Run: yugabyted status  # then set YB_SUBSCRIBER_CONNSTR with that host (e.g. host=10.157.0.4 port=5433 user=yugabyte dbname=yugabyte)"; exit 1; }
  echo "Subscriber OK."
  SUB_OK=$("$YB_YSQLSH_PATH" "$YB_CONN" -t -A -c "SELECT current_setting('yb_enable_pg_subscription', true);" 2>/dev/null || echo "off")
  if [ "$SUB_OK" != "on" ]; then
    echo "FAIL: subscription is disabled on the subscriber (yb_enable_pg_subscription is not 'on')."
    echo "Start yugabyted with: yugabyted start --tserver_flags 'ysql_pg_conf_csv=yb_enable_pg_subscription=on'"
    echo "If the cluster is already running, stop it and restart with that flag."
    exit 1
  fi
  echo "Subscription enabled (yb_enable_pg_subscription=on)."
  echo ""
  echo "=== Subscriber (YugabyteDB) replica identity (public schema, db postgres) ==="
  echo "  d=DEFAULT(PK), n=NOTHING, f=FULL, i=INDEX, c=CHANGE (YB default)"
  YB_CONN_DB="$YB_CONN dbname=postgres"
  "$YB_YSQLSH_PATH" "$YB_CONN_DB" -c "
    SELECT c.relname AS table_name,
           c.relreplident::text AS ri,
           CASE c.relreplident
             WHEN 'd' THEN 'DEFAULT (primary key)'
             WHEN 'n' THEN 'NOTHING'
             WHEN 'f' THEN 'FULL'
             WHEN 'i' THEN 'INDEX'
             WHEN 'c' THEN 'CHANGE (YB)'
             ELSE '?'
           END AS replica_identity
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
    ORDER BY c.relname;" 2>/dev/null || true
  echo ""
  echo "=== 3. Publisher: drop test function if present ==="
  $PG_PSQL "$PG_CONN_DB" -c "DROP FUNCTION IF EXISTS public.pg_get_replica_identity_index(int) CASCADE;" 2>/dev/null || true
  echo "Done. Run ./run_subscription_tap_manual.sh (no args) to run the test."
  exit 0
fi

cd "$YB_TEST_DIR"
TEST_FILE="${1:-t/001_rep_changes.pl}"

# Only adapted tests live here; the rest are the untouched upstream files.
if [ "$TEST_FILE" != "debug" ] && [ ! -f "$TEST_FILE" ]; then
  TEST_FILE="$PG_TEST_DIR/t/$(basename "$TEST_FILE")"
fi

if [ "$TEST_FILE" = "debug" ]; then
  TEST_FILE="t/001_rep_changes.pl"
  echo "Running $TEST_FILE with DIE handler (errors below; ignore 'Too late INIT' if shown)..."
  echo "---"
  perl -I "$PERL_LIB" -I "$YB_TEST_DIR" -MSubscriptionCommon -e 'BEGIN{$SIG{__DIE__}=sub{print STDERR "DIE: ",@_}} do shift' "$TEST_FILE" 2>&1
  EXIT=$?
  echo "---"
  echo "Exit code: $EXIT"
  exit $EXIT
fi

# Run the test file as the main script (Cluster/Utils INIT blocks run correctly).
echo "Running $TEST_FILE..."
echo "---"
perl -I "$PERL_LIB" -I "$YB_TEST_DIR" -MSubscriptionCommon "$TEST_FILE" 2>&1
EXIT=$?
echo "---"
echo "Exit code: $EXIT"
exit $EXIT
