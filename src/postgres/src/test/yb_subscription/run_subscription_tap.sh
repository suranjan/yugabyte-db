#!/bin/bash
# Run the PostgreSQL subscription TAP tests against an external subscriber
# (yugabyted) and an external publisher (Postgres, or another yugabyted).
#
# Tests come from two places.  Most run unmodified out of the upstream
# src/test/subscription/t; only the ones that needed real adaptation have a
# copy in this directory's t/, and that copy wins.  Diffing the two directories
# therefore shows exactly what YugabyteDB required:
#
#   diff -u ../subscription/t/001_rep_changes.pl t/001_rep_changes.pl
#
# Nothing upstream is edited, not even a use line: SubscriptionCommon is loaded
# through PERL5OPT instead, and it patches Cluster->new() before the test body
# runs.

set -e
YB_SRC="${YB_SRC_ROOT:-$(cd "$(dirname "$0")/../../../../.." && pwd)}"
PG_PSQL="${PG_PUBLISHER_PSQL_PATH:-psql}"
PG_CONN="${PG_PUBLISHER_CONNSTR:-host=127.0.0.1 port=5432 user=$USER}"
# Use host from "yugabyted status" (often not 127.0.0.1). Override with YB_SUBSCRIBER_CONNSTR.
YB_CONN="${YB_SUBSCRIBER_CONNSTR:-host=127.0.0.1 port=5433 user=yugabyte dbname=yugabyte}"

YB_TEST_DIR="$YB_SRC/src/postgres/src/test/yb_subscription"
PG_TEST_DIR="$YB_SRC/src/postgres/src/test/subscription"
PERL_LIB="$YB_SRC/src/postgres/src/test/perl"

export PATH="${PG_PUBLISHER_PSQL_PATH%/*}:${YB_YSQLSH_PATH%/*}:$PATH"
export PG_PUBLISHER_CONNSTR="$PG_CONN"
export PG_PUBLISHER_PSQL_PATH="$PG_PSQL"
export YB_SUBSCRIBER_CONNSTR="$YB_CONN"
export YB_YSQLSH_PATH="${YB_YSQLSH_PATH:-$YB_SRC/build/latest/postgres/bin/ysqlsh}"
export TESTDIR="${TESTDIR:-/tmp/subscription_tap_check}"
mkdir -p "$TESTDIR"

# Load the harness without touching the upstream tests.  -M runs before the
# script body, so Cluster->new() is already patched by the time a test calls it.
# This goes through prove --exec rather than PERL5OPT: PERL5OPT would also apply
# to prove itself, which then runs the harness's load-time database reset in the
# runner process and dies before any test starts.
PERL_EXEC="perl -I$PERL_LIB -I$YB_TEST_DIR -MSubscriptionCommon"

# Say which two servers this is about to use, and refuse the one combination that
# is always a mistake.  Both connection strings have defaults, so forgetting to
# export them does not fail: the publisher quietly falls back to a local
# PostgreSQL on 5432 and the subscriber to 127.0.0.1:5433, which is usually the
# publisher cluster.  A run like that replicates the wrong direction between the
# wrong servers and still reports passes, so print the endpoints and stop if they
# are the same server.
pub_endpoint=$(echo "$PG_CONN" | grep -oE 'host=[^ ]+|port=[0-9]+' | sort | tr '\n' ' ')
sub_endpoint=$(echo "$YB_CONN" | grep -oE 'host=[^ ]+|port=[0-9]+' | sort | tr '\n' ' ')
pub_version=$("$PG_PSQL" "$PG_CONN" -t -A -c "SELECT version()" 2>/dev/null | head -1)
case "$pub_version" in
	*-YB-*) pub_kind="YugabyteDB" ;;
	PostgreSQL*) pub_kind="PostgreSQL" ;;
	*) pub_kind="unreachable" ;;
esac
echo "Publisher  ($pub_kind): $PG_CONN"
echo "Subscriber (YugabyteDB): $YB_CONN"
if [ "$pub_endpoint" = "$sub_endpoint" ]; then
	echo "ERROR: publisher and subscriber are the same server ($pub_endpoint)." >&2
	echo "  Export PG_PUBLISHER_CONNSTR and YB_SUBSCRIBER_CONNSTR explicitly." >&2
	exit 1
fi
if [ "$pub_kind" = unreachable ]; then
	echo "ERROR: cannot reach the publisher at $PG_CONN with $PG_PSQL." >&2
	exit 1
fi

# Require subscription enabled on the subscriber (must start yugabyted with ysql_pg_conf_csv).
SUB_OK=$("$YB_YSQLSH_PATH" "$YB_CONN" -t -A -c "SELECT current_setting('yb_enable_pg_subscription', true);" 2>/dev/null || echo "off")
if [ "$SUB_OK" != "on" ]; then
  echo "ERROR: yb_enable_pg_subscription is not 'on' on the subscriber. Start yugabyted with:"
  echo "  yugabyted start --tserver_flags 'ysql_pg_conf_csv=yb_enable_pg_subscription=on'"
  echo "Then stop and restart the cluster if it was started without this flag."
  exit 1
fi

# 010_truncate asks for 6 via append_conf, which cannot raise a PGC_POSTMASTER
# setting on a running server.  Without it the last subscription never gets a
# tablesync worker and the test waits forever instead of failing.
WORKERS=$("$YB_YSQLSH_PATH" "$YB_CONN" -t -A -c "SELECT current_setting('max_logical_replication_workers');" 2>/dev/null || echo 0)
if [ "${WORKERS:-0}" -lt 6 ]; then
  echo "WARNING: max_logical_replication_workers is ${WORKERS:-unknown}, want >= 6."
  echo "  Tests that create several subscriptions (010_truncate) will hang in sync."
  echo "  Restart the subscriber with:"
  echo "    --tserver_flags 'ysql_pg_conf_csv={yb_enable_pg_subscription=on,max_logical_replication_workers=6}'"
fi

# Resolve each requested test to its YugabyteDB copy when there is one, and to
# the untouched upstream file otherwise.  With no arguments, run everything.
resolve()
{
	local name
	name=$(basename "$1")
	if [ -f "$YB_TEST_DIR/t/$name" ]; then
		echo "$YB_TEST_DIR/t/$name"
	else
		echo "$PG_TEST_DIR/t/$name"
	fi
}

# 022_twophase_cascade and 100_bugs build extra local PostgreSQL nodes with
# initdb, which YugabyteDB's initdb cannot do standalone.  They do not merely
# fail: they call BAIL_OUT, and prove abandons every test still queued behind a
# bailout, so leaving them in truncates the run at 022 and silently loses
# everything from 023 onwards.  Name one explicitly to run it anyway.
SKIP_BY_DEFAULT="022_twophase_cascade.pl 100_bugs.pl"

tests=()
if [ $# -eq 0 ]; then
	for f in "$PG_TEST_DIR"/t/*.pl "$YB_TEST_DIR"/t/*.pl; do
		name=$(basename "$f")
		case " $SKIP_BY_DEFAULT " in
			*" $name "*) continue ;;
		esac
		tests+=("$name")
	done
	# YB-only tests have no upstream file, so the two globs overlap; dedupe.
	mapfile -t tests < <(printf '%s\n' "${tests[@]}" | sort -u)
	for i in "${!tests[@]}"; do tests[$i]=$(resolve "${tests[$i]}"); done
else
	for t in "$@"; do tests+=("$(resolve "$t")"); done
fi

if [ $# -eq 0 ]; then
	echo "Not run (they BAIL_OUT and would truncate the suite):$(printf ' %s' $SKIP_BY_DEFAULT)"
fi
echo "Running ${#tests[@]} subscription TAP test(s)..."
cd "$YB_TEST_DIR"
/usr/bin/prove --exec "$PERL_EXEC" -v "${tests[@]}"
echo "Done."
