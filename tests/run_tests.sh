#!/bin/bash

set -eux

PG_DUMP="$(which pg_dump)"

DBNAME=pg_split_dump_tests
export PGUSER=postgres
export PGHOST=${PGHOST:-"/var/run/postgresql"}

POSTGRES_VERSION="$(psql -tAXq -c 'SHOW server_version_num')"

dropdb --if-exists "$DBNAME"
createdb "$DBNAME"
psql -1 -X -v ON_ERROR_STOP=1 -f input.sql -d "$DBNAME"

if [ -d tmp ]; then
    rm -r tmp
fi
mkdir tmp

RUST_BACKTRACE=1 ../target/debug/pg_split_dump --format=t --pg-dump-binary="$PG_DUMP" "user=$PGUSER host=$PGHOST dbname=$DBNAME" tmp/tests.tar

RUST_BACKTRACE=1 ./bin/create_expected_archive/target/debug/create_expected_archive $POSTGRES_VERSION ./expected tmp/expected.tar

RUST_BACKTRACE=1 ../tar_diff/target/debug/tar_diff tmp/expected.tar tmp/tests.tar > tmp/diff
if [ -s tmp/diff ]; then
    set +x

    echo "pg_split_dump tests FAILED" >&1
    echo "" >&1
    cat tmp/diff >&1
    exit 1
fi
