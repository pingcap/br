#!/bin/sh
#
# Copyright 2019 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3
old_conf=$(run_sql "show config where name = 'alter-primary-key'")

for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

# backup full and kill tikv to test reset connection
echo "backup with limit start..."
export GO_FAILPOINTS="github.com/pingcap/br/pkg/backup/reset-retryable-error=1*return(true)"
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB-limit" --concurrency 4
export GO_FAILPOINTS=""

# backup full
echo "backup with lz4 start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB-lz4" --concurrency 4 --compression lz4
size_lz4=$(du -d 0 $TEST_DIR/$DB-lz4 | awk '{print $1}')

echo "backup with zstd start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB-zstd" --concurrency 4 --compression zstd --compression-level 6
size_zstd=$(du -d 0 $TEST_DIR/$DB-zstd | awk '{print $1}')

if [ "$size_lz4" -le "$size_zstd" ]; then
  echo "full backup lz4 size $size_lz4 is small than backup with zstd $size_zstd"
  exit -1
fi

for ct in limit lz4 zstd; do
  for i in $(seq $DB_COUNT); do
      run_sql "DROP DATABASE $DB${i};"
  done

  # restore full
  echo "restore with $ct backup start..."
  run_br restore full -s "local://$TEST_DIR/$DB-$ct" --pd $PD_ADDR --ratelimit 1024

  for i in $(seq $DB_COUNT); do
      row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
  done

  fail=false
  for i in $(seq $DB_COUNT); do
      if [ "${row_count_ori[i]}" != "${row_count_new[i]}" ];then
          fail=true
          echo "TEST: [$TEST_NAME] fail on database $DB${i}"
      fi
      echo "database $DB${i} [original] row count: ${row_count_ori[i]}, [after br] row count: ${row_count_new[i]}"
  done

  if $fail; then
      echo "TEST: [$TEST_NAME] failed!"
      exit 1
  else
      echo "TEST: [$TEST_NAME] successed!"
  fi
done

# test whether we have changed the cluster config.
test "$old_conf" = "$(run_sql "show config where name = 'alter-primary-key'")"

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done
