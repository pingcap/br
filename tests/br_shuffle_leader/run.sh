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
TABLE_COUNT=3

for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    for j in $(seq $TABLE_COUNT); do
        go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i} -p table=$TABLE${j}
    done
done

for i in $(seq $DB_COUNT); do
    for j in $(seq $TABLE_COUNT); do
      row_count_ori[(${i}*$TABLE_COUNT+${j})]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE${j};" | awk '/COUNT/{print $2}')
    done
done

# add shuffle leader scheduler
echo "add shuffle-leader-scheduler"
echo "-u $PD_ADDR -d sched add shuffle-leader-scheduler" | pd-ctl

# backup with shuffle leader
echo "backup start..."
br --pd $PD_ADDR backup table -s "local://$TEST_DIR/$DB" --db $DB -t $TABLE --ratelimit 1 --concurrency 4

run_sql "DROP TABLE $DB.$TABLE;"

# restore with shuffle leader
echo "restore start..."
br restore table --db $DB --table $TABLE --connect "root@tcp($TIDB_ADDR)/" -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

# remove shuffle leader scheduler
echo "-u $PD_ADDR -d sched remove shuffle-leader-scheduler" | pd-ctl

for i in $(seq $DB_COUNT); do
    for j in $(seq $TABLE_COUNT); do
        row_count_new[(${i}*$TABLE_COUNT+${j})]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
    done
done

fail=false
for i in $(seq $DB_COUNT); do
    for j in $(seq $TABLE_COUNT); do
        if [ "${row_count_ori[i*TABLE_COUNT+j]}" != "${row_count_new[i*TABLE_COUNT+j]}" ];then
            fail=true
            echo "TEST: [$TEST_NAME] fail on database $DB${i} $TABLE${j}"
        fi
        echo "database $DB${i} $TABLE${j} [original] row count: ${row_count_ori[i*TABLE_COUNT+j]}, [after br] row count: ${row_count_new[i*TABLE_COUNT+j]}"
    done
done

if $fail; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
else
    echo "TEST: [$TEST_NAME] successed!"
fi
