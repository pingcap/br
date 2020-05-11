#!/bin/sh
#
# Copyright 2020 PingCAP, Inc.
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
LOG="/tmp/not-leader.log"
DB_COUNT=3

for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done


# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --ratelimit 5 --concurrency 4

success=0
for j in $(seq 10); do
    echo "trying $j time(s) to seek failed condition."
    if [ -e $LOG ]; then
        rm $LOG
    fi

    for i in $(seq $DB_COUNT); do
        run_sql "DROP DATABASE $DB${i};"
    done


    # restore full
    echo "restore start..."

    unset BR_LOG_TO_TERM
    GO_FAILPOINTS="github.com/pingcap/br/pkg/restore/not-leader-error=return(50)" \
    run_br restore full -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --ratelimit 1024 --log-file $LOG || true
    BR_LOG_TO_TERM=1

    if grep "split region meet not leader error" $LOG && grep "Full restore Success" $LOG; then
        echo "success injected fail-point"
        success=1
        break
    fi
done

if [ $success -ne 1 ]; then
    echo "Retry too many times but not found retrying."
    echo "failpoint control variable is: " $GO_FAILPOINTS
fi

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
fi

echo "TEST $TEST_NAME passed."


