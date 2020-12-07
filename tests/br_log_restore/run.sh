#!/bin/bash
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

set -eux
DB="$TEST_NAME"
TABLE="usertable"
DB_COUNT=3
BUCKET="cdcs3"
CDC_COUNT=3

# start the s3 server
export MINIO_ACCESS_KEY=brs3accesskey
export MINIO_SECRET_KEY=brs3secretkey
export MINIO_BROWSER=off
export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
export S3_ENDPOINT=127.0.0.1:24928
rm -rf "$TEST_DIR/$DB"
mkdir -p "$TEST_DIR/$DB"
bin/minio server --address $S3_ENDPOINT "$TEST_DIR/$DB" &
MINIO_PID=$!
i=0
while ! curl -o /dev/null -v -s "http://$S3_ENDPOINT/"; do
    i=$(($i+1))
    if [ $i -gt 7 ]; then
        echo 'Failed to start minio'
        exit 1
    fi
    sleep 2
done


s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$S3_ENDPOINT --host-bucket=$S3_ENDPOINT --no-ssl mb s3://$BUCKET

# Start cdc servers
bin/cdc server --pd=http://$PD_ADDR --log-file=ticdc.log --addr=0.0.0.0:18301 --advertise-addr=127.0.0.1:18301 &
CDC_PID=$!
stop_tmp_server() {
    kill -2 $MINIO_PID
    kill -2 $CDC_PID
}
trap stop_tmp_server EXIT

# TODO: remove this after TiCDC supports TiDB clustered index
run_sql "set @@global.tidb_enable_clustered_index=0"
# TiDB global variables cache 2 seconds
sleep 2

# create change feed for s3 log
bin/cdc cli changefeed create --pd=http://$PD_ADDR --sink-uri="s3://$BUCKET/$DB?endpoint=http://$S3_ENDPOINT" --changefeed-id="simple-replication-task"

start_ts=$(run_sql "show master status;" | grep Position | awk -F ':' '{print $2}' | xargs)

# Fill in the database
for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

# test drop & create schema/table, finally only db2 has one row
run_sql "create schema ${DB}_DDL1;"
run_sql "create table ${DB}_DDL1.t1 (a int primary key, b varchar(10));"
run_sql "insert into ${DB}_DDL1.t1 values (1, 'x');"

run_sql "drop schema ${DB}_DDL1;"
run_sql "create schema ${DB}_DDL1;"
run_sql "create schema ${DB}_DDL2;"

run_sql "create table ${DB}_DDL2.t2 (a int primary key, b varchar(10));"
run_sql "insert into ${DB}_DDl2.t2 values (2, 'x');"

run_sql "drop table ${DB}_DDL2.t2;"
run_sql "create table ${DB}_DDL2.t2 (a int primary key, b varchar(10));"
run_sql "insert into ${DB}_DDL2.t2 values (3, 'x');"
run_sql "delete from ${DB}_DDL2.t2 where a = 3;"
run_sql "insert into ${DB}_DDL2.t2 values (4, 'x');"

end_ts=$(run_sql "show master status;" | grep Position | awk -F ':' '{print $2}' | xargs)

# if we restore with ts range [start_ts, end_ts], then the below record won't be restored.
run_sql "insert into ${DB}_DDL2.t2 values (5, 'x');"

# sleep wait cdc log sync to storage
# TODO find another way to check cdc log has synced
# need wait more time for cdc log synced, because we add some ddl.
sleep 50

# remove the change feed, because we don't want to record the drop ddl.
echo "Y" | bin/cdc cli unsafe reset --pd=http://$PD_ADDR

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done
run_sql "DROP DATABASE ${DB}_DDL1"
run_sql "DROP DATABASE ${DB}_DDL2"

# restore full
echo "restore start..."
run_br restore cdclog -s "s3://$BUCKET/$DB" --pd $PD_ADDR --s3.endpoint="http://$S3_ENDPOINT" \
    --log-file "restore.log" --log-level "info" --start-ts $start_ts --end-ts $end_ts

for i in $(seq $DB_COUNT); do
    row_count_new[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

fail=false
row_count=$(run_sql "SELECT COUNT(*) FROM ${DB}_DDL2.t2 WHERE a=4;" | awk '/COUNT/{print $2}')
if [ "$row_count" -ne "1" ]; then
    fail=true
    echo "TEST: [$TEST_NAME] fail on dml&ddl drop test."
fi


# record a=5 shouldn't be restore, because we set -end-ts without this record.
row_count=$(run_sql "SELECT COUNT(*) FROM ${DB}_DDL2.t2 WHERE a=5;" | awk '/COUNT/{print $2}')
if [ "$row_count" -ne "0" ]; then
    fail=true
    echo "TEST: [$TEST_NAME] fail on ts range test."
fi

echo "restore again to restore a=5 record..."
run_br restore cdclog -s "s3://$BUCKET/$DB" --pd $PD_ADDR --s3.endpoint="http://$S3_ENDPOINT" \
    --log-file "restore.log" --log-level "info" --start-ts $end_ts

# record a=5 should be restore, because we set -end-ts without this record.
row_count=$(run_sql "SELECT COUNT(*) FROM ${DB}_DDL2.t2 WHERE a=5;" | awk '/COUNT/{print $2}')
if [ "$row_count" -ne "1" ]; then
    fail=true
    echo "TEST: [$TEST_NAME] fail on recover ts range test."
fi

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

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done

run_sql "DROP DATABASE ${DB}_DDL1"
run_sql "DROP DATABASE ${DB}_DDL2"
