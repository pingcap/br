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

# start the s3 server
export MINIO_ACCESS_KEY=brs3accesskey
export MINIO_SECRET_KEY=brs3secretkey
export MINIO_BROWSER=off
export AWS_ACCESS_KEY_ID=$MINIO_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=$MINIO_SECRET_KEY
export S3_ENDPOINT=127.0.0.1:24927
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

stop_minio() {
    kill -2 $MINIO_PID
}
trap stop_minio EXIT

s3cmd --access_key=$MINIO_ACCESS_KEY --secret_key=$MINIO_SECRET_KEY --host=$S3_ENDPOINT --host-bucket=$S3_ENDPOINT --no-ssl mb s3://mybucket

# Fill in the database
for i in $(seq $DB_COUNT); do
    run_sql "CREATE DATABASE $DB${i};"
    go-ycsb load mysql -P tests/$TEST_NAME/workload -p mysql.host=$TIDB_IP -p mysql.port=$TIDB_PORT -p mysql.user=root -p mysql.db=$DB${i}
done

for i in $(seq $DB_COUNT); do
    row_count_ori[${i}]=$(run_sql "SELECT COUNT(*) FROM $DB${i}.$TABLE;" | awk '/COUNT/{print $2}')
done

# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "s3://mybucket/$DB?endpoint=http://$S3_ENDPOINT"

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done

# restore full
echo "restore start..."
run_br restore full -s "s3://mybucket/$DB" --pd $PD_ADDR --s3.endpoint="http://$S3_ENDPOINT"

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

for i in $(seq $DB_COUNT); do
    run_sql "DROP DATABASE $DB${i};"
done
