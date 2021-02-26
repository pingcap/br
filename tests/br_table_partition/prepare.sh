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

ROW_COUNT=100
CONCURRENCY=8

insertRecords() {
    for i in $(seq $2 $3); do
        run_sql "INSERT INTO $1 VALUES (\
            $i, \
            REPEAT(' ', 255), \
            REPEAT(' ', 255), \
            REPEAT(' ', 255), \
            REPEAT(' ', 255)\
        );"
    done
}

createTable() {
    run_sql "CREATE TABLE IF NOT EXISTS $DB.$TABLE$1 (\
        c1 INT, \
        c2 CHAR(255), \
        c3 CHAR(255), \
        c4 CHAR(255), \
        c5 CHAR(255)) \
        \
        PARTITION BY RANGE(c1) ( \
        PARTITION p0 VALUES LESS THAN (0), \
        PARTITION p1 VALUES LESS THAN ($(expr $ROW_COUNT / 2)) \
    );"
    run_sql "ALTER TABLE $DB.$TABLE$1 \
      ADD PARTITION (PARTITION p2 VALUES LESS THAN MAXVALUE);"
}

echo "load database $DB"
run_sql "CREATE DATABASE IF NOT EXISTS $DB;"
for i in $(seq $TABLE_COUNT); do
  createTable "${i}" &
done
wait

for i in $(seq $TABLE_COUNT); do
    for j in $(seq $CONCURRENCY); do
        insertRecords $DB.$TABLE${i} $(expr $ROW_COUNT / $CONCURRENCY \* $(expr $j - 1) + 1) $(expr $ROW_COUNT / $CONCURRENCY \* $j) &
    done
done
wait
