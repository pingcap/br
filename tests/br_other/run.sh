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

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.usertable1 VALUES (\"aa\", \"b\");"

# backup full
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/$DB" --ratelimit 5 --concurrency 4 --fastchecksum true

# Test validate backupmeta
run_br validate backupmeta -s "local://$TEST_DIR/$DB"
run_br validate backupmeta -s "local://$TEST_DIR/$DB" --offset 100

# Test validate checksum
run_br validate checksum -s "local://$TEST_DIR/$DB"

# Test validate checksum
for sst in $TEST_DIR/$DB/*.sst; do
    echo "corrupted!" >> $sst
    echo "$sst corrupted!"
    break
done

corrupted=0
run_br validate checksum -s "local://$TEST_DIR/$DB" || corrupted=1
if [ "$corrupted" -ne "1" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"

# Test version
run_br version
