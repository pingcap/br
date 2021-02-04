#!/bin/sh
#
# Copyright 2021 PingCAP, Inc.
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

# backup empty using BR
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/br_version_1" --ratelimit 5 --concurrency 4
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on backup empty cluster version!"
    exit 1
fi

run_br -s "local://$TEST_DIR/br_version_1" debug decode --field "BrVersion"
run_br -s "local://$TEST_DIR/br_version_1" debug decode --field "ClusterVersion"

# backup empty using BR via SQL
echo "backup start..."
run_sql "BACKUP DATABASE $DB TO \"local://$TEST_DIR/br_version_2\""

run_br -s "local://$TEST_DIR/br_version_2" debug decode --field "BrVersion"
run_br -s "local://$TEST_DIR/br_version_2" debug decode --field "ClusterVersion"

# create a database and insert some data
run_sql "CREATE DATABASE $DB;"
run_sql "CREATE TABLE $DB.usertable1 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
# insert one row to make sure table is restored.
run_sql "INSERT INTO $DB.usertable1 VALUES (\"a\", \"b\");"

# backup tables using BR
echo "backup start..."
run_br --pd $PD_ADDR backup full -s "local://$TEST_DIR/br_version_3" --ratelimit 5 --concurrency 4
if [ $? -ne 0 ]; then
    echo "TEST: [$TEST_NAME] failed on backup empty cluster version!"
    exit 1
fi

run_br -s "local://$TEST_DIR/br_version_3" debug decode --field "BrVersion"
run_br -s "local://$TEST_DIR/br_version_3" debug decode --field "ClusterVersion"

# backup tables using BR via SQL
echo "backup start..."
run_sql "BACKUP DATABASE $DB TO \"local://$TEST_DIR/br_version_4\""

run_br -s "local://$TEST_DIR/br_version_4" debug decode --field "BrVersion"
run_br -s "local://$TEST_DIR/br_version_4" debug decode --field "ClusterVersion"

run_sql "DROP DATABASE $DB"
echo "TEST: [$TEST_NAME] successed!"
