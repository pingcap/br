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
TABLE="usertable1"

run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.$TABLE( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.$TABLE VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.$TABLE VALUES (\"aa\", \"b\");"

row_count_ori=$(run_sql "SELECT COUNT(*) FROM $DB$.$TAble;" | awk '/COUNT/{print $2}')

# backup table
echo "backup start..."
run_br --pd $PD_ADDR backup table --db $DB --table usertable1 -s "local://$TEST_DIR/$DB" --ratelimit 5 --concurrency 4

run_sql "DROP DATABASE $DB;"

# Test validate decode
run_br validate decode -s "local://$TEST_DIR/$DB"

# should generate backupmeta.json
if [ ! -f "$TEST_DIR/$DB/backupmeta.json" ]; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

# Test validate encode
run_br validate encode -s "local://$TEST_DIR/$DB"

# should generate backupmeta_from_json
if [ ! -f "$TEST_DIR/$DB/backupmeta_from_json" ]; then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

# replace backupmeta
mv "$TEST_DIR/$DB/backupmeta_from_json" "$TEST_DIR/$DB/backupmeta"

# restore table
echo "restore start..."
run_br --pd $PD_ADDR restore table --db $DB --table usertable1 -s "local://$TEST_DIR/$DB"

row_count_new=$(run_sql "SELECT COUNT(*) FROM $DB$.$TABLE;" | awk '/COUNT/{print $2}')

if [ "${row_count_ori}" != "${row_count_new}" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"
