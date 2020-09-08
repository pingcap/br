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
TABLES_COUNT=300

run_sql "create schema $DB;"

# generate 300 tables with 1 row content.
i=1
while [ $i -le $TABLES_COUNT ]; do
    run_sql "create table $DB.sbtest$i(id int primary key, k int not null, c char(120) not null, pad char(60) not null);"
    run_sql "insert into $DB.sbtest$i values ($i, $i, '$i', '$i');"
    i=$(($i+1))
done

# backup db
echo "backup start..."
run_br backup db --db "$DB" -s "local://$TEST_DIR/$DB" --pd $PD_ADDR

# truncate every table
# (FIXME: drop instead of truncate. if we drop then create-table will still be executed and wastes time executing DDLs)
i=1
while [ $i -le $TABLES_COUNT ]; do
    run_sql "truncate $DB.sbtest$i;"
    i=$(($i+1))
done

# restore db
# (FIXME: shouldn't need --no-schema to be fast, currently the alter-auto-id DDL slows things down)
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --no-schema

run_sql "DROP DATABASE $DB;"
