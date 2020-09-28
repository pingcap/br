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

cur=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $cur/../_utils/run_services

DB="$TEST_NAME"
TABLE="usertable1"
TABLE2="usertable2"

echo "Restart cluster with tls"
start_services_with_tls "$cur"

run_sql "DROP DATABASE IF EXISTS $DB;"
run_sql "CREATE DATABASE $DB;"

run_sql "CREATE TABLE $DB.$TABLE( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.$TABLE VALUES (\"a\", \"b\");"
run_sql "INSERT INTO $DB.$TABLE VALUES (\"aa\", \"b\");"

run_sql "CREATE TABLE $DB.$TABLE2( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.$TABLE2 VALUES (\"c\", \"d\");"

# backup db
echo "backup start..."
run_br --pd $PD_ADDR backup db --db "$DB" -s "local://$TEST_DIR/$DB" --ratelimit 5 --concurrency 4 --ca $cur/certificates/ca.pem --cert $cur/certificates/client.pem --key $cur/certificates/client-key.pem

run_sql "DROP DATABASE $DB;"

# restore db
echo "restore start..."
run_br restore db --db $DB -s "local://$TEST_DIR/$DB" --pd $PD_ADDR --ca $cur/certificates/ca.pem --cert $cur/certificates/client.pem --key $cur/certificates/client-key.pem

table_count=$(run_sql "use $DB; show tables;" | grep "Tables_in" | wc -l)
if [ "$table_count" -ne "2" ];then
    echo "TEST: [$TEST_NAME] failed!"
    exit 1
fi

run_sql "DROP DATABASE $DB;"

echo "Restart service without tls"
start_services
