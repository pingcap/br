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

run_sql "CREATE TABLE $DB.usertable2 ( \
  YCSB_KEY varchar(64) NOT NULL, \
  FIELD0 varchar(1) DEFAULT NULL, \
  PRIMARY KEY (YCSB_KEY) \
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

run_sql "INSERT INTO $DB.usertable2 VALUES (\"c\", \"d\");"

# TODO: use backup db, once it is supported.
# backup full
echo "backup start..."
echo "TEST_GCS_BUCKET: ${TEST_GCS_BUCKET}"
echo "TEST_GCS_CREDENTIALS: ${TEST_GCS_CREDENTIALS}"
run_br --pd $PD_ADDR backup full -s "gcs://${TEST_GCS_BUCKET}/$DB" --ratelimit 5 --concurrency 4 --fastchecksum true --only-meta --gcs.credentials_file "${TEST_GCS_CREDENTIALS}" --gcs.predefined_acl "bucketOwnerRead" --gcs.storage_class "NEARLINE"

run_sql "DROP DATABASE $DB;"

# restore db
echo "restore start..."
run_br restore db --db $DB -s "gcs://${TEST_GCS_BUCKET}/$DB" --pd $PD_ADDR --only-meta --gcs.credentials_file "${TEST_GCS_CREDENTIALS}"

run_sql "DROP DATABASE $DB;"
