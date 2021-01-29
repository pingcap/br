# BR

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_br_multi_branch/job/master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_br_multi_branch/job/master/)
[![codecov](https://codecov.io/gh/pingcap/br/branch/master/graph/badge.svg)](https://codecov.io/gh/pingcap/br)
[![LICENSE](https://img.shields.io/github/license/pingcap/br.svg)](https://github.com/pingcap/br/blob/master/LICENSE)
[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/pingcap/br)
[![Go Report Card](https://goreportcard.com/badge/github.com/pingcap/br)](https://goreportcard.com/report/github.com/pingcap/br)
[![GitHub release](https://img.shields.io/github/tag/pingcap/br.svg?label=release)](https://github.com/pingcap/br/releases)
[![GitHub release date](https://img.shields.io/github/release-date/pingcap/br.svg)](https://github.com/pingcap/br/releases)

**Backup & Restore (BR)** is a command-line tool for distributed backup and restoration of the TiDB cluster data.

## Architecture

<img src="images/arch.svg?sanitize=true" alt="architecture" width="600"/>

## Documentation

[Chinese Document](https://docs.pingcap.com/zh/tidb/v4.0/backup-and-restore-tool)

[English Document](https://docs.pingcap.com/tidb/v4.0/backup-and-restore-tool)

[Backup SQL Statement](https://docs.pingcap.com/tidb/v4.0/sql-statement-backup)

[Restore SQL Statement](https://docs.pingcap.com/tidb/v4.0/sql-statement-restore)

## Building

To build binary and run test:

```bash
$ make
$ make test
```

Notice BR supports building with Go version `Go >= 1.13`

When BR is built successfully, you can find binary in the `bin` directory.

## Quick start

```sh
# Start TiDB cluster
docker-compose -f docker-compose.yaml rm -s -v && \
docker-compose -f docker-compose.yaml build && \
docker-compose -f docker-compose.yaml up --remove-orphans

# Attach to control container to run BR
docker exec -it br_control_1 bash

# Load testing data to TiDB
go-ycsb load mysql -p workload=core \
    -p mysql.host=tidb -p mysql.port=4000 -p mysql.user=root \
    -p recordcount=100000 -p threadcount=100

# How many rows do we get? 100000 rows.
mysql -uroot -htidb -P4000 -E -e "SELECT COUNT(*) FROM test.usertable"

# Build BR and backup!
make build && \
bin/br backup full --pd pd0:2379 --storage "local:///data/backup/full" \
    --log-file "/logs/br_backup.log"

# Let's drop database.
mysql -uroot -htidb -P4000 -E -e "DROP DATABASE test; SHOW DATABASES;"

# Restore!
bin/br restore full --pd pd0:2379 --storage "local:///data/backup/full" \
    --log-file "/logs/br_restore.log"

# How many rows do we get again? Expected to be 100000 rows.
mysql -uroot -htidb -P4000 -E -e "SELECT COUNT(*) FROM test.usertable"

# Test S3 compatible storage (MinIO).
# Create a bucket to save backup by mc (a MinIO Client).
mc config host add minio $S3_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY && \
mc mb minio/mybucket

# Backup to S3 compatible storage.
bin/br backup full --pd pd0:2379 --storage "s3://mybucket/full" \
    --s3.endpoint="$S3_ENDPOINT"

# Drop database and restore!
mysql -uroot -htidb -P4000 -E -e "DROP DATABASE test; SHOW DATABASES;" && \
bin/br restore full --pd pd0:2379 --storage "s3://mybucket/full" \
    --s3.endpoint="$S3_ENDPOINT"
```

## Compatible test

See [COMPATBILE_TEST](./COMPATIBLE_TEST.md)

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

BR is under the Apache 2.0 license. See the [LICENSE](./LICENSE.md) file for details.
