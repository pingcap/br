This folder contains all tests which relies on external processes such as TiDB.

Unit tests (the `*_test.go` files inside the source directory) should *never* rely on external
programs.

## Preparations

1. The following 7 executables must be copied or linked into these locations:
    * `bin/tidb-server`
	* `bin/tikv-server`
	* `bin/pd-server`
    * `bin/pd-ctl`
	* `bin/go-ycsb`
	* `bin/minio`
    * `bin/tiflash`
    * `bin/cdc`

    The versions must be ≥2.1.0 as usual.

    What's more, there must be dynamic link library for TiFlash, see make target `bin` to learn more. You can install most of dependencies by running `download_tools.sh`.

2. The following programs must be installed:

    * `mysql` (the CLI client)
    * `curl`
    * `s3cmd`

3. The user executing the tests must have permission to create the folder
    `/tmp/backup_restore_test`. All test artifacts will be written into this folder.

## Running

Make sure the path is `br/`

Run `make integration_test` to execute the integration tests. This command will

1. Build `br`
2. Check that all 7 required executables and `br` executable exist
3. Run `tests/up.sh` to build a testing Docker container
4. Execute `tests/run.sh` in the testing Docker container

If the first two steps are done before, you could also run `tests/run.sh` directly.
This script will

1. Start PD, TiKV and TiDB in background with local storage
2. Find out all `tests/*/run.sh` and run it

## Writing new tests

New integration tests can be written as shell scripts in `tests/TEST_NAME/run.sh`.
The script should exit with a nonzero error code on failure.

Several convenient commands are provided:

* `run_sql <SQL>` — Executes an SQL query on the TiDB database
