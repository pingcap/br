# Compatible test

## Background

We have some incompatible issue before which makes BR cannot restore backuped data in some situations.
So we need a test workflow to check the compatblity.

## Goal

- Restore Backward Compatible

## WorkFlow

### Data Prepare

This workflow needs previous backup data. to get this data. we have the following steps

- Build clusters with previous version.
- Perform sysbench prepae on each cluster.
- Run backup jobs with correspond BR with different storage(s3, gcs, local).
- Collect all backup data and upload to a fileserver.

We have 3 different storage. so we should make sure backward compatible seperately.

To reduce the whole time costs. we choose to download previous data. not to generate them temporarily.
And consider we need update backup data when new version BR published. we need another workflow to prepare data in the future.

### Test Content

- Start TiDB cluster with nightly version.
- Build BR binary with current directory.
- Use BR to restore different version backup data one by one.
- Make sure restore data is expected.

### Running tests

Start cluster with docker-compose and Build br with latest version.

```sh
docker-compose -f docker-compose.yaml rm -s -v && \
docker-compose -f docker-compose.yaml build && \
docker-compose -f docker-compose.yaml up --remove-orphans
```

```sh
docker-compose -f docker-compose.yaml control make compatible_test
```
