# Compatible test

## Background

We had some incompatible issues before which made BR cannot restore backuped data in some situations.
So we need a test workflow to check the compatblity.

## Goal

- Restore Backward Compatible

## WorkFlow

### Data Prepare

This workflow needs previous backup data. to get this data. we have the following steps

- Build clusters with previous version.
- Run backup jobs with correspond BR with different storage(s3, gcs, local).

We have 3 different storage. so we should make sure backward compatible seperately.

### Test Content

- Start TiDB cluster with nightly version.
- Build BR binary with current directory.
- Use BR to restore different version backup data one by one.
- Make sure restore data is expected.

### Running tests

Start a cluster with docker-compose and Build br with latest version.

```sh
docker-compose -f docker-compose.yaml rm -s -v && \
docker-compose -f docker-compose.yaml build && \
docker-compose -f docker-compose.yaml up --remove-orphans
```

```sh
docker-compose -f docker-compose.yaml control make compatible_test
```
