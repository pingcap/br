# BR (Backup and Restore) Change Log
All notable changes to this project are documented in this file.
See also,
- [TiDB Changelog](https://github.com/pingcap/tidb/blob/master/CHANGELOG.md),
- [TiKV Changelog](https://github.com/tikv/tikv/blob/master/CHANGELOG.md),
- [PD Changelog](https://github.com/pingcap/pd/blob/master/CHANGELOG.md).

## [3.1.0-beta.1] - 2020-1-10

- Fix the inaccurate backup progress information [#127](https://github.com/pingcap/br/pull/127)
- Improve the performance of splitting Regions [#122](https://github.com/pingcap/br/pull/122)
- Add the backup and restore feature for partitioned tables [#137](https://github.com/pingcap/br/pull/137)
- Add the feature of automatically scheduling PD schedulers [#123](https://github.com/pingcap/br/pull/123)
- Fix the issue that data is overwritten after non `PKIsHandle` tables are restored [#139](https://github.com/pingcap/br/pull/139)

## [3.1.0-beta] - 2019-12-20

Initial release of the distributed backup and restore tool
