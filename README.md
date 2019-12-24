# BR

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_br_master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_br_master/)
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

[Chinese Document](https://pingcap.com/docs-cn/dev/how-to/maintain/backup-and-restore/br/)

[English Document](https://pingcap.com/docs/dev/how-to/maintain/backup-and-restore/br/)

## Building

1. Install [retool](https://github.com/twitchtv/retool)

2. To build binary and run test:

```bash
$ make
$ make test
```

Notice BR supports building with Go version `Go >= 1.13`

When BR is built successfully, you can find binary in the `bin` directory.

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

BR is under the Apache 2.0 license. See the [LICENSE](./LICENSE.md) file for details.
