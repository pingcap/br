# BR

[![Build Status](https://internal.pingcap.net/idc-jenkins/job/build_br_master/badge/icon)](https://internal.pingcap.net/idc-jenkins/job/build_br_master/)

**Backup & Restore (BR)** is a command-line tool for distributed backup and restoration of the TiDB cluster data.

## Architecture

![architecture](https://raw.githubusercontent.com/pingcap/docs/891bcb480bf9441b4db3457989eb4ea77dfd38b5/media/br-arch.png)


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

## Installing and Deployment

TODO

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING](./CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License

BR is under the Apache 2.0 license. See the [LICENSE](./LICENSE.md) file for details.
