module github.com/pingcap/br

go 1.13

require (
	cloud.google.com/go/storage v1.5.0
	github.com/aws/aws-sdk-go v1.30.24
	github.com/cheggaaa/pb/v3 v3.0.4
	github.com/coreos/go-semver v0.3.0
	github.com/fsouza/fake-gcs-server v1.17.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200603062251-b230c36c413c
	github.com/pingcap/kvproto v0.0.0-20200518112156-d4aeb467de29
	github.com/pingcap/log v0.0.0-20200511115504-543df19646ad
	github.com/pingcap/parser v0.0.0-20200609110328-c65941b9fbb3
	github.com/pingcap/pd/v4 v4.0.0-rc.2.0.20200520083007-2c251bd8f181
	github.com/pingcap/tidb v1.1.0-beta.0.20200622093932-c98514e7516e
	github.com/pingcap/tidb-tools v4.0.0+incompatible
	github.com/pingcap/tipb v0.0.0-20200615034523-dcfcea0b5965
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/common v0.9.1
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.15.0
	golang.org/x/oauth2 v0.0.0-20191202225959-858c2ad4c8b6
	google.golang.org/api v0.15.1
	google.golang.org/grpc v1.26.0
)
