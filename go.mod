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
	github.com/pingcap/errors v0.11.5-0.20200902104258-eba4f1d8f6de
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/kvproto v0.0.0-20200828054126-d677e6fd224a
	github.com/pingcap/log v0.0.0-20200828042413-fce0951f1463
	github.com/pingcap/parser v0.0.0-20200909072241-6dac7bb703e2
	github.com/pingcap/tidb v1.1.0-beta.0.20200910052409-5d52a34b2476
	github.com/pingcap/tidb-tools v4.0.5-0.20200820092506-34ea90c93237+incompatible
	github.com/pingcap/tipb v0.0.0-20200618092958-4fad48b4c8c3
	github.com/prometheus/client_golang v1.5.1
	github.com/prometheus/common v0.9.1
	github.com/satori/go.uuid v1.2.0
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/tikv/pd v1.1.0-beta.0.20200820084926-bcfa77a7a593
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.16.0
	golang.org/x/oauth2 v0.0.0-20191202225959-858c2ad4c8b6
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	google.golang.org/api v0.15.1
	google.golang.org/grpc v1.26.0
)

replace github.com/pingcap/kvproto => github.com/pingcap/kvproto v0.0.0-20200908120510-1f17b31feb24
