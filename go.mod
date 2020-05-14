module github.com/pingcap/br

go 1.13

require (
	cloud.google.com/go/storage v1.5.0
	github.com/aws/aws-sdk-go v1.30.24
	github.com/cheggaaa/pb/v3 v3.0.4
	github.com/fsouza/fake-gcs-server v1.17.0
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gogo/protobuf v1.3.1
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200506114213-c17f16071c53
	github.com/pingcap/kvproto v0.0.0-20200509065137-6a4d5c264a8b
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v0.0.0-20200512060642-fa8905763928
	github.com/pingcap/pd/v4 v4.0.0-rc.1.0.20200511074607-3bb650739add
	github.com/pingcap/tidb v1.1.0-beta.0.20200512142211-0623e4d44563
	github.com/pingcap/tidb-tools v4.0.0-rc.1.0.20200421113014-507d2bb3a15e+incompatible
	github.com/pingcap/tipb v0.0.0-20200417094153-7316d94df1ee
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
