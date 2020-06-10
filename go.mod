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
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200506114213-c17f16071c53
	github.com/pingcap/kvproto v0.0.0-20200518112156-d4aeb467de29
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v3.1.0-beta.2.0.20200425032215-994651e9b6df+incompatible
	github.com/pingcap/pd/v3 v3.1.1-0.20200426091027-e639f0b1e62b
	github.com/pingcap/pd/v4 v4.0.2 // indirect
	github.com/pingcap/tidb v1.1.0-beta.0.20200426082429-52b31342cb0e
	github.com/pingcap/tidb-tools v4.0.0-rc.2.0.20200521050818-6dd445d83fe0+incompatible
	github.com/pingcap/tipb v0.0.0-20200426072603-ce17d2d03251
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/common v0.4.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/multierr v1.5.0
	go.uber.org/zap v1.14.1
	golang.org/x/oauth2 v0.0.0-20191202225959-858c2ad4c8b6
	google.golang.org/api v0.15.0
	google.golang.org/grpc v1.26.0
)
