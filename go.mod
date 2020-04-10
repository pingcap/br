module github.com/pingcap/br

go 1.13

require (
	cloud.google.com/go/storage v1.4.0
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/aws/aws-sdk-go v1.26.1
	github.com/cheggaaa/pb/v3 v3.0.1
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/fatih/color v1.9.0 // indirect
	github.com/fsouza/fake-gcs-server v1.15.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.1
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/klauspost/cpuid v1.2.0 // indirect
	github.com/mattn/go-runewidth v0.0.7 // indirect
	github.com/montanaflynn/stats v0.5.0 // indirect
	github.com/onsi/ginkgo v1.11.0 // indirect
	github.com/onsi/gomega v1.8.1 // indirect
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/kvproto v0.0.0-20200409042513-05af14db7537
	github.com/pingcap/log v0.0.0-20200117041106-d28c14d3b1cd
	github.com/pingcap/parser v0.0.0-20200326020624-68d423641be5
	github.com/pingcap/pd/v4 v4.0.0-beta.1.0.20200305072537-61d9f9cc35d3
	github.com/pingcap/tidb v0.0.0-20200401141416-959eca8f3a39
	github.com/pingcap/tidb-tools v4.0.0-beta.1.0.20200306084441-875bd09aa3d5+incompatible
	github.com/pingcap/tipb v0.0.0-20200212061130-c4d518eb1d60
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/common v0.4.1
	github.com/sirupsen/logrus v1.4.2
	github.com/spacemonkeygo/openssl v0.0.0-20181017203307-c2dcc5cca94a
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.5
	github.com/syndtr/goleveldb v1.0.1-0.20190625010220-02440ea7a285 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/twitchtv/retool v1.3.7 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.opencensus.io v0.22.2 // indirect
	go.uber.org/zap v1.14.1
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	google.golang.org/api v0.14.0
	google.golang.org/grpc v1.25.1
)

replace github.com/pingcap/kvproto => github.com/yiwu-arbug/kvproto v0.0.0-20200409052246-1b82ce617089
