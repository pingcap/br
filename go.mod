module github.com/pingcap/br

go 1.12

require (
	cloud.google.com/go v0.45.1 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.3.0
	github.com/google/btree v1.0.0
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/google/pprof v0.0.0-20190723021845-34ac40c74b70 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/kr/pty v1.1.8 // indirect
	github.com/overvenus/br v0.0.0-20190822094751-e3b8df9d3a0a
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/kvproto v0.0.0-20190905044949-498b62681de0
	github.com/pingcap/log v0.0.0-20190715063458-479153f07ebd
	github.com/pingcap/parser v0.0.0-20190822024127-41d48df05864
	github.com/pingcap/pd v0.0.0-20190806095100-82f1dd11d823
	github.com/pingcap/tidb v0.0.0-20190822044546-9312090275b2
	github.com/prometheus/client_golang v1.1.0
	github.com/rogpeppe/go-internal v1.3.1 // indirect
	github.com/spf13/cobra v0.0.5
	github.com/spf13/pflag v1.0.3
	github.com/twinj/uuid v1.0.0
	go.opencensus.io v0.22.1 // indirect
	go.uber.org/zap v1.10.0
	golang.org/x/crypto v0.0.0-20190829043050-9756ffdc2472 // indirect
	golang.org/x/exp v0.0.0-20190829153037-c13cbed26979 // indirect
	golang.org/x/image v0.0.0-20190902063713-cb417be4ba39 // indirect
	golang.org/x/mobile v0.0.0-20190830201351-c6da95954960 // indirect
	golang.org/x/net v0.0.0-20190827160401-ba9fcec4b297 // indirect
	golang.org/x/sys v0.0.0-20190904154756-749cb33beabd // indirect
	golang.org/x/tools v0.0.0-20190905035308-adb45749da8e // indirect
	google.golang.org/appengine v1.6.2 // indirect
	google.golang.org/grpc v1.23.0
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	honnef.co/go/tools v0.0.1-2019.2.3 // indirect
)

// replace github.com/pingcap/kvproto => /home/stn/kvproto
