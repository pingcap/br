PROTOC ?= $(shell which protoc)
PROTOS := $(shell find $(shell pwd) -type f -name '*.proto' -print)
CWD := $(shell pwd)

build:
	GO111MODULE=on go build

pb: tools
	@PATH=$(CWD)/_tools/bin:$$PATH && for p in $(PROTOS); do { \
      dir=`dirname $$p`; \
      $(PROTOC) --proto_path $$dir --go_out=$$dir $$p; \
      name=`basename -s .proto $$p`; \
      goimports -w $$dir/$$name.pb.go; \
	} done;

tools:
	@echo "install tools..."
	@GO111MODULE=off go get github.com/twitchtv/retool
	@GO111MODULE=off retool sync
