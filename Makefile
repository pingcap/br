PROTOC ?= $(shell which protoc)
PROTOS := $(shell find $(shell pwd) -type f -name '*.proto' -print)
CWD := $(shell pwd)
PACKAGES := go list ./...
PACKAGE_DIRECTORIES := $(PACKAGES) | sed 's/github.com\/pingcap\/br\/*//'
GOCHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'

all: check build test

build:
	GO111MODULE=on go build -race

test:
	GO111MODULE=on go test -race ./...

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

check-all: static lint tidy
	@echo "checking"

check: tools check-all

static: export GO111MODULE=on
static:
	@ # Not running vet and fmt through metalinter becauase it ends up looking at vendor
	gofmt -s -l $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)
	retool do govet --shadow $$($(PACKAGE_DIRECTORIES)) 2>&1 | $(GOCHECKER)

	CGO_ENABLED=0 retool do golangci-lint run --disable-all --deadline 120s \
	  --enable misspell \
	  --enable staticcheck \
	  --enable ineffassign \
	  $$($(PACKAGE_DIRECTORIES))

lint:
	@echo "linting"
	CGO_ENABLED=0 retool do revive -formatter friendly -config revive.toml $$($(PACKAGES))

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	git diff --quiet go.mod go.sum
