all: bin/goimports bin/govet bin/revive bin/overalls bin/golangci-lint

bin/goimports:
	go build -o $@ golang.org/x/tools/cmd/goimports

bin/govet:
	go build -o $@ github.com/dnephin/govet

bin/revive:
	go build -o $@ github.com/mgechev/revive

bin/overalls:
	go build -o $@ github.com/go-playground/overalls

bin/golangci-lint:
	go build -o $@ github.com/golangci/golangci-lint/cmd/golangci-lint
