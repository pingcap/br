package tools

// This file ensures `go mod tidy` will not delete entries to all tools.

import (
	// golangci-lint is a package-based linter
	_ "github.com/golangci/golangci-lint/pkg/commands"

	// revive is a file-based linter
	_ "github.com/mgechev/revive/lint"

	// goimports formats Go code
	_ "golang.org/x/tools/imports"

	// overalls performs test coverage
	_ "github.com/go-playground/overalls"

	// govet checks for code correctness
	_ "github.com/dnephin/govet"

	// failpoint enables manual 'failure' of some execution points.
	_ "github.com/pingcap/failpoint"

	// errdoc-gen generates errors.toml.
	_ "github.com/pingcap/tiup/components/errdoc/errdoc-gen"
)
