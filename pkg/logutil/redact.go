// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package logutil

import (
	"fmt"

	"github.com/pingcap/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// InitRedact inits the enableRedactLog
func InitRedact(redactLog bool) {
	errors.RedactLogEnabled.Store(redactLog)
}

// NeedRedact returns whether to redact log
func NeedRedact() bool {
	return errors.RedactLogEnabled.Load()
}

// ZapRedactReflect receives zap.Reflect and return omitted information if redact log enabled
func ZapRedactReflect(key string, val interface{}) zapcore.Field {
	if NeedRedact() {
		return zap.String(key, "?")
	}
	return zap.Reflect(key, val)
}

// ZapRedactStringer receives stringer argument and return omitted information in zap.Field  if redact log enabled
func ZapRedactStringer(key string, arg fmt.Stringer) zap.Field {
	return zap.Stringer(key, RedactStringer(arg))
}

// RedactString receives string argument and return omitted information if redact log enabled
func RedactString(arg string) string {
	if NeedRedact() {
		return "?"
	}
	return arg
}

// RedactStringer receives stringer argument and return omitted information if redact log enabled
func RedactStringer(arg fmt.Stringer) fmt.Stringer {
	if NeedRedact() {
		return stringer{}
	}
	return arg
}

type stringer struct{}

// String implement fmt.Stringer
func (s stringer) String() string {
	return "?"
}
