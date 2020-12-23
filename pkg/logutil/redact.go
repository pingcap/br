// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package logutil

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var enableRedactLog bool

// InitRedact inits the enableRedactLog
func InitRedact(redact bool) {
	enableRedactLog = redact
}

// NeedRedact returns whether to redact log
func NeedRedact() bool {
	return enableRedactLog
}

// ZapRedactString receives string argument and return omitted information in zap.Field if redact log enabled
func ZapRedactString(key string, val string) zapcore.Field {
	return zap.String(key, RedactString(val))
}

// ZapRedactReflect receives zap.Reflect and return omitted information if redact log enabled
func ZapRedactReflect(key string, val interface{}) zapcore.Field {
	if NeedRedact() {
		return zap.String(key, "?")
	}
	return zap.Reflect(key, val)
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
