// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package logutil

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	_enableRedactLog bool
)

// InitRedact inits the _enableRedactLog
func InitRedact(redact bool) {
	_enableRedactLog = redact
}

// NeedRedact returns whether to redact log
func NeedRedact() bool {
	return _enableRedactLog
}

// Redact returns zap.Skip() if we need to redact this field
func Redact(field zapcore.Field) zapcore.Field {
	if NeedRedact() {
		return zap.Skip()
	}
	return field
}
