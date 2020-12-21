// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package logutil

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	_enableRedactLog bool
)

func InitRedact(redact bool) {
	_enableRedactLog = redact
}

func NeedRedact() bool {
	return _enableRedactLog
}

func RedactField(field zapcore.Field) zapcore.Field {
	if NeedRedact() {
		return zap.Skip()
	}
	return field
}
