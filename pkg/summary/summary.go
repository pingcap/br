// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package summary

import "time"

// SetUnit set unit "backup/restore" for summary log
func SetUnit(unit string) {
	collector.SetUnit(unit)
}

// CollectSuccessUnit collects success time costs
func CollectSuccessUnit(name string, arg interface{}) {
	collector.CollectSuccessUnit(name, arg)
}

// CollectFailureUnit collects fail reason
func CollectFailureUnit(name string, reason error) {
	collector.CollectFailureUnit(name, reason)
}

// CollectDuration collects log time field
func CollectDuration(name string, t time.Duration) {
	collector.CollectDuration(name, t)
}

// CollectInt collects log int field
func CollectInt(name string, t int) {
	collector.CollectInt(name, t)
}

// Summary outputs summary log
func Summary(name string) {
	collector.Summary(name)
}
