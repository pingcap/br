package summary

import "time"

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
