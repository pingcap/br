// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package restore

import "github.com/jedib0t/go-pretty/v6/table"

type Template interface {
	// InfoCollect records some infos from checks' results.
	// It used to remind user some meta infos of this import task.
	// e.g. which tables will be import into cluster.
	InfoCollect(msg string)

	// PerformanceCollect mainly collect performance related checks' results.
	// If the performance is not as expect. It will output a warn to user.
	// and it won't break the whole import task.
	PerformanceCollect(passed bool, msg string)

	// CriticalCollect records critical level checks' results.
	// if one of critical check not passed. it will stop import task.
	CriticalCollect(passed bool, msg string)

	// Success represents the whole check has passed or not.
	Success() bool

	// Output print all checks results.
	Output() string
}

type SimpleTemplate struct {
	count  int
	passed bool
	t      table.Writer
}

func NewSimpleTemplate() Template {
	t := table.NewWriter()
	t.AppendHeader(table.Row{"#", "Check Item", "Passed"})
	return &SimpleTemplate{
		0,
		true,
		t,
	}
}

func (c *SimpleTemplate) InfoCollect(msg string) {
}

func (c *SimpleTemplate) PerformanceCollect(passed bool, msg string) {
	c.count++
	c.t.AppendRow(table.Row{c.count, msg, passed})
	c.t.AppendSeparator()
}

func (c *SimpleTemplate) CriticalCollect(passed bool, msg string) {
	if !passed {
		c.passed = false
	}
	c.count++
	c.t.AppendRow(table.Row{c.count, msg, passed})
	c.t.AppendSeparator()
}

func (c *SimpleTemplate) Success() bool {
	return c.passed
}

func (c *SimpleTemplate) Output() string {
	return c.t.Render()
}
