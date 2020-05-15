// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/spf13/pflag"
)

var _ = Suite(&testCommonSuite{})

type testCommonSuite struct{}

type fakeValue string

func (f fakeValue) String() string {
	return string(f)
}

func (f fakeValue) Set(string) error {
	panic("implement me")
}

func (f fakeValue) Type() string {
	panic("implement me")
}

func (*testCommonSuite) TestUrlNoQuery(c *C) {
	flag := &pflag.Flag{
		Name:  flagStorage,
		Value: fakeValue("s3://some/what?secret=a123456789&key=987654321"),
	}

	field := flagToZapField(flag)
	c.Assert(field.Key, Equals, flagStorage)
	c.Assert(field.Interface.(fmt.Stringer).String(), Equals, "s3://some/what")
}
