// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package version

import (
	"context"
	"testing"

	"github.com/coreos/go-semver/semver"
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/tikv/pd/client"

	"github.com/pingcap/br/pkg/version/build"
)

type checkSuite struct{}

var _ = Suite(&checkSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type mockPDClient struct {
	pd.Client
	getAllStores func() []*metapb.Store
}

func (m *mockPDClient) GetAllStores(ctx context.Context, opts ...pd.GetStoreOption) ([]*metapb.Store, error) {
	if m.getAllStores != nil {
		return m.getAllStores(), nil
	}
	return []*metapb.Store{}, nil
}

func tiflash(version string) []*metapb.Store {
	return []*metapb.Store{
		{Version: version, Labels: []*metapb.StoreLabel{{Key: "engine", Value: "tiflash"}}},
	}
}

func (s *checkSuite) TestCheckClusterVersion(c *C) {
	mock := mockPDClient{
		Client: nil,
	}

	{
		build.ReleaseVersion = "v4.0.5"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v4.0.0-rc.1")
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, ErrorMatches, `incompatible.*version v4.0.0-rc.1, try update it to 4.0.0.*`)
	}

	{
		build.ReleaseVersion = "v3.0.14"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.1.0-beta.1")
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, ErrorMatches, `incompatible.*version v3.1.0-beta.1, try update it to 3.1.0.*`)
	}

	{
		build.ReleaseVersion = "v3.1.1"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.0.15")
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, ErrorMatches, `incompatible.*version v3.0.15, try update it to 3.1.0.*`)
	}

	{
		build.ReleaseVersion = "v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, IsNil)
	}

	{
		build.ReleaseVersion = "v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV is too lower to support BR
			return []*metapb.Store{{Version: `v2.1.0`}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, ErrorMatches, ".*TiKV .* don't support BR, please upgrade cluster .*")
	}

	{
		build.ReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v3.1.0-beta.2 is incompatible with BR v3.1.0
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, ErrorMatches, "TiKV .* mismatch, please .*")
	}

	{
		build.ReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc major version mismatch with BR v3.1.0
			return []*metapb.Store{{Version: "v4.0.0-rc"}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, ErrorMatches, "TiKV .* major version mismatch, please .*")
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 is incompatible with BR v4.0.0-beta.1
			return []*metapb.Store{{Version: "v4.0.0-beta.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, ErrorMatches, "TiKV .* mismatch, please .*")
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.1 with BR v4.0.0-rc.2 is ok
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, IsNil)
	}

	{
		build.ReleaseVersion = "v4.0.0-rc.1"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 with BR v4.0.0-rc.1 is ok
			return []*metapb.Store{{Version: "v4.0.0-rc.2"}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, IsNil)
	}
}

func (s *checkSuite) TestCompareVersion(c *C) {
	c.Assert(semver.New("4.0.0-rc").Compare(*semver.New("4.0.0-rc.2")), Equals, -1)
	c.Assert(semver.New("4.0.0-beta.3").Compare(*semver.New("4.0.0-rc.2")), Equals, -1)
	c.Assert(semver.New("4.0.0-rc.1").Compare(*semver.New("4.0.0")), Equals, -1)
	c.Assert(semver.New("4.0.0-beta.1").Compare(*semver.New("4.0.0")), Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")), Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")), Equals, 1)
	c.Assert(semver.New(removeVAndHash("v3.0.0-beta-211-g09beefbe0-dirty")).
		Compare(*semver.New("3.0.0-beta")), Equals, 0)
	c.Assert(semver.New(removeVAndHash("v3.0.5-dirty")).
		Compare(*semver.New("3.0.5")), Equals, 0)
	c.Assert(semver.New(removeVAndHash("v3.0.5-beta.12-dirty")).
		Compare(*semver.New("3.0.5-beta.12")), Equals, 0)
	c.Assert(semver.New(removeVAndHash("v2.1.0-rc.1-7-g38c939f-dirty")).
		Compare(*semver.New("2.1.0-rc.1")), Equals, 0)
}

func (s *checkSuite) TestNextMajorVersion(c *C) {
	build.ReleaseVersion = "v4.0.0-rc.1"
	c.Assert(NextMajorVersion().String(), Equals, "5.0.0")
	build.ReleaseVersion = "4.0.0-rc-35-g31dae220"
	c.Assert(NextMajorVersion().String(), Equals, "5.0.0")
	build.ReleaseVersion = "4.0.0-9-g30f0b014"
	c.Assert(NextMajorVersion().String(), Equals, "5.0.0")

	build.ReleaseVersion = "v5.0.0-rc.2"
	c.Assert(NextMajorVersion().String(), Equals, "6.0.0")
	build.ReleaseVersion = "v5.0.0-master"
	c.Assert(NextMajorVersion().String(), Equals, "6.0.0")
}

func (s *checkSuite) TestExtractTiDBVersion(c *C) {
	vers, err := ExtractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.0.4-1-g06a0bf5")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.4"))

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.0.7")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.0.7"))

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	vers, err = ExtractTiDBVersion("5.7.25-TiDB-v3.0.0-beta-211-g09beefbe0-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.0-beta"))

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5"))

	vers, err = ExtractTiDBVersion("8.0.12-TiDB-v3.0.5-beta.12-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("3.0.5-beta.12"))

	vers, err = ExtractTiDBVersion("5.7.10-TiDB-v2.1.0-rc.1-7-g38c939f-dirty")
	c.Assert(err, IsNil)
	c.Assert(*vers, Equals, *semver.New("2.1.0-rc.1"))

	_, err = ExtractTiDBVersion("")
	c.Assert(err, ErrorMatches, "not a valid TiDB version.*")

	_, err = ExtractTiDBVersion("8.0.12")
	c.Assert(err, ErrorMatches, "not a valid TiDB version.*")

	_, err = ExtractTiDBVersion("not-a-valid-version")
	c.Assert(err, NotNil)
}
