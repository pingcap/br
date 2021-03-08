package utils

import (
	"context"
	"os"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	pd "github.com/tikv/pd/client"
)

type versionSuite struct{}

var _ = check.Suite(&versionSuite{})

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

func (s *versionSuite) TestCheckClusterVersion(c *check.C) {
	mock := mockPDClient{
		Client: nil,
	}

	{
		BRReleaseVersion = "v4.0.5"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v4.0.0-rc.1")
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.ErrorMatches, `incompatible.*version v4.0.0-rc.1, try update it to 4.0.0.*`)
	}

	{
		BRReleaseVersion = "v3.0.14"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.1.0-beta.1")
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.ErrorMatches, `incompatible.*version v3.1.0-beta.1, try update it to 3.1.0.*`)
	}

	{
		BRReleaseVersion = "v3.1.1"
		mock.getAllStores = func() []*metapb.Store {
			return tiflash("v3.0.15")
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.ErrorMatches, `incompatible.*version v3.0.15, try update it to 3.1.0.*`)
	}

	{
		BRReleaseVersion = "v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.IsNil)
	}

	{
		BRReleaseVersion = "v3.1.0-beta.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV is too lower to support BR
			return []*metapb.Store{{Version: `v2.1.0`}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.ErrorMatches, ".*TiKV .* don't support BR, please upgrade cluster .*")
	}

	{
		BRReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v3.1.0-beta.2 is incompatible with BR v3.1.0
			return []*metapb.Store{{Version: minTiKVVersion.String()}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.ErrorMatches, "TiKV .* mismatch, please .*")
	}

	{
		BRReleaseVersion = "v3.1.0"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc major version mismatch with BR v3.1.0
			return []*metapb.Store{{Version: "v4.0.0-rc"}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.ErrorMatches, "TiKV .* major version mismatch, please .*")
	}

	{
		BRReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 is incompatible with BR v4.0.0-beta.1
			return []*metapb.Store{{Version: "v4.0.0-beta.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.ErrorMatches, "TiKV .* mismatch, please .*")
	}

	{
		BRReleaseVersion = "v4.0.0-rc.2"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.1 with BR v4.0.0-rc.2 is ok
			return []*metapb.Store{{Version: "v4.0.0-rc.1"}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.IsNil)
	}

	{
		BRReleaseVersion = "v4.0.0-rc.1"
		mock.getAllStores = func() []*metapb.Store {
			// TiKV v4.0.0-rc.2 with BR v4.0.0-rc.1 is ok
			return []*metapb.Store{{Version: "v4.0.0-rc.2"}}
		}
		err := CheckClusterVersion(context.Background(), &mock)
		c.Assert(err, check.IsNil)
	}
}

func (s *versionSuite) TestCompareVersion(c *check.C) {
	c.Assert(semver.New("4.0.0-rc").Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	c.Assert(semver.New("4.0.0-beta.3").Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	c.Assert(semver.New("4.0.0-rc.1").Compare(*semver.New("4.0.0")), check.Equals, -1)
	c.Assert(semver.New("4.0.0-beta.1").Compare(*semver.New("4.0.0")), check.Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-rc-35-g31dae220")).Compare(*semver.New("4.0.0-rc.2")), check.Equals, -1)
	c.Assert(semver.New(removeVAndHash("4.0.0-9-g30f0b014")).Compare(*semver.New("4.0.0-rc.1")), check.Equals, 1)
	c.Assert(semver.New(removeVAndHash("v3.0.0-beta-211-g09beefbe0-dirty")).
		Compare(*semver.New("3.0.0-beta")), check.Equals, 0)
	c.Assert(semver.New(removeVAndHash("v3.0.5-dirty")).
		Compare(*semver.New("3.0.5")), check.Equals, 0)
	c.Assert(semver.New(removeVAndHash("v3.0.5-beta.12-dirty")).
		Compare(*semver.New("3.0.5-beta.12")), check.Equals, 0)
	c.Assert(semver.New(removeVAndHash("v2.1.0-rc.1-7-g38c939f-dirty")).
		Compare(*semver.New("2.1.0-rc.1")), check.Equals, 0)
}

func (s *versionSuite) TestProxyFields(c *check.C) {
	revIndex := map[string]int{
		"http_proxy":  0,
		"https_proxy": 1,
		"no_proxy":    2,
	}
	envs := [...]string{"http_proxy", "https_proxy", "no_proxy"}
	envPreset := [...]string{"http://127.0.0.1:8080", "https://127.0.0.1:8443", "localhost,127.0.0.1"}

	// Exhaust all combinations of those environment variables' selection.
	// Each bit of the mask decided whether this index of `envs` would be set.
	for mask := 0; mask <= 0b111; mask++ {
		for _, env := range envs {
			c.Assert(os.Unsetenv(env), check.IsNil)
		}

		for i := 0; i < 3; i++ {
			if (1<<i)&mask != 0 {
				c.Assert(os.Setenv(envs[i], envPreset[i]), check.IsNil)
			}
		}

		for _, field := range ProxyFields() {
			idx, ok := revIndex[field.Key]
			c.Assert(ok, check.IsTrue)
			c.Assert((1<<idx)&mask, check.Not(check.Equals), 0)
			c.Assert(field.String, check.Equals, envPreset[idx])
		}
	}
}
