package conn

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/mock/mockid"
	"github.com/pingcap/pd/server"
	"google.golang.org/grpc"
)

func TestT(t *testing.T) {
	TestingT(t)
}

func TestClient(t *testing.T) {
	server.EnableZap = true
	TestingT(t)
}

var _ = Suite(&testClientSuite{})

type idAllocator struct {
	allocator *mockid.IDAllocator
}

func (i *idAllocator) alloc() uint64 {
	id, _ := i.allocator.Alloc()
	return id
}

var (
	regionIDAllocator = &idAllocator{allocator: &mockid.IDAllocator{}}
	// Note: IDs below are entirely arbitrary. They are only for checking
	// whether GetRegion/GetStore works.
	// If we alloc ID in client in the future, these IDs must be updated.
	stores = []*metapb.Store{
		{Id: 1,
			Address: "localhost:1",
		},
		{Id: 2,
			Address: "localhost:2",
		},
		{Id: 3,
			Address: "localhost:3",
		},
		{Id: 4,
			Address: "localhost:4",
		},
	}

	peers = []*metapb.Peer{
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[0].GetId(),
		},
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[1].GetId(),
		},
		{Id: regionIDAllocator.alloc(),
			StoreId: stores[2].GetId(),
		},
	}
)

type testClientSuite struct {
	ctx     context.Context
	cancel  context.CancelFunc
	cleanup server.CleanupFunc

	srv *server.Server
	mgr *Mgr
}

func (s *testClientSuite) SetUpSuite(c *C) {
	var err error
	_, s.srv, s.cleanup, err = server.NewTestServer(c)
	c.Assert(err, IsNil)
	s.ctx, s.cancel = context.WithCancel(context.Background())

	conn, err := grpc.Dial(
		strings.TrimPrefix(s.srv.GetAddr(), "http://"), grpc.WithInsecure())
	c.Assert(err, IsNil)
	defer conn.Close()

	grpcPDClient := pdpb.NewPDClient(conn)
	mustWaitLeader(c, map[string]*server.Server{s.srv.GetAddr(): s.srv})
	bootstrapServer(c, newHeader(s.srv), grpcPDClient)

	cluster := s.srv.GetRaftCluster()
	c.Assert(cluster, NotNil)
	for _, store := range stores {
		_, err := s.srv.PutStore(
			s.ctx,
			&pdpb.PutStoreRequest{Header: newHeader(s.srv), Store: store},
		)
		c.Assert(err, IsNil)
	}

	// Disable pd connection check.
	pdGet = func(string, string, *http.Client) ([]byte, error) {
		return []byte{}, nil
	}
	s.mgr, err = NewMgr(
		s.ctx, strings.TrimPrefix(s.srv.GetAddr(), "http://"))
	c.Assert(err, IsNil)
}

func (s *testClientSuite) TearDownSuite(c *C) {
	s.cancel()

	s.cleanup()
}

func mustWaitLeader(c *C, svrs map[string]*server.Server) *server.Server {
	for i := 0; i < 500; i++ {
		for _, s := range svrs {
			if s.IsLeader() {
				return s
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.Fatal("no leader")
	return nil
}

func newHeader(srv *server.Server) *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: srv.ClusterID(),
	}
}

func bootstrapServer(c *C, header *pdpb.RequestHeader, client pdpb.PDClient) {
	regionID := regionIDAllocator.alloc()
	region := &metapb.Region{
		Id: regionID,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
		Peers: peers[:1],
	}
	req := &pdpb.BootstrapRequest{
		Header: header,
		Store:  stores[0],
		Region: region,
	}
	_, err := client.Bootstrap(context.Background(), req)
	c.Assert(err, IsNil)
}
