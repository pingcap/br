package restore

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/parser/model"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"go.uber.org/zap"
)

var _ = Suite(&testRestoreSchemaSuite{})

type testRestoreSchemaSuite struct {
	server    *server.Server
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	tidbdrv   *server.TiDBDriver
	dom       *domain.Domain
}

func TestT(t *testing.T) {
	TestingT(t)
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *testRestoreSchemaSuite) SetUpSuite(c *C) {
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.MustNewMVCCStore()
		store, err := mockstore.NewMockTikvStore(
			mockstore.WithCluster(s.cluster),
			mockstore.WithMVCCStore(s.mvccStore),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.SetSchemaLease(0)
		session.DisableStats4Test()
	}
	var err error
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testRestoreSchemaSuite) TearDownSuite(c *C) {
	testleak.AfterTest(c)()
}

func (s *testRestoreSchemaSuite) startServer(c *C) {
	mvccStore := mocktikv.MustNewMVCCStore()
	var err error
	s.store, err = mockstore.NewMockTikvStore(mockstore.WithMVCCStore(mvccStore))
	c.Assert(err, IsNil)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.tidbdrv = server.NewTiDBDriver(s.store)

	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Store = "tikv"
	cfg.Status.StatusPort = 10090
	cfg.Status.ReportStatus = true

	svr, err := server.NewServer(cfg, s.tidbdrv)
	c.Assert(err, IsNil)
	s.server = svr
	go svr.Run()
	waitUntilServerOnline(cfg.Status.StatusPort)
}

func (s *testRestoreSchemaSuite) stopServer(c *C) {
	if s.dom != nil {
		s.dom.Close()
	}
	if s.store != nil {
		s.store.Close()
	}
	if s.server != nil {
		s.server.Close()
	}
}

func (s *testRestoreSchemaSuite) TestRestoreAutoIncID(c *C) {
	s.startServer(c)
	defer s.stopServer(c)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (10);")

	// Query the current AutoIncID
	autoIncID, err := strconv.ParseUint(tk.MustQuery("admin show t next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))

	// Get schemas of db and table
	info, err := s.dom.GetSnapshotInfoSchema(math.MaxUint64)
	c.Assert(err, IsNil, Commentf("Error get snapshot info schema: %s", err))
	dbInfo, exists := info.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue, Commentf("Error get db info"))
	tableInfo, err := info.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil, Commentf("Error get table info: %s", err))

	// Get the next AutoIncID
	idAlloc := autoid.NewAllocator(s.store, dbInfo.ID, false)
	globalAutoID, err := idAlloc.NextGlobalAutoID(tableInfo.Meta().ID)
	c.Assert(err, IsNil, Commentf("Error allocate next auto id"))
	c.Assert(autoIncID == uint64(globalAutoID), IsTrue, Commentf("Error wrong next auto id: autoIncID=%d, globalAutoID=%d", autoIncID, globalAutoID))

	// Alter AutoIncID to the next AutoIncID + 100
	tableInfo.Meta().AutoIncID = globalAutoID + 100
	err = AlterAutoIncID("test", tableInfo.Meta(), "root@tcp(127.0.0.1:4001)/")
	c.Assert(err, IsNil, Commentf("Error alter auto inc id: %s", err))

	// Check if AutoIncID is altered successfully
	autoIncID, err = strconv.ParseUint(tk.MustQuery("admin show t next_row_id").Rows()[0][3].(string), 10, 64)
	c.Assert(err, IsNil, Commentf("Error query auto inc id: %s", err))
	c.Assert(autoIncID == uint64(globalAutoID+100), IsTrue, Commentf("Error alter auto inc id: autoIncID=%d, globalAutoID=%d", autoIncID, globalAutoID))
}

type configOverrider func(*mysql.Config)

const retryTime = 100

var defaultDSNConfig = mysql.Config{
	User: "root",
	Net:  "tcp",
	Addr: "127.0.0.1:4001",
}

// getDSN generates a DSN string for MySQL connection.
func getDSN(overriders ...configOverrider) string {
	var cfg = defaultDSNConfig
	for _, overrider := range overriders {
		if overrider != nil {
			overrider(&cfg)
		}
	}
	return cfg.FormatDSN()
}

func waitUntilServerOnline(statusPort uint) {
	// connect server
	retry := 0
	for ; retry < retryTime; retry++ {
		time.Sleep(time.Millisecond * 10)
		db, err := sql.Open("mysql", getDSN())
		if err == nil {
			db.Close()
			break
		}
	}
	if retry == retryTime {
		log.Fatal("failed to connect DB in every 10 ms", zap.Int("retryTime", retryTime))
	}
	// connect http status
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", statusPort)
	for retry = 0; retry < retryTime; retry++ {
		resp, err := http.Get(statusURL)
		if err == nil {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	if retry == retryTime {
		log.Fatal("failed to connect HTTP status in every 10 ms", zap.Int("retryTime", retryTime))
	}
}
