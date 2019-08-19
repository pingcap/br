package restore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	sql_driver "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"
)

// Table wraps schemas of source table and destination table
type Table struct {
	SrcSchema  *model.TableInfo
	DestSchema *model.TableInfo
}

// CreateTable creates the destination table, then gets schemas of tables
func CreateTable(srcDNS string, destDNS string, tableName string, statusPort int) (*Table, error) {
	// Connect databases
	srcDb, err := sql.Open("mysql", srcDNS)
	if err != nil {
		return nil, err
	}
	destDb, err := sql.Open("mysql", destDNS)
	if err != nil {
		return nil, err
	}
	// Create table in destination database
	rows, err := srcDb.Query(fmt.Sprintf("show create table %s", tableName))
	if err != nil {
		return nil, err
	}
	var (
		name           string
		createTableSQL string
	)
	rows.Next()
	err = rows.Scan(&name, &createTableSQL)
	defer rows.Close()
	if err != nil {
		return nil, err
	}
	log.Info("get table create sql", zap.String("SQL", createTableSQL))
	_, err = destDb.Exec(createTableSQL)
	if err != nil {
		return nil, err
	}
	// Get the schemas of the tables
	srcAddr, srcName, err := parseDbAddr(srcDNS)
	if err != nil {
		return nil, err
	}
	log.Info("parse db dns", zap.String("DNS", srcDNS), zap.Reflect("Addr", srcAddr), zap.String("Name", srcName))
	destAddr, destName, err := parseDbAddr(destDNS)
	if err != nil {
		return nil, err
	}
	log.Info("parse db dns", zap.String("DNS", destDNS), zap.Reflect("Addr", destAddr), zap.String("Name", destName))
	var srcSchema *model.TableInfo
	var destSchema *model.TableInfo
	err = withRetry(3, time.Millisecond*300, func() error {
		srcSchema, err = getTidbSchema(fmt.Sprintf("%s:%d", srcAddr.IP, statusPort), srcName, tableName)
		if err != nil {
			return err
		}
		destSchema, err = getTidbSchema(fmt.Sprintf("%s:%d", destAddr.IP, statusPort), destName, tableName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Table{
		SrcSchema:  srcSchema,
		DestSchema: destSchema,
	}, nil
}

func withRetry(n uint, interval time.Duration, f func() error) error {
	var err error
	for i := uint(0); i < n; i++ {
		err = f()
		if err == nil {
			return nil
		}
		time.Sleep(interval)
	}
	return err
}

func parseDbAddr(dns string) (*net.TCPAddr, string, error) {
	cfg, err := sql_driver.ParseDSN(dns)
	if err != nil {
		return nil, "", err
	}
	r, err := net.ResolveTCPAddr("tcp", cfg.Addr)
	if err != nil {
		return nil, "", err
	}
	return r, cfg.DBName, nil
}

func getTidbSchema(addr string, dbName string, tableName string) (*model.TableInfo, error) {
	url := fmt.Sprintf("http://%s/schema/%s/%s", addr, dbName, tableName)
	log.Info("query table schema", zap.String("URL", url))
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var schema model.TableInfo
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &schema)
	if err != nil {
		return nil, err
	}
	return &schema, nil
}
