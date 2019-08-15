package restore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	sql_driver "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/parser/model"
)

type Table struct {
	SrcDb      *sql.DB
	DestDb     *sql.DB
	TableName  string
	SrcSchema  *model.TableInfo
	DestSchema *model.TableInfo
}

func CreateTable(srcDns string, destDns string, tableName string) (*Table, error) {
	// Connect databases
	srcDb, err := sql.Open("mysql", srcDns)
	if err != nil {
		return nil, err
	}
	destDb, err := sql.Open("mysql", destDns)
	if err != nil {
		return nil, err
	}
	// Create table in destination database
	rows, err := srcDb.Query(fmt.Sprintf("show create table %s;", tableName))
	if err != nil {
		return nil, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	createTableSql := cols[1]
	_, err = destDb.Exec(createTableSql)
	if err != nil {
		return nil, err
	}
	// Get the schemas of the tables
	srcAddr, srcName, err := ParseDbAddr(srcDns)
	if err != nil {
		return nil, err
	}
	destAddr, destName, err := ParseDbAddr(destDns)
	if err != nil {
		return nil, err
	}
	var srcSchema *model.TableInfo
	var destSchema *model.TableInfo
	err = withRetry(3, time.Millisecond*300, func() error {
		srcSchema, err = getTidbSchema(srcAddr, srcName, tableName)
		if err != nil {
			return err
		}
		destSchema, err = getTidbSchema(destAddr, destName, tableName)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Table{
		SrcDb:      srcDb,
		DestDb:     destDb,
		TableName:  tableName,
		SrcSchema:  srcSchema,
		DestSchema: destSchema,
	}, nil
}

func withRetry(n uint, interval time.Duration, f func() error) error {
	var err error
	for i := uint(0); i < n; i++ {
		err = f()
		if err == nil {
			break
		}
		time.Sleep(interval)
	}
	return err
}

func ParseDbAddr(dns string) (string, string, error) {
	cfg, err := sql_driver.ParseDSN(dns)
	if err != nil {
		return "", "", err
	}
	return cfg.Addr, cfg.DBName, nil
}

func getTidbSchema(addr string, dbName string, tableName string) (*model.TableInfo, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/schema/%s/%s", addr, dbName, tableName))
	if err != nil {
		return nil, err
	}
	var schema *model.TableInfo
	data, err := ioutil.ReadAll(resp.Body)
	err = json.Unmarshal(data, schema)
	if err != nil {
		return nil, err
	}
	return schema, nil
}
