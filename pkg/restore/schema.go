package restore

import (
	"bytes"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	tidbTable "github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/format"
	"github.com/twinj/uuid"
	"go.uber.org/zap"
)

// FileGroup wraps the schema and files of a table
type FileGroup struct {
	UUID   uuid.UUID
	Db     *model.DBInfo
	Schema *model.TableInfo
	Files  []*FilePair
}

// Database wraps the schema and tables of a database
type Database struct {
	Schema     *model.DBInfo
	FileGroups []*FileGroup
	Tables     []*model.TableInfo
}

// FilePair wraps a default cf file & a write cf file
type FilePair struct {
	Default *backup.File
	Write   *backup.File
}

// GetFileGroups returns file groups by name
func (db *Database) GetFileGroups(name string) []*FileGroup {
	fileGroups := make([]*FileGroup, 0)
	for _, group := range db.FileGroups {
		if group.Schema.Name.String() == name {
			fileGroups = append(fileGroups, group)
		}
	}
	return fileGroups
}

// GetTable returns a table info by name
func (db *Database) GetTable(name string) *model.TableInfo {
	for _, table := range db.Tables {
		if table.Name.String() == name {
			return table
		}
	}
	return nil
}

// CreateTable executes a CREATE TABLE SQL
func CreateTable(dbName string, table *model.TableInfo, dsn string) error {
	dbDSN := dsn + url.QueryEscape(dbName)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		log.Error("open database failed", zap.String("addr", dbDSN), zap.Error(err))
		return errors.Trace(err)
	}
	createSQL := GetCreateTableSQL(table)
	_, err = db.Exec(createSQL)
	if err != nil {
		log.Error("create table failed",
			zap.String("SQL", createSQL),
			zap.String("db", dbName),
			zap.String("addr", dsn),
			zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// CreateDatabase executes a CREATE DATABASE SQL
func CreateDatabase(schema *model.DBInfo, dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Error("open database failed", zap.String("addr", dsn), zap.Error(err))
		return errors.Trace(err)
	}
	createSQL := GetCreateDatabaseSQL(schema)
	_, err = db.Exec(createSQL)
	if err != nil {
		log.Error("create database failed", zap.String("SQL", createSQL), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// AnalyzeTable executes a ANALYZE TABLE SQL
func AnalyzeTable(dbName string, table *model.TableInfo, dsn string) error {
	dbDSN := dsn + url.QueryEscape(dbName)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		log.Error("open database failed", zap.String("addr", dbDSN), zap.Error(err))
		return errors.Trace(err)
	}
	analyzeSQL := fmt.Sprintf("ANALYZE TABLE %s", table.Name.String())
	_, err = db.Exec(analyzeSQL)
	if err != nil {
		log.Error("analyze table failed", zap.String("SQL", analyzeSQL), zap.String("db", dbName), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// AlterAutoIncID alters max auto-increment id of table
func AlterAutoIncID(dbName string, table *model.TableInfo, dsn string) error {
	dbDSN := dsn + url.QueryEscape(dbName)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		log.Error("open database failed", zap.String("addr", dbDSN), zap.Error(err))
		return errors.Trace(err)
	}
	alterIDSQL := fmt.Sprintf("ALTER TABLE %s auto_increment = %d", table.Name.String(), table.AutoIncID)
	_, err = db.Exec(alterIDSQL)
	if err != nil {
		log.Error("alter auto inc id failed", zap.String("SQL", alterIDSQL), zap.String("db", dbName), zap.Error(err))
		return errors.Trace(err)
	}
	log.Info("alter auto inc id",
		zap.String("table", table.Name.String()),
		zap.String("db", dbName),
		zap.Int64("auto_inc_id", table.AutoIncID),
	)
	return nil
}

// GetCreateDatabaseSQL generates a CREATE DATABASE SQL from DBInfo
func GetCreateDatabaseSQL(db *model.DBInfo) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE DATABASE IF NOT EXISTS %s", db.Name.String())
	fmt.Fprintf(&buf, " CHARACTER SET %s COLLATE %s", db.Charset, db.Collate)
	buf.WriteString(";")

	return buf.String()
}

// GetCreateTableSQL generates a CREATE TABLE SQL from TableInfo
func GetCreateTableSQL(t *model.TableInfo) string {
	var buf bytes.Buffer

	tblCharset := t.Charset
	tblCollate := t.Collate
	fmt.Fprintf(&buf, "CREATE TABLE IF NOT EXISTS %s (\n", t.Name)
	var pkCol *model.ColumnInfo
	for i, col := range t.Columns {
		fmt.Fprintf(&buf, "  %s %s", col.Name.String(), getColumnTypeDesc(col))
		if col.Charset != "binary" {
			if col.Charset != tblCharset || col.Collate != tblCollate {
				fmt.Fprintf(&buf, " CHARACTER SET %s COLLATE %s", col.Charset, col.Collate)
			}
		}
		if col.IsGenerated() {
			fmt.Fprintf(&buf, " GENERATED ALWAYS AS (%s)", col.GeneratedExprString)
			if col.GeneratedStored {
				buf.WriteString(" STORED")
			} else {
				buf.WriteString(" VIRTUAL")
			}
		}
		if mysql.HasAutoIncrementFlag(col.Flag) {
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if mysql.HasNotNullFlag(col.Flag) {
				buf.WriteString(" NOT NULL")
			}
			// default values are not shown for generated columns in MySQL
			if !mysql.HasNoDefaultValueFlag(col.Flag) && !col.IsGenerated() {
				defaultValue := col.GetDefaultValue()
				switch defaultValue {
				case nil:
					if !mysql.HasNotNullFlag(col.Flag) {
						if col.Tp == mysql.TypeTimestamp {
							buf.WriteString(" NULL")
						}
						buf.WriteString(" DEFAULT NULL")
					}
				case "CURRENT_TIMESTAMP":
					buf.WriteString(" DEFAULT CURRENT_TIMESTAMP")
					if col.Decimal > 0 {
						buf.WriteString(fmt.Sprintf("(%d)", col.Decimal))
					}
				default:
					defaultValStr := fmt.Sprintf("%v", defaultValue)

					if col.Tp == mysql.TypeBit {
						defaultValBinaryLiteral := types.BinaryLiteral(defaultValStr)
						fmt.Fprintf(&buf, " DEFAULT %s", defaultValBinaryLiteral.ToBitLiteralString(true))
					} else {
						fmt.Fprintf(&buf, " DEFAULT '%s'", format.OutputFormat(defaultValStr))
					}
				}
			}
			if mysql.HasOnUpdateNowFlag(col.Flag) {
				buf.WriteString(" ON UPDATE CURRENT_TIMESTAMP")
				buf.WriteString(tidbTable.OptionalFsp(&col.FieldType))
			}
		}
		if len(col.Comment) > 0 {
			fmt.Fprintf(&buf, " COMMENT '%s'", format.OutputFormat(col.Comment))
		}
		if i != len(t.Cols())-1 {
			buf.WriteString(",\n")
		}
		if t.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			pkCol = col
		}
	}

	if pkCol != nil {
		// If PKIsHandle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		fmt.Fprintf(&buf, "  PRIMARY KEY (%s)", pkCol.Name.String())
	}

	publicIndices := make([]*model.IndexInfo, 0, len(t.Indices))
	for _, idx := range t.Indices {
		if idx.State == model.StatePublic {
			publicIndices = append(publicIndices, idx)
		}
	}
	if len(publicIndices) > 0 {
		buf.WriteString(",\n")
	}

	for i, idx := range publicIndices {
		if idx.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idx.Unique {
			fmt.Fprintf(&buf, "  UNIQUE KEY %s ", idx.Name.String())
		} else {
			fmt.Fprintf(&buf, "  KEY %s ", idx.Name.String())
		}

		cols := make([]string, 0, len(idx.Columns))
		for _, c := range idx.Columns {
			colInfo := c.Name.String()
			if c.Length != types.UnspecifiedLength {
				colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
			}
			cols = append(cols, colInfo)
		}
		fmt.Fprintf(&buf, "(%s)", strings.Join(cols, ","))
		if i != len(publicIndices)-1 {
			buf.WriteString(",\n")
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// Because we only support case sensitive utf8_bin collate, we need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	if len(tblCollate) == 0 {
		// If we can not find default collate for the given charset,
		// do not show the collate part.
		fmt.Fprintf(&buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		fmt.Fprintf(&buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblCollate)
	}

	// Displayed if the compression typed is set.
	if len(t.Compression) != 0 {
		fmt.Fprintf(&buf, " COMPRESSION='%s'", t.Compression)
	}

	if t.ShardRowIDBits > 0 {
		fmt.Fprintf(&buf, "/*!90000 SHARD_ROW_ID_BITS=%d ", t.ShardRowIDBits)
		if t.PreSplitRegions > 0 {
			fmt.Fprintf(&buf, "PRE_SPLIT_REGIONS=%d ", t.PreSplitRegions)
		}
		buf.WriteString("*/")
	}

	if len(t.Comment) > 0 {
		fmt.Fprintf(&buf, " COMMENT='%s'", format.OutputFormat(t.Comment))
	}
	// add partition info here.
	appendPartitionInfo(t.Partition, &buf)
	buf.WriteString(";")

	return buf.String()
}

func getColumnTypeDesc(col *model.ColumnInfo) string {
	desc := col.FieldType.CompactStr()
	if mysql.HasUnsignedFlag(col.Flag) && col.Tp != mysql.TypeBit && col.Tp != mysql.TypeYear {
		desc += " unsigned"
	}
	if mysql.HasZerofillFlag(col.Flag) && col.Tp != mysql.TypeYear {
		desc += " zerofill"
	}
	return desc
}

func appendPartitionInfo(partitionInfo *model.PartitionInfo, buf *bytes.Buffer) {
	if partitionInfo == nil {
		return
	}
	if partitionInfo.Type == model.PartitionTypeHash {
		fmt.Fprintf(buf, "\nPARTITION BY HASH( %s )", partitionInfo.Expr)
		fmt.Fprintf(buf, "\nPARTITIONS %d", partitionInfo.Num)
		return
	}
	// this if statement takes care of range columns case
	if partitionInfo.Columns != nil && partitionInfo.Type == model.PartitionTypeRange {
		buf.WriteString("\nPARTITION BY RANGE COLUMNS(")
		for i, col := range partitionInfo.Columns {
			buf.WriteString(col.L)
			if i < len(partitionInfo.Columns)-1 {
				buf.WriteString(",")
			}
		}
		buf.WriteString(") (\n")
	} else {
		fmt.Fprintf(buf, "\nPARTITION BY %s ( %s ) (\n", partitionInfo.Type.String(), partitionInfo.Expr)
	}
	for i, def := range partitionInfo.Definitions {
		lessThans := strings.Join(def.LessThan, ",")
		fmt.Fprintf(buf, "  PARTITION %s VALUES LESS THAN (%s)", def.Name, lessThans)
		if i < len(partitionInfo.Definitions)-1 {
			buf.WriteString(",\n")
		} else {
			buf.WriteString("\n")
		}
	}
	buf.WriteString(")")
}
