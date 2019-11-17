package checksum

import (
	"context"
	"log"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils"
)

// ExecutorBuilder is used to build a "kv.Request".
type ExecutorBuilder struct {
	table *model.TableInfo
	ts    uint64

	oldTable *utils.Table
}

// NewExecutorBuilder returns a new executor builder
func NewExecutorBuilder(table *model.TableInfo, ts uint64) *ExecutorBuilder {
	return &ExecutorBuilder{
		table: table,
		ts:    ts,
	}
}

// SetOldTable set a old table info to the builder
func (builder *ExecutorBuilder) SetOldTable(oldTable *utils.Table) *ExecutorBuilder {
	builder.oldTable = oldTable
	return builder
}

// Build builds a checksum executor
func (builder *ExecutorBuilder) Build() (*Executor, error) {
	reqs, err := buildChecksumRequest(builder.table, builder.oldTable, builder.ts)
	if err != nil {
		return nil, err
	}
	return &Executor{reqs: reqs}, nil
}

func buildChecksumRequest(
	newTable *model.TableInfo,
	oldTable *utils.Table,
	startTS uint64,
) ([]*kv.Request, error) {
	var partDefs []model.PartitionDefinition
	if part := newTable.Partition; part != nil {
		partDefs = part.Definitions
	}

	reqs := make([]*kv.Request, 0, (len(newTable.Indices)+1)*(len(partDefs)+1))
	rs, err := buildRequest(newTable, newTable.ID, oldTable, startTS)
	if err != nil {
		return nil, err
	}
	reqs = append(reqs, rs...)

	for _, partDef := range partDefs {
		rs, err := buildRequest(newTable, partDef.ID, oldTable, startTS)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reqs = append(reqs, rs...)
	}

	return reqs, nil
}

func buildRequest(
	tableInfo *model.TableInfo,
	tableID int64,
	oldTable *utils.Table,
	startTS uint64,
) ([]*kv.Request, error) {
	reqs := make([]*kv.Request, 0)
	req, err := buildTableRequest(tableID, oldTable, startTS)
	if err != nil {
		return nil, err
	}
	reqs = append(reqs, req)

	for _, indexInfo := range tableInfo.Indices {
		if indexInfo.State != model.StatePublic {
			continue
		}
		var oldTableID int64
		var oldIndexInfo *model.IndexInfo
		if oldTable != nil {
			for _, oldIndex := range oldTable.Schema.Indices {
				if oldIndex.Name == indexInfo.Name {
					oldIndexInfo = oldIndex
					oldTableID = oldTable.Schema.ID
					break
				}
			}
			if oldIndexInfo == nil {
				log.Panic("index not found",
					zap.Reflect("table", tableInfo),
					zap.Reflect("oldTable", oldTable.Schema),
					zap.Stringer("index", indexInfo.Name))
			}
		}
		req, err = buildIndexRequest(
			tableID, indexInfo, oldTableID, oldIndexInfo, startTS)
		if err != nil {
			return nil, err
		}
		reqs = append(reqs, req)
	}

	return reqs, nil
}

func buildTableRequest(
	tableID int64,
	oldTable *utils.Table,
	startTS uint64,
) (*kv.Request, error) {
	var rule *tipb.ChecksumRewriteRule
	if oldTable != nil {
		rule = &tipb.ChecksumRewriteRule{
			OldPrefix: tablecodec.GenTableRecordPrefix(oldTable.Schema.ID),
			NewPrefix: tablecodec.GenTableRecordPrefix(tableID),
		}
	}

	checksum := &tipb.ChecksumRequest{
		StartTs:   startTS,
		ScanOn:    tipb.ChecksumScanOn_Table,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
		Rule:      rule,
	}

	ranges := ranger.FullIntRange(false)

	var builder distsql.RequestBuilder
	return builder.SetTableRanges(tableID, ranges, nil).
		SetChecksumRequest(checksum).
		SetConcurrency(variable.DefDistSQLScanConcurrency).
		Build()
}

func buildIndexRequest(
	tableID int64,
	indexInfo *model.IndexInfo,
	oldTableID int64,
	oldIndexInfo *model.IndexInfo,
	startTS uint64,
) (*kv.Request, error) {
	var rule *tipb.ChecksumRewriteRule
	if oldIndexInfo != nil {
		rule = &tipb.ChecksumRewriteRule{
			OldPrefix: tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexInfo.ID),
			NewPrefix: tablecodec.EncodeTableIndexPrefix(tableID, indexInfo.ID),
		}
	}
	checksum := &tipb.ChecksumRequest{
		StartTs:   startTS,
		ScanOn:    tipb.ChecksumScanOn_Index,
		Algorithm: tipb.ChecksumAlgorithm_Crc64_Xor,
		Rule:      rule,
	}

	ranges := ranger.FullRange()

	var builder distsql.RequestBuilder
	return builder.SetIndexRanges(nil, tableID, indexInfo.ID, ranges).
		SetChecksumRequest(checksum).
		SetConcurrency(variable.DefDistSQLScanConcurrency).
		Build()
}

func sendChecksumRequest(
	ctx context.Context,
	client kv.Client,
	req *kv.Request,
) (resp *tipb.ChecksumResponse, err error) {
	res, err := distsql.Checksum(ctx, client, req, nil)
	if err != nil {
		return nil, err
	}
	res.Fetch(ctx)
	defer func() {
		if err1 := res.Close(); err1 != nil {
			err = err1
		}
	}()

	resp = &tipb.ChecksumResponse{}

	for {
		data, err := res.NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		checksum := &tipb.ChecksumResponse{}
		if err = checksum.Unmarshal(data); err != nil {
			return nil, err
		}
		updateChecksumResponse(resp, checksum)
	}

	return resp, nil
}

func updateChecksumResponse(resp, update *tipb.ChecksumResponse) {
	resp.Checksum ^= update.Checksum
	resp.TotalKvs += update.TotalKvs
	resp.TotalBytes += update.TotalBytes
}

// Executor is a checksum executor
type Executor struct {
	reqs []*kv.Request
}

// Len returns the total number of checksum requests
func (exec *Executor) Len() int {
	return len(exec.reqs)
}

// Execute executes a checksum executor
func (exec *Executor) Execute(
	ctx context.Context,
	client kv.Client,
	updateFn func(),
) (*tipb.ChecksumResponse, error) {
	checksumResp := &tipb.ChecksumResponse{}
	for _, req := range exec.reqs {
		resp, err := sendChecksumRequest(ctx, client, req)
		if err != nil {
			return nil, err
		}
		updateChecksumResponse(checksumResp, resp)
		updateFn()
	}
	return checksumResp, nil
}
