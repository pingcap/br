package restore

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"google.golang.org/grpc"
)

func Restore(ctx context.Context, concurrency int, importerAddr string, meta *backup.BackupMeta, table *Table, pdAddr string) {
	fileCh := make(chan *backup.File)
	tableIds, indexIds := getIdPairsFromTable(table)

	var wg *sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		go func(fileCh chan *backup.File) {
			wg.Add(1)
			var conn *grpc.ClientConn
			conn, err := grpc.Dial(importerAddr, grpc.WithInsecure())
			if err != nil {
				panic(err)
			}
			client := import_kvpb.NewImportKVClient(conn)

			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case file := <-fileCh:
					req := &import_kvpb.ImportFileRequest{
						File:     file,
						Path:     meta.Path,
						PdAddr:   pdAddr,
						TableIds: tableIds,
						IndexIds: indexIds,
					}
					var resp *import_kvpb.ImportFileResponse
					resp, err = client.ImportFile(ctx, req)
					if err != nil || resp.Error != nil {
						panic(fmt.Errorf("import file failed, err: %v, resp: %v", err, resp))
					}
				}
			}
		}(fileCh)
	}

	wg.Wait()
}

func getIdPairsFromTable(table *Table) ([]*import_kvpb.IdPair, []*import_kvpb.IdPair) {
	tableIds := make([]*import_kvpb.IdPair, 0)
	tableIds = append(tableIds, &import_kvpb.IdPair{
		OldId: table.SrcSchema.ID,
		NewId: table.DestSchema.ID,
	})

	indexIds := make([]*import_kvpb.IdPair, 0)
	for _, src := range table.SrcSchema.Indices {
		for _, dest := range table.DestSchema.Indices {
			if src.Name == dest.Name {
				indexIds = append(indexIds, &import_kvpb.IdPair{
					OldId: src.ID,
					NewId: dest.ID,
				})
			}
		}
	}
	return tableIds, indexIds
}
