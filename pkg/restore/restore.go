package restore

import (
	"context"
	"fmt"
	"github.com/overvenus/br/pkg/meta"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"go.uber.org/zap"
	"strings"
	"sync"

	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_kvpb"
	"google.golang.org/grpc"
)

// FilePair wraps a default cf file & a write cf file
type FilePair struct {
	Default *backup.File
	Write   *backup.File
}

// Restore starts a restore task
func Restore(concurrency int, importerAddr string, backupMeta *backup.BackupMeta, table *Table, pdAddrs string) {
	ctx, cancel := context.WithCancel(context.Background())
	fileCh := make(chan *FilePair)
	respCh := make(chan *import_kvpb.RestoreFileResponse)

	tableIds, indexIds := getIDPairsFromTable(table)
	log.Info("get table ids", zap.Reflect("table_id", tableIds), zap.Reflect("index_id", indexIds))

	addrs := strings.Split(pdAddrs, ",")
	pdClient, err := pd.NewClient(addrs, pd.SecurityOption{})
	if err != nil {
		panic(errors.Trace(err))
	}
	p, l, err := pdClient.GetTS(ctx)
	if err != nil {
		panic(errors.Trace(err))
	}
	ts := meta.Timestamp{
		Physical: p,
		Logical:  l,
	}
	restoreTS := meta.EncodeTs(ts)

	err = switchClusterMode(ctx, importerAddr, addrs[0], import_sstpb.SwitchMode_Import)
	if err != nil {
		panic(fmt.Sprintf("switch mode err: %v", errors.Trace(err)))
	}
	log.Info("switch to import mode")

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(fileCh chan *FilePair, respCh chan *import_kvpb.RestoreFileResponse) {
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
				case pair := <-fileCh:
					req := &import_kvpb.RestoreFileRequest{
						Default:   pair.Default,
						Write:     pair.Write,
						Path:      backupMeta.Path,
						PdAddr:    addrs[0],
						TableIds:  tableIds,
						IndexIds:  indexIds,
						RestoreTs: restoreTS,
					}
					resp, err := client.RestoreFile(ctx, req)
					if err != nil || resp.Error != nil {
						panic(fmt.Errorf("restore file failed, err: %v, resp: %v", err, resp))
					}
					log.Info("restore file", zap.Reflect("file", pair), zap.Uint64("restore_ts", restoreTS))
					respCh <- resp
				}
			}
		}(fileCh, respCh)
	}

	filePairs := make([]*FilePair, 0)
	for _, file := range backupMeta.Files {
		if strings.HasSuffix(file.Name, "write") {
			var defaultFile *backup.File
			defaultName := strings.TrimSuffix(file.Name, "write") + "default"
			for _, f := range backupMeta.Files {
				if f.Name == defaultName {
					defaultFile = f
				}
			}
			filePairs = append(filePairs, &FilePair{
				Default: defaultFile,
				Write:   file,
			})
		}
	}

	go func() {
		for _, p := range filePairs {
			fileCh <- p
		}
	}()

	go func() {
		for i := 0; i < len(filePairs); i++ {
			_ = <-respCh
		}

		err = switchClusterMode(ctx, importerAddr, addrs[0], import_sstpb.SwitchMode_Normal)
		if err != nil {
			panic(fmt.Sprintf("switch mode err: %v", errors.Trace(err)))
		}
		log.Info("switch to normal mode")

		cancel()
	}()

	wg.Wait()
}

func getIDPairsFromTable(table *Table) ([]*import_kvpb.IdPair, []*import_kvpb.IdPair) {
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

func switchClusterMode(ctx context.Context, importerAddr string, pdAddr string, mode import_sstpb.SwitchMode) error {
	conn, err := grpc.Dial(importerAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	client := import_kvpb.NewImportKVClient(conn)
	req := &import_kvpb.SwitchModeRequest{
		PdAddr: pdAddr,
		Request: &import_sstpb.SwitchModeRequest{
			Mode: mode,
		},
	}
	_, err = client.SwitchMode(ctx, req)
	return err
}
