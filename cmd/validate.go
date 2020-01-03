package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/pd/pkg/mock/mockid"
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/utils"
)

// NewValidateCommand return a debug subcommand.
func NewValidateCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:   "validate <subcommand>",
		Short: "commands to check/debug backup data",
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return err
			}
			utils.LogBRInfo()
			utils.LogArguments(c)
			return nil
		},
	}
	meta.AddCommand(newCheckSumCommand())
	meta.AddCommand(newBackupMetaCommand())
	meta.AddCommand(decodeBackupMetaCommand())
	meta.AddCommand(encodeBackupMetaCommand())
	meta.Hidden = true

	return meta
}

func newCheckSumCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "checksum",
		Short: "check the backup data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()

			u, err := storage.ParseBackendFromFlags(cmd.Flags(), FlagStorage)
			if err != nil {
				return err
			}
			s, err := storage.Create(ctx, u)
			if err != nil {
				return errors.Trace(err)
			}

			metaData, err := s.Read(ctx, utils.MetaFile)
			if err != nil {
				return errors.Trace(err)
			}

			backupMeta := &backup.BackupMeta{}
			err = proto.Unmarshal(metaData, backupMeta)
			if err != nil {
				return errors.Trace(err)
			}

			dbs, err := utils.LoadBackupTables(backupMeta)
			if err != nil {
				return errors.Trace(err)
			}

			for _, schema := range backupMeta.Schemas {
				dbInfo := &model.DBInfo{}
				err = json.Unmarshal(schema.Db, dbInfo)
				if err != nil {
					return errors.Trace(err)
				}
				tblInfo := &model.TableInfo{}
				err = json.Unmarshal(schema.Table, tblInfo)
				if err != nil {
					return errors.Trace(err)
				}
				tbl := dbs[dbInfo.Name.String()].GetTable(tblInfo.Name.String())

				var calCRC64 uint64
				var totalKVs uint64
				var totalBytes uint64
				for _, file := range tbl.Files {
					calCRC64 ^= file.Crc64Xor
					totalKVs += file.GetTotalKvs()
					totalBytes += file.GetTotalBytes()
					log.Info("file info", zap.Stringer("table", tblInfo.Name),
						zap.String("file", file.GetName()),
						zap.Uint64("crc64xor", file.GetCrc64Xor()),
						zap.Uint64("totalKvs", file.GetTotalKvs()),
						zap.Uint64("totalBytes", file.GetTotalBytes()),
						zap.Uint64("startVersion", file.GetStartVersion()),
						zap.Uint64("endVersion", file.GetEndVersion()),
						zap.Binary("startKey", file.GetStartKey()),
						zap.Binary("endKey", file.GetEndKey()),
					)

					var data []byte
					data, err = s.Read(ctx, file.Name)
					if err != nil {
						return errors.Trace(err)
					}
					s := sha256.Sum256(data)
					if !bytes.Equal(s[:], file.Sha256) {
						return errors.Errorf(`
backup data checksum failed: %s may be changed
calculated sha256 is %s,
origin sha256 is %s`,
							file.Name, hex.EncodeToString(s[:]), hex.EncodeToString(file.Sha256))
					}
				}
				log.Info("table info", zap.Stringer("table", tblInfo.Name),
					zap.Uint64("CRC64", calCRC64),
					zap.Uint64("totalKvs", totalKVs),
					zap.Uint64("totalBytes", totalBytes),
					zap.Uint64("schemaTotalKvs", schema.TotalKvs),
					zap.Uint64("schemaTotalBytes", schema.TotalBytes),
					zap.Uint64("schemaCRC64", schema.Crc64Xor))
			}
			cmd.Println("backup data checksum succeed!")
			return nil
		},
	}
	command.Hidden = true
	return command
}

func newBackupMetaCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "backupmeta",
		Short: "check the backup meta",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()

			tableIDOffset, err := cmd.Flags().GetUint64("offset")
			if err != nil {
				return err
			}
			u, err := storage.ParseBackendFromFlags(cmd.Flags(), FlagStorage)
			if err != nil {
				return err
			}
			s, err := storage.Create(ctx, u)
			if err != nil {
				log.Error("create storage failed", zap.Error(err))
				return errors.Trace(err)
			}
			data, err := s.Read(ctx, utils.MetaFile)
			if err != nil {
				log.Error("load backupmeta failed", zap.Error(err))
				return err
			}
			backupMeta := &backup.BackupMeta{}
			err = proto.Unmarshal(data, backupMeta)
			if err != nil {
				log.Error("parse backupmeta failed", zap.Error(err))
				return err
			}
			dbs, err := utils.LoadBackupTables(backupMeta)
			if err != nil {
				log.Error("load tables failed", zap.Error(err))
				return err
			}
			files := make([]*backup.File, 0)
			tables := make([]*utils.Table, 0)
			for _, db := range dbs {
				for _, table := range db.Tables {
					files = append(files, table.Files...)
				}
				tables = append(tables, db.Tables...)
			}
			// Check if the ranges of files overlapped
			rangeTree := restore_util.NewRangeTree()
			for _, file := range files {
				if out := rangeTree.InsertRange(restore_util.Range{
					StartKey: file.GetStartKey(),
					EndKey:   file.GetEndKey(),
				}); out != nil {
					log.Error(
						"file ranges overlapped",
						zap.Stringer("out", out.(*restore_util.Range)),
						zap.Stringer("file", file),
					)
				}
			}

			sort.Sort(utils.Tables(tables))
			tableIDAllocator := mockid.NewIDAllocator()
			// Advance table ID allocator to the offset.
			for offset := uint64(0); offset < tableIDOffset; offset++ {
				_, _ = tableIDAllocator.Alloc() // Ignore error
			}
			rewriteRules := &restore_util.RewriteRules{
				Table: make([]*import_sstpb.RewriteRule, 0),
				Data:  make([]*import_sstpb.RewriteRule, 0),
			}
			tableIDMap := make(map[int64]int64)
			// Simulate to create table
			for _, table := range tables {
				indexIDAllocator := mockid.NewIDAllocator()
				newTable := new(model.TableInfo)
				tableID, _ := tableIDAllocator.Alloc()
				newTable.ID = int64(tableID)
				newTable.Name = table.Schema.Name
				newTable.Indices = make([]*model.IndexInfo, len(table.Schema.Indices))
				for i, indexInfo := range table.Schema.Indices {
					indexID, _ := indexIDAllocator.Alloc()
					newTable.Indices[i] = &model.IndexInfo{
						ID:   int64(indexID),
						Name: indexInfo.Name,
					}
				}
				// TODO: support table partition
				rules := restore.GetRewriteRules(newTable, table.Schema, 0)
				rewriteRules.Table = append(rewriteRules.Table, rules.Table...)
				rewriteRules.Data = append(rewriteRules.Data, rules.Data...)
				tableIDMap[table.Schema.ID] = int64(tableID)
			}
			for oldID, newID := range tableIDMap {
				if _, ok := tableIDMap[oldID+1]; !ok {
					rewriteRules.Table = append(rewriteRules.Table, &import_sstpb.RewriteRule{
						OldKeyPrefix: tablecodec.EncodeTablePrefix(oldID + 1),
						NewKeyPrefix: tablecodec.EncodeTablePrefix(newID + 1),
					})
				}
			}
			// Validate rewrite rules
			for _, file := range files {
				err = restore.ValidateFileRewriteRule(file, rewriteRules)
				if err != nil {
					return err
				}
			}
			cmd.Println("Check backupmeta done")
			return nil
		},
	}
	command.Flags().String("path", "", "the path of backupmeta")
	command.Flags().Uint64P("offset", "", 0, "the offset of table id alloctor")
	command.Hidden = true
	return command
}

func decodeBackupMetaCommand() *cobra.Command {
	decodeBackupMetaCmd := &cobra.Command{
		Use:   "decode",
		Short: "decode backupmeta to json",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()
			u, err := storage.ParseBackendFromFlags(cmd.Flags(), FlagStorage)
			if err != nil {
				return errors.Trace(err)
			}
			s, err := storage.Create(ctx, u)
			if err != nil {
				return errors.Trace(err)
			}
			metaData, err := s.Read(ctx, utils.MetaFile)
			if err != nil {
				return errors.Trace(err)
			}

			backupMeta := &backup.BackupMeta{}
			err = proto.Unmarshal(metaData, backupMeta)
			if err != nil {
				return errors.Trace(err)
			}
			backupMetaJSON, err := json.Marshal(backupMeta)
			if err != nil {
				return errors.Trace(err)
			}

			err = s.Write(ctx, utils.MetaJSONFile, backupMetaJSON)
			if err != nil {
				return errors.Trace(err)
			}

			field, err := cmd.Flags().GetString("field")
			if err != nil {
				log.Error("get field flag failed", zap.Error(err))
				return err
			}
			switch field {
			case "start-version":
				fmt.Println(backupMeta.StartVersion)
			case "end-version":
				fmt.Println(backupMeta.EndVersion)
			}
			return nil
		},
	}

	decodeBackupMetaCmd.Flags().String("field", "", "decode specified field")

	return decodeBackupMetaCmd
}

func encodeBackupMetaCommand() *cobra.Command {
	encodeBackupMetaCmd := &cobra.Command{
		Use:   "encode",
		Short: "encode backupmeta json file to backupmeta",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithCancel(GetDefaultContext())
			defer cancel()
			u, err := storage.ParseBackendFromFlags(cmd.Flags(), FlagStorage)
			if err != nil {
				return errors.Trace(err)
			}
			s, err := storage.Create(ctx, u)
			if err != nil {
				return errors.Trace(err)
			}
			metaData, err := s.Read(ctx, utils.MetaJSONFile)
			if err != nil {
				return errors.Trace(err)
			}

			backupMetaJSON := &backup.BackupMeta{}
			err = json.Unmarshal(metaData, backupMetaJSON)
			if err != nil {
				return errors.Trace(err)
			}
			backupMeta, err := proto.Marshal(backupMetaJSON)
			if err != nil {
				return errors.Trace(err)
			}

			fileName := utils.MetaFile
			if ok, _ := s.FileExists(ctx, fileName); ok {
				// Do not overwrite origin meta file
				fileName += "_from_json"
			}
			err = s.Write(ctx, fileName, backupMeta)
			if err != nil {
				return errors.Trace(err)
			}
			return nil
		},
	}
	return encodeBackupMetaCmd
}
