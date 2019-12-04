package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/utils"
)

// NewMetaCommand return a meta subcommand.
func NewMetaCommand() *cobra.Command {
	meta := &cobra.Command{
		Use:   "meta <subcommand>",
		Short: "show meta data of a cluster",
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return err
			}
			utils.LogBRInfo()
			utils.LogArguments(c)
			return nil
		},
	}
	checksumCmd := &cobra.Command{
		Use:   "checksum",
		Short: "check the backup data",
		RunE: func(cmd *cobra.Command, _ []string) error {
			u, err := cmd.Flags().GetString("storage")
			if err != nil {
				return errors.Trace(err)
			}
			if u == "" {
				return errors.New("empty backup store is not allowed")
			}
			storage, err := utils.CreateStorage(u)
			if err != nil {
				return errors.Trace(err)
			}
			metaData, err := storage.Read(utils.MetaFile)
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
					data, err = storage.Read(file.Name)
					if err != nil {
						return errors.Trace(err)
					}
					s := sha256.Sum256(data)
					hexBytes := make([]byte, hex.EncodedLen(len(s)))
					hex.Encode(hexBytes, s[:])
					if !bytes.Equal(hexBytes, file.Sha256) {
						return errors.Errorf(`
backup data checksum failed: %s may be changed
calculated sha256 is %s,
origin sha256 is %s`, file.Name, s, file.Sha256)
					}
					log.Info("table info", zap.Stringer("table", tblInfo.Name),
						zap.Uint64("CRC64", calCRC64),
						zap.Uint64("totalKvs", totalKVs),
						zap.Uint64("totalBytes", totalBytes),
						zap.Uint64("schemaTotalKvs", schema.TotalKvs),
						zap.Uint64("schemaTotalBytes", schema.TotalBytes),
						zap.Uint64("schemaCRC64", schema.Crc64Xor))
				}
			}
			cmd.Println("backup data checksum succeed!")
			return nil
		},
	}
	checksumCmd.Hidden = true
	meta.AddCommand(checksumCmd)

	decodeBackupMetaCmd := &cobra.Command{
		Use:   "decode",
		Short: "decode backupmeta to json",
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := cmd.Flags().GetString("storage")
			if err != nil {
				return errors.Trace(err)
			}
			if u == "" {
				return errors.New("empty backup store is not allowed")
			}
			storage, err := utils.CreateStorage(u)
			if err != nil {
				return errors.Trace(err)
			}
			metaData, err := storage.Read(utils.MetaFile)
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

			err = storage.Write(utils.MetaJSONFile, backupMetaJSON)
			if err != nil {
				return errors.Trace(err)
			}

			return nil
		},
	}
	meta.AddCommand(decodeBackupMetaCmd)

	loadBackupMetaCmd := &cobra.Command{
		Use:   "load",
		Short: "load backupmeta json file to backupmeta",
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := cmd.Flags().GetString("storage")
			if err != nil {
				return errors.Trace(err)
			}
			if u == "" {
				return errors.New("empty backup store is not allowed")
			}
			storage, err := utils.CreateStorage(u)
			if err != nil {
				return errors.Trace(err)
			}
			metaData, err := storage.Read(utils.MetaJSONFile)
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
			if storage.FileExists(fileName) {
				// Do not overwrite origin meta file
				fileName += "_from_json"
			}
			err = storage.Write(fileName, backupMeta)
			if err != nil {
				return errors.Trace(err)
			}

			return nil
		},
	}
	meta.AddCommand(loadBackupMetaCmd)

	return meta
}
