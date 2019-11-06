package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/spf13/cobra"
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
			return nil
		},
	}
	meta.AddCommand(&cobra.Command{
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

			for _, file := range backupMeta.Files {
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
			}

			cmd.Println("backup data checksum succeed!")
			return nil
		},
	})
	return meta
}
