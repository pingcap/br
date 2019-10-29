package cmd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

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
	}
	meta.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "show cluster version",
		RunE: func(cmd *cobra.Command, _ []string) error {
			backer := GetDefaultBacker()
			v, err := backer.GetClusterVersion()
			if err != nil {
				fmt.Println(errors.ErrorStack(err))
				return err
			}
			cmd.Println(v)
			return nil
		},
	})
	meta.AddCommand(&cobra.Command{
		Use:   "safepoint",
		Short: "show the current GC safepoint of cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			backer := GetDefaultBacker()
			sp, err := backer.GetGCSafePoint()
			if err != nil {
				fmt.Println(errors.ErrorStack(err))
				return err
			}
			cmd.Printf("Timestamp { Physical: %d, Logical: %d }\n",
				sp.Physical, sp.Logical)
			return nil
		},
	})
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
					return errors.Errorf("backup data checksum failed: %s may be changed\n calculated sha256 is %s\n, origin sha256 is %s", file.Name, s, file.Sha256)
				}
			}

			cmd.Println("backup data checksum succeed!")
			return nil
		},
	})
	return meta
}
