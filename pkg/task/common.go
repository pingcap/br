// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"context"
	"crypto/tls"
	"fmt"
	"regexp"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.etcd.io/etcd/pkg/transport"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/glue"
	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/utils"
)

const (
	// flagSendCreds specify whether to send credentials to tikv
	flagSendCreds = "send-credentials-to-tikv"
	// flagStorage is the name of storage flag.
	flagStorage = "storage"
	// flagPD is the name of PD url flag.
	flagPD = "pd"
	// flagCA is the name of TLS CA flag.
	flagCA = "ca"
	// flagCert is the name of TLS cert flag.
	flagCert = "cert"
	// flagKey is the name of TLS key flag.
	flagKey = "key"

	flagDatabase = "db"
	flagTable    = "table"

	flagRateLimit     = "ratelimit"
	flagRateLimitUnit = "ratelimit-unit"
	flagConcurrency   = "concurrency"
	flagChecksum      = "checksum"
)

// TLSConfig is the common configuration for TLS connection.
type TLSConfig struct {
	CA   string `json:"ca" toml:"ca"`
	Cert string `json:"cert" toml:"cert"`
	Key  string `json:"key" toml:"key"`
}

// IsEnabled checks if TLS open or not
func (tls *TLSConfig) IsEnabled() bool {
	return tls.CA != ""
}

// ToTLSConfig generate tls.Config
func (tls *TLSConfig) ToTLSConfig() (*tls.Config, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      tls.Cert,
		KeyFile:       tls.Key,
		TrustedCAFile: tls.CA,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tlsConfig, nil
}

// Config is the common configuration for all BRIE tasks.
type Config struct {
	storage.BackendOptions

	Storage     string    `json:"storage" toml:"storage"`
	PD          []string  `json:"pd" toml:"pd"`
	TLS         TLSConfig `json:"tls" toml:"tls"`
	RateLimit   uint64    `json:"rate-limit" toml:"rate-limit"`
	Concurrency uint32    `json:"concurrency" toml:"concurrency"`
	Checksum    bool      `json:"checksum" toml:"checksum"`
	SendCreds   bool      `json:"send-credentials-to-tikv" toml:"send-credentials-to-tikv"`
	// LogProgress is true means the progress bar is printed to the log instead of stdout.
	LogProgress bool `json:"log-progress" toml:"log-progress"`

	CaseSensitive bool         `json:"case-sensitive" toml:"case-sensitive"`
	Filter        filter.Rules `json:"black-white-list" toml:"black-white-list"`
}

// DefineCommonFlags defines the flags common to all BRIE commands.
func DefineCommonFlags(flags *pflag.FlagSet) {
	flags.BoolP(flagSendCreds, "c", true, "Whether send credentials to tikv")
	flags.StringP(flagStorage, "s", "", `specify the url where backup storage, eg, "local:///path/to/save"`)
	flags.StringSliceP(flagPD, "u", []string{"127.0.0.1:2379"}, "PD address")
	flags.String(flagCA, "", "CA certificate path for TLS connection")
	flags.String(flagCert, "", "Certificate path for TLS connection")
	flags.String(flagKey, "", "Private key path for TLS connection")

	flags.Uint64(flagRateLimit, 0, "The rate limit of the task, MB/s per node")
	flags.Bool(flagChecksum, true, "Run checksum at end of task")

	// Default concurrency is different for backup and restore.
	// Leave it 0 and let them adjust the value.
	flags.Uint32(flagConcurrency, 0, "The size of thread pool on each node that executes the task")
	// It may confuse users , so just hide it.
	_ = flags.MarkHidden(flagConcurrency)

	flags.Uint64(flagRateLimitUnit, utils.MB, "The unit of rate limit")
	_ = flags.MarkHidden(flagRateLimitUnit)

	storage.DefineFlags(flags)
}

// DefineDatabaseFlags defines the required --db flag.
func DefineDatabaseFlags(command *cobra.Command) {
	command.Flags().String(flagDatabase, "", "database name")
	_ = command.MarkFlagRequired(flagDatabase)
}

// DefineTableFlags defines the required --db and --table flags.
func DefineTableFlags(command *cobra.Command) {
	DefineDatabaseFlags(command)
	command.Flags().StringP(flagTable, "t", "", "table name")
	_ = command.MarkFlagRequired(flagTable)
}

// ParseFromFlags parses the TLS config from the flag set.
func (tls *TLSConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	tls.CA, err = flags.GetString(flagCA)
	if err != nil {
		return errors.Trace(err)
	}
	tls.Cert, err = flags.GetString(flagCert)
	if err != nil {
		return errors.Trace(err)
	}
	tls.Key, err = flags.GetString(flagKey)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// ParseFromFlags parses the config from the flag set.
func (cfg *Config) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Storage, err = flags.GetString(flagStorage)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.SendCreds, err = flags.GetBool(flagSendCreds)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.PD, err = flags.GetStringSlice(flagPD)
	if err != nil {
		return errors.Trace(err)
	}
	if len(cfg.PD) == 0 {
		return errors.New("must provide at least one PD server address")
	}
	cfg.Concurrency, err = flags.GetUint32(flagConcurrency)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.Checksum, err = flags.GetBool(flagChecksum)
	if err != nil {
		return errors.Trace(err)
	}

	var rateLimit, rateLimitUnit uint64
	rateLimit, err = flags.GetUint64(flagRateLimit)
	if err != nil {
		return errors.Trace(err)
	}
	rateLimitUnit, err = flags.GetUint64(flagRateLimitUnit)
	if err != nil {
		return errors.Trace(err)
	}
	cfg.RateLimit = rateLimit * rateLimitUnit

	if dbFlag := flags.Lookup(flagDatabase); dbFlag != nil {
		db := escapeFilterName(dbFlag.Value.String())
		if len(db) == 0 {
			return errors.New("empty database name is not allowed")
		}
		if tblFlag := flags.Lookup(flagTable); tblFlag != nil {
			tbl := escapeFilterName(tblFlag.Value.String())
			if len(tbl) == 0 {
				return errors.New("empty table name is not allowed")
			}
			cfg.Filter.DoTables = []*filter.Table{{Schema: db, Name: tbl}}
		} else {
			cfg.Filter.DoDBs = []string{db}
		}
	}

	if err := cfg.BackendOptions.ParseFromFlags(flags); err != nil {
		return err
	}
	return cfg.TLS.ParseFromFlags(flags)
}

// newMgr creates a new mgr at the given PD address.
func newMgr(ctx context.Context, g glue.Glue, pds []string, tlsConfig TLSConfig) (*conn.Mgr, error) {
	var (
		tlsConf *tls.Config
		err     error
	)
	pdAddress := strings.Join(pds, ",")
	if len(pdAddress) == 0 {
		return nil, errors.New("pd address can not be empty")
	}

	securityOption := pd.SecurityOption{}
	if tlsConfig.IsEnabled() {
		securityOption.CAPath = tlsConfig.CA
		securityOption.CertPath = tlsConfig.Cert
		securityOption.KeyPath = tlsConfig.Key
		tlsConf, err = tlsConfig.ToTLSConfig()
		if err != nil {
			return nil, err
		}
	}

	// Disable GC because TiDB enables GC already.
	store, err := g.Open(fmt.Sprintf("tikv://%s?disableGC=true", pdAddress), securityOption)
	if err != nil {
		return nil, err
	}
	return conn.NewMgr(ctx, g, pdAddress, store.(tikv.Storage), tlsConf, securityOption)
}

// GetStorage gets the storage backend from the config.
func GetStorage(
	ctx context.Context,
	cfg *Config,
) (*backup.StorageBackend, storage.ExternalStorage, error) {
	u, err := storage.ParseBackend(cfg.Storage, &cfg.BackendOptions)
	if err != nil {
		return nil, nil, err
	}
	s, err := storage.Create(ctx, u, cfg.SendCreds)
	if err != nil {
		return nil, nil, errors.Annotate(err, "create storage failed")
	}
	return u, s, nil
}

// ReadBackupMeta reads the backupmeta file from the storage.
func ReadBackupMeta(
	ctx context.Context,
	cfg *Config,
) (*backup.StorageBackend, storage.ExternalStorage, *backup.BackupMeta, error) {
	u, s, err := GetStorage(ctx, cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	metaData, err := s.Read(ctx, utils.MetaFile)
	if err != nil {
		return nil, nil, nil, errors.Annotate(err, "load backupmeta failed")
	}
	backupMeta := &backup.BackupMeta{}
	if err = proto.Unmarshal(metaData, backupMeta); err != nil {
		return nil, nil, nil, errors.Annotate(err, "parse backupmeta failed")
	}
	return u, s, backupMeta, nil
}

func escapeFilterName(name string) string {
	if !strings.HasPrefix(name, "~") {
		return name
	}
	return "~^" + regexp.QuoteMeta(name) + "$"
}
