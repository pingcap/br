package task

import (
	"bytes"
	"context"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
)

// RestoreRawConfig is the configuration specific for raw kv restore tasks.
type RestoreRawConfig struct {
	Config

	StartKey []byte
	EndKey   []byte
	CF       string
	Online   bool //`json:"online" toml:"online"`
}

// DefineRawRestoreFlags defines common flags for the backup command.
func DefineRawRestoreFlags(command *cobra.Command) {
	command.Flags().StringP("format", "", "hex", "start/end key format, support raw|escaped|hex")
	command.Flags().StringP("cf", "", "default", "backup specify cf, correspond to tikv cf")
	command.Flags().StringP("start", "", "", "backup raw kv start key, key is inclusive")
	command.Flags().StringP("end", "", "", "backup raw kv end key, key is exclusive")
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *RestoreRawConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	// TODO: Wait for merging #101
	//start, err := flags.GetString("start")
	//if err != nil {
	//	return err
	//}
	//cfg.StartKey, err = utils.ParseKey(flags, start)
	//if err != nil {
	//	return err
	//}
	//end, err := flags.GetString("end")
	//if err != nil {
	//	return err
	//}
	//cfg.EndKey, err = utils.ParseKey(flags, end)
	//if err != nil {
	//	return err
	//}

	if bytes.Compare(cfg.StartKey, cfg.EndKey) > 0 {
		return errors.New("input endKey must greater or equal than startKey")
	}

	var err error
	cfg.CF, err = flags.GetString("cf")
	if err != nil {
		return err
	}
	if err = cfg.Config.ParseFromFlags(flags); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunRestoreRaw starts a raw kv restore task inside the current goroutine.
func RunRestoreRaw(c context.Context, cmdName string, cfg *RestoreRawConfig) error {
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, cfg.PD)
	if err != nil {
		return err
	}
	defer mgr.Close()

	client, err := restore.NewRestoreClient(ctx, mgr.GetPDClient(), mgr.GetTiKV())
	if err != nil {
		return err
	}
	defer client.Close()

	if !client.IsRawKvMode() {
		return errors.New("cannot do raw restore from transactional data")
	}

	client.SetRateLimit(cfg.RateLimit)
	client.SetConcurrency(uint(cfg.Concurrency))
	if cfg.Online {
		client.EnableOnline()
	}

	defer summary.Summary(cmdName)

	files, err := client.GetFilesInRawRange(cfg.StartKey, cfg.EndKey, cfg.CF)
	if err != nil {
		return errors.Trace(err)
	}

	if len(files) == 0 {
		return errors.New("all files are filtered out from the backup archive, nothing to restore")
	}
	summary.CollectInt("restore files", len(files))

	// Empty rewrite rules
	rewriteRules := &restore.RewriteRules{}

	ranges, err := restore.ValidateFileRanges(files, rewriteRules)
	if err != nil {
		return errors.Trace(err)
	}

	// Redirect to log if there is no log file to avoid unreadable output.
	// TODO: How to show progress?
	updateCh := utils.StartProgress(
		ctx,
		"Table Restore",
		// Split/Scatter + Download/Ingest
		int64(len(ranges)+len(files)),
		!cfg.LogProgress)

	err = restore.SplitRanges(ctx, client, ranges, rewriteRules, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	removedSchedulers, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return errors.Trace(err)
	}

	err = client.RestoreRaw(cfg.StartKey, cfg.EndKey, files, updateCh)
	if err != nil {
		return errors.Trace(err)
	}

	err = restorePostWork(ctx, client, mgr, removedSchedulers)
	if err != nil {
		return errors.Trace(err)
	}
	// Restore has finished.
	close(updateCh)

	return nil
}
