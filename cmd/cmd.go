package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/overvenus/br/backup"
)

var defaultBacker *backup.Backer
var defaultBackerMu = sync.Mutex{}

// SetDefaultBacker sets the default backer for command line usage.
func SetDefaultBacker(ctx context.Context, pdAddrs string) {
	defaultBackerMu.Lock()
	defer defaultBackerMu.Unlock()
	print(pdAddrs)
	var err error
	defaultBacker, err = backup.NewBacker(ctx, pdAddrs)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// GetDefaultBacker returns the default backer for command line usage.
func GetDefaultBacker() *backup.Backer {
	defaultBackerMu.Lock()
	defer defaultBackerMu.Unlock()
	return defaultBacker
}
