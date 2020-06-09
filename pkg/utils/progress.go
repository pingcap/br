// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// ProgressPrinter prints a progress bar.
type ProgressPrinter struct {
	name        string
	total       int64
	redirectLog bool

	updateCh chan struct{}
}

// NewProgressPrinter returns a new progress printer.
func NewProgressPrinter(
	name string,
	total int64,
	redirectLog bool,
) *ProgressPrinter {
	return &ProgressPrinter{
		name:        name,
		total:       total,
		redirectLog: redirectLog,
		updateCh:    make(chan struct{}, total/2),
	}
}

// UpdateCh returns an update channel.
func (pp *ProgressPrinter) UpdateCh() chan<- struct{} {
	return pp.updateCh
}

// goPrintProgress starts a gorouinte and prints progress.
func (pp *ProgressPrinter) goPrintProgress(
	ctx context.Context,
	testWriter io.Writer, // Only for tests
) {
	bar := pb.New64(pp.total)
	if pp.redirectLog || testWriter != nil {
		tmpl := `{"P":"{{percent .}}","C":"{{counters . }}","E":"{{etime .}}","R":"{{rtime .}}","S":"{{speed .}}"}`
		bar.SetTemplateString(tmpl)
		bar.SetRefreshRate(2 * time.Minute)
		bar.Set(pb.Static, false)       // Do not update automatically
		bar.Set(pb.ReturnSymbol, false) // Do not append '\r'
		bar.Set(pb.Terminal, false)     // Do not use terminal width
		// Hack! set Color to avoid separate progress string
		bar.Set(pb.Color, true)
		bar.SetWriter(&wrappedWriter{name: pp.name})
	} else {
		tmpl := `{{string . "barName" | green}} {{ bar . "<" "-" (cycle . "-" "\\" "|" "/" ) "." ">"}} {{percent .}}`
		bar.SetTemplateString(tmpl)
		bar.Set("barName", pp.name)
	}
	if testWriter != nil {
		bar.SetWriter(testWriter)
		bar.SetRefreshRate(10 * time.Millisecond)
	}
	bar.Start()

	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		defer bar.Finish()

		var counter int64
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-pp.updateCh:
				if !ok {
					bar.SetCurrent(pp.total)
					return
				}
				counter++
			case <-t.C:
			}

			if counter <= pp.total {
				bar.SetCurrent(counter)
			} else {
				bar.SetCurrent(pp.total)
			}
		}
	}()
}

type wrappedWriter struct {
	name string
}

func (ww *wrappedWriter) Write(p []byte) (int, error) {
	var info struct {
		P string
		C string
		E string
		R string
		S string
	}
	if err := json.Unmarshal(p, &info); err != nil {
		return 0, err
	}
	log.Info("progress",
		zap.String("step", ww.name),
		zap.String("progress", info.P),
		zap.String("count", info.C),
		zap.String("speed", info.S),
		zap.String("elapsed", info.E),
		zap.String("remaining", info.R))
	return len(p), nil
}

// StartProgress starts progress bar.
func StartProgress(
	ctx context.Context,
	name string,
	total int64,
	redirectLog bool,
) chan<- struct{} {
	progress := NewProgressPrinter(name, total, redirectLog)
	progress.goPrintProgress(ctx, nil)
	return progress.UpdateCh()
}
