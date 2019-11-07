package utils

import (
	"context"
	"time"

	"github.com/cheggaaa/pb/v3"
)

// ProgressPrinter prints a progress bar
type ProgressPrinter struct {
	name  string
	total int64

	updateCh chan struct{}
}

// NewProgressPrinter returns a new progress printer
func NewProgressPrinter(name string, total int64) *ProgressPrinter {
	return &ProgressPrinter{
		name:     name,
		total:    total,
		updateCh: make(chan struct{}, total/2),
	}
}

// UpdateCh returns an update channel
func (pp *ProgressPrinter) UpdateCh() chan<- struct{} {
	return pp.updateCh
}

// GoPrintProgress starts a gorouinte and prints progress
func (pp *ProgressPrinter) GoPrintProgress(ctx context.Context) {
	tmpl := `{{string . "barName" | red}} {{ bar . "<" "-" (cycle . "↖" "↗" "↘" "↙" ) "." ">"}} {{percent .}}`
	bar := pb.ProgressBarTemplate(tmpl).Start64(pp.total)
	bar.Set("barName", pp.name)
	bar.Start()

	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()

		var counter int64
		for {
			select {
			case <-ctx.Done():
				bar.SetCurrent(pp.total)
				bar.Finish()
				return
			case <-pp.updateCh:
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
