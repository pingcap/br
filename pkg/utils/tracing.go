// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"context"
	"fmt"

	"github.com/cheynewallace/tabby"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/util/logutil"
)

// TracerStartSpan starts the tracer for BR
func TracerStartSpan(ctx context.Context) (context.Context, *basictracer.InMemorySpanRecorder) {
	recorder := basictracer.NewInMemoryRecorder()
	tracer := basictracer.New(recorder)
	span := tracer.StartSpan("trace")
	return opentracing.ContextWithSpan(ctx, span), recorder
}

// TracerFinishSpan finishes the tracer for BR
func TracerFinishSpan(ctx context.Context, recorder *basictracer.InMemorySpanRecorder) {
	span := opentracing.SpanFromContext(ctx)
	if recorder == nil || span == nil {
		return
	}
	span.Finish()
	allSpans := recorder.GetSpans()
	tub := tabby.New()
	for rIdx := range allSpans {
		span := &allSpans[rIdx]

		tub.AddLine(span.Start, "--- start span "+span.Operation+" ----", "", span.Operation)

		var tags string
		if len(span.Tags) > 0 {
			tags = fmt.Sprintf("%v", span.Tags)
		}
		for _, l := range span.Logs {
			for _, field := range l.Fields {
				if field.Key() == logutil.TraceEventKey {
					tub.AddLine(l.Timestamp, field.Value().(string), tags, span.Operation)
				}
			}
		}
	}
	tub.Print()
}
