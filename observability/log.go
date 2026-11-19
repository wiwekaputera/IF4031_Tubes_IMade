package observability

import (
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
)

// LogHook adds trace_id and span_id to zerolog events logged with a context:
//
//	logger := zerolog.New(os.Stdout).Hook(observability.LogHook{})
//	logger.Info().Ctx(r.Context()).Msg("request processed")
//
// The trace ID in a log line is the same one Jaeger shows, so a slow trace
// leads straight to its logs and back.
type LogHook struct{}

// Run implements zerolog.Hook.
func (LogHook) Run(e *zerolog.Event, _ zerolog.Level, _ string) {
	ctx := e.GetCtx()
	if ctx == nil {
		return
	}
	sc := trace.SpanContextFromContext(ctx)
	if sc.HasTraceID() {
		e.Str("trace_id", sc.TraceID().String())
	}
	if sc.HasSpanID() {
		e.Str("span_id", sc.SpanID().String())
	}
}
