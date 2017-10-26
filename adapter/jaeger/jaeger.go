package jaeger

import (
	"context"
	"io"
	"time"

	"github.com/opentracing/opentracing-go"
	jaeger "github.com/uber/jaeger-client-go"

	"istio.io/mixer/adapter/jaeger/config"
	"istio.io/mixer/pkg/adapter"
	"istio.io/mixer/template/tracespan"
)

type (
	builder struct {
		// cfg *config.Params
	}

	handler struct {
		l      adapter.Logger
		tracer opentracing.Tracer
		closer io.Closer
	}

	client struct {
	}
)

// NewBuilder returns a builder implementing the logentry.HandlerBuilder interface.
func NewBuilder() tracespan.HandlerBuilder {
	return &builder{}
}

func (b *builder) SetTraceSpanTypes(types map[string]*tracespan.Type) {
}

func (b *builder) SetAdapterConfig(cfg adapter.Config) {
	// b.cfg = cfg.(*config.Params)
}

func (b *builder) Validate() *adapter.ConfigErrors {
	return nil
}

func (b *builder) Build(ctx context.Context, env adapter.Env) (adapter.Handler, error) {
	logger := env.Logger()
	// cfg := b.cfg

	// jaeger.NewRemoteReporter()

	tracer, closer := jaeger.NewTracer(
		"istio-mixer",
		jaeger.NewConstSampler(true),
		jaeger.NewLoggingReporter(jaeger.StdLogger),
		jaeger.TracerOptions.PoolSpans(true),
	)

	return &handler{l: logger, tracer: tracer, closer: closer}, nil
}

func (h *handler) HandleTraceSpan(ctx context.Context, spans []*tracespan.Instance) error {
	h.l.Warningf("handling trace spans: %d", len(spans))
	for _, s := range spans {
		tID, err := jaeger.TraceIDFromString(s.TraceId)
		if err != nil {
			h.l.Warningf("skipping span: could not parse trace id: %v", err)
			continue
		}

		sID, err := jaeger.SpanIDFromString(s.SpanId)
		if err != nil {
			h.l.Warningf("skipping span: could not parse trace id: %v", err)
			continue
		}

		psID, err := jaeger.SpanIDFromString(s.ParentSpanId)
		if err != nil {
			h.l.Warningf("skipping span: could not parse trace id: %v", err)
			continue
		}
		sctx := jaeger.NewSpanContext(tID, sID, psID, false, nil)

		span := h.tracer.StartSpan(
			s.SpanName,
			opentracing.StartTime(s.StartTime.In(time.UTC)),
			opentracing.Tags(s.Attributes),
			opentracing.ChildOf(sctx),
		)
		span.SetTag("status_code", s.StatusCode)
		span.SetTag("status_message", s.StatusMessage)
		span.FinishWithOptions(opentracing.FinishOptions{FinishTime: s.EndTime.In(time.UTC)})
	}
	return nil
}

func (h *handler) Close() error { return h.closer.Close() }

// GetInfo returns the Info associated with this adapter implementation.
func GetInfo() adapter.Info {
	return adapter.Info{
		Name:        "jaeger",
		Impl:        "istio.io/mixer/adapter/jaeger",
		Description: "Tracing adapter with configurable backend export",
		SupportedTemplates: []string{
			tracespan.TemplateName,
		},
		DefaultConfig: &config.Params{},
		NewBuilder:    func() adapter.HandlerBuilder { return &builder{} },
	}
}
