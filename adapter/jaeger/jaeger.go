package jaeger

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	jaeger "github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-client-go/transport"
	zipkin "github.com/uber/jaeger-client-go/transport/zipkin"

	"istio.io/mixer/adapter/jaeger/config"
	"istio.io/mixer/pkg/adapter"
	"istio.io/mixer/template/tracespan"
)

type (
	builder struct {
		cfg *config.Params
	}

	handler struct {
		l        adapter.Logger
		zipkinAddr string
		jaegerAddr string

		// TODO: make these an LRU or similar
		tracers map[string]opentracing.Tracer
		closers map[string]io.Closer
	}

	client struct {
	}
)

var (
	sampler = jaeger.NewConstSampler(true)
)

// NewBuilder returns a builder implementing the logentry.HandlerBuilder interface.
func NewBuilder() tracespan.HandlerBuilder {
	return &builder{}
}

func (b *builder) SetTraceSpanTypes(types map[string]*tracespan.Type) {
}

func (b *builder) SetAdapterConfig(cfg adapter.Config) {
	b.cfg = cfg.(*config.Params)
}

func (b *builder) Validate() *adapter.ConfigErrors {
	return nil
}

func (b *builder) Build(ctx context.Context, env adapter.Env) (adapter.Handler, error) {
	logger := env.Logger()

	return &handler{
		l: logger,
		zipkinAddr:b.cfg.ZipkinAddress,
		jaegerAddr:b.cfg.JaegerAddress,
		tracers: make(map[string]opentracing.Tracer),
		closers: make(map[string]io.Closer),
	}, nil
}

func (h *handler) HandleTraceSpan(ctx context.Context, spans []*tracespan.Instance) error {

	var closer io.Closer
	h.l.Warningf("handling trace spans: %d", len(spans))
	for _, s := range spans {
		tracer, found := h.tracers[s.ServiceName]
		if !found {
			h.l.Warningf("new tracer for service: %s", s.ServiceName)

			reporter, err := h.compositeReporter()
			if err != nil {
				return fmt.Errorf("failed to create separate reporter: %v", err)
			}

			tracer, closer = jaeger.NewTracer(s.ServiceName, sampler, reporter, jaeger.TracerOptions.PoolSpans(true))
			h.tracers[s.ServiceName] = tracer
			h.closers[s.ServiceName] = closer
		}

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

		psID := jaeger.SpanID(0)
		if len(strings.TrimSpace(s.ParentSpanId)) > 0 {
			if psID, err = jaeger.SpanIDFromString(s.ParentSpanId); err != nil {
				h.l.Warningf("skipping span: could not parse parent span id: %v", err)
				continue
			}
		}
		sctx := jaeger.NewSpanContext(tID, sID, psID, true, nil)

		span := tracer.StartSpan(
			s.SpanName,
			opentracing.StartTime(s.StartTime.In(time.UTC)),
			opentracing.Tags(s.Attributes),
			opentracing.ChildOf(sctx),
		)
		span.SetTag("status_code", s.StatusCode)
		span.SetTag("status_message", s.StatusMessage)
		span.FinishWithOptions(opentracing.FinishOptions{FinishTime: s.EndTime.In(time.UTC)})

		h.l.Warningf("handled trace span: %v", span)
	}
	return nil
}

func (h *handler) Close() error {
	var err error
	for _, closer := range h.closers {
		err = closer.Close()
	}
	return err
}

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

func (h handler) compositeReporter() (jaeger.Reporter, error) {
	ztrans, err := zipkin.NewHTTPTransport(h.zipkinAddr, zipkin.HTTPBatchSize(10), zipkin.HTTPLogger(log.StdLogger))

	if err != nil {
		return nil, err
	}

	zrep := jaeger.NewRemoteReporter(ztrans)

	jtrans := transport.NewHTTPTransport(h.jaegerAddr, transport.HTTPBatchSize(10))

	jrep := jaeger.NewRemoteReporter(jtrans)

	return jaeger.NewCompositeReporter(jaeger.NewLoggingReporter(log.StdLogger), zrep, jrep), nil
}
