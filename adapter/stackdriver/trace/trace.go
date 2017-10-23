package trace

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	api "google.golang.org/api/cloudtrace/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/support/bundler"
	"google.golang.org/api/transport"

	"istio.io/mixer/adapter/stackdriver/config"
	"istio.io/mixer/pkg/adapter"
	"istio.io/mixer/template/tracespan"
)

type (
	builder struct {
		cfg *config.Params
	}

	handler struct {
		l      adapter.Logger
		tracer *client
	}

	client struct {
		service   *api.Service
		projectID string
		bundler   *bundler.Bundler
	}
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
	cfg := b.cfg

	c, err := newClient(ctx, cfg.ProjectId)
	if err != nil {
		return nil, fmt.Errorf("could not create Stackdriver trace client: %v", err)
	}

	return &handler{l: logger, tracer: c}, nil
}

func (h *handler) HandleTraceSpan(ctx context.Context, spans []*tracespan.Instance) error {

	h.l.Warningf("handling trace spans: %d", len(spans))

	for _, s := range spans {
		tID, err := strconv.ParseInt(s.TraceId, 16, 64)
		if err != nil {
			h.l.Warningf("skipping span: could not parse trace id: %v", err)
			continue
		}

		// TraceId: This identifier is a 128-bit numeric value formatted as a 32-byte hex string.
		traceID := fmt.Sprintf("%032x", tID)

		var psID uint64
		if len(strings.TrimSpace(s.ParentSpanId)) > 0 {
			psID, err = strconv.ParseUint(s.ParentSpanId, 16, 64)
			if err != nil {
				h.l.Warningf("skipping span: could not parse parent span id")
				continue
			}
		}

		sID := nextSpanID()

		startTime := s.StartTime
		start := startTime.In(time.UTC).Format(time.RFC3339Nano)

		endTime := s.EndTime
		end := endTime.In(time.UTC).Format(time.RFC3339Nano)

		lbls := map[string]string{}
		for k, v := range s.Attributes {
			if strings.HasSuffix(k, "method") {
				lbls[labelMethod] = toString(v)
				continue
			}
			if strings.HasSuffix(k, "url") {
				lbls[labelURL] = toString(v)
				continue
			}
			//if strings.HasSuffix(k, "url") {
			//	lbls[labelURL] = toString(v)
			//	continue
			//}
			if v != nil {
				lbls[k] = toString(v)
			}
		}

		if s.StatusCode != 0 {
			lbls[labelStatusCode] = strconv.FormatInt(s.StatusCode, 10)
		}

		if len(s.StatusMessage) > 0 {
			lbls[labelErrorMessage] = s.StatusMessage
		}

		span := &api.TraceSpan{
			EndTime:      end,
			StartTime:    start,
			Kind:         spanKindServer,
			Name:         s.SpanName,
			ParentSpanId: psID,
			SpanId:       sID, // must be non-0 and unique
			Labels:       lbls,
		}

		h.l.Warningf("trace span: %v", span)

		trace := &api.Trace{
			ProjectId: h.tracer.projectID,
			Spans:     []*api.TraceSpan{span},
			TraceId:   traceID,
		}
		err = h.tracer.bundler.Add(trace, 2)
		if err == bundler.ErrOversizedItem {
			err = h.tracer.upload([]*api.Trace{trace})
			log.Println("uploaded")
		}
		if err != nil {
			log.Println("error uploading trace:", err)
		}
	}
	return nil
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch l := v.(type) {
	case string:
		return l
	default:
		return fmt.Sprintf("%v", l)
	}
}

func (h *handler) Close() error { return nil }

const (
	httpHeader          = `X-Cloud-Trace-Context`
	userAgent           = `istio-mixer-trace-adapter` //`gcloud-golang-trace/20160501`
	cloudPlatformScope  = `https://www.googleapis.com/auth/cloud-platform`
	spanKindClient      = `RPC_CLIENT`
	spanKindServer      = `RPC_SERVER`
	spanKindUnspecified = `SPAN_KIND_UNSPECIFIED`
	labelHost           = `trace.cloud.google.com/http/host`
	labelMethod         = `trace.cloud.google.com/http/method`
	labelStackTrace     = `trace.cloud.google.com/stacktrace`
	labelStatusCode     = `trace.cloud.google.com/http/status_code`
	labelErrorMessage   = `trace.cloud.google.com/error/message`
	labelURL            = `trace.cloud.google.com/http/url`
	labelSamplingPolicy = `trace.cloud.google.com/sampling_policy`
	labelSamplingWeight = `trace.cloud.google.com/sampling_weight`
)

/*
/agent
/component
/error/message
/error/name
/http/client_city
/http/client_country
/http/client_protocol
/http/client_region
/http/host
/http/method
/http/redirected_url
/http/request/size
/http/response/size
/http/status_code
/http/url
/http/user_agent
/pid
/stacktrace
/tid
*/

const (
	// ScopeTraceAppend grants permissions to write trace data for a project.
	ScopeTraceAppend = "https://www.googleapis.com/auth/trace.append"

	// ScopeCloudPlatform grants permissions to view and manage your data
	// across Google Cloud Platform services.
	ScopeCloudPlatform = "https://www.googleapis.com/auth/cloud-platform"
)

var (
	spanIDCounter   uint64
	spanIDIncrement uint64
)

func init() {
	// Set spanIDCounter and spanIDIncrement to random values.  nextSpanID will
	// return an arithmetic progression using these values, skipping zero.  We set
	// the LSB of spanIDIncrement to 1, so that the cycle length is 2^64.
	binary.Read(rand.Reader, binary.LittleEndian, &spanIDCounter)
	binary.Read(rand.Reader, binary.LittleEndian, &spanIDIncrement)
	spanIDIncrement |= 1
}

// nextSpanID returns a new span ID.  It will never return zero.
func nextSpanID() uint64 {
	var id uint64
	for id == 0 {
		id = atomic.AddUint64(&spanIDCounter, spanIDIncrement)
	}
	return id
}

func (c *client) upload(traces []*api.Trace) error {
	log.Printf("uploading: %#v\n", traces)
	_, err := c.service.Projects.PatchTraces(c.projectID, &api.Traces{Traces: traces}).Do()
	return err
}

// newClient creates a new Google Stackdriver Trace client.
func newClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*client, error) {
	o := []option.ClientOption{
		option.WithScopes(cloudPlatformScope, ScopeTraceAppend, ScopeCloudPlatform),
		option.WithUserAgent(userAgent),
	}
	o = append(o, opts...)
	hc, basePath, err := transport.NewHTTPClient(ctx, o...)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client for Google Stackdriver Trace API: %v", err)
	}
	apiService, err := api.New(hc)
	if err != nil {
		return nil, fmt.Errorf("creating Google Stackdriver Trace API client: %v", err)
	}
	if basePath != "" {
		// An option set a basepath, so override api.New's default.
		apiService.BasePath = basePath
	}
	c := &client{
		service:   apiService,
		projectID: projectID,
	}
	bundler := bundler.NewBundler((*api.Trace)(nil), func(bundle interface{}) {
		traces := bundle.([]*api.Trace)
		err := c.upload(traces)
		log.Println("uploaded")
		if err != nil {
			log.Printf("failed to upload %d traces to the Cloud Trace server: %v", len(traces), err)
		}
	})
	bundler.DelayThreshold = 2 * time.Second
	bundler.BundleCountThreshold = 100
	// We're not measuring bytes here, we're counting traces and spans as one "byte" each.
	bundler.BundleByteThreshold = 1000
	bundler.BundleByteLimit = 1000
	bundler.BufferedByteLimit = 10000
	c.bundler = bundler
	return c, nil
}
