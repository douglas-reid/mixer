// Copyright 2017 Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package sdtrace provides an implementation of Mixer's access-logs aspect that
// generates Stackdriver traces from Report()s and pushes them to Stackdriver.
package sdtrace // import "istio.io/mixer/adapter/sdtrace"

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	api "google.golang.org/api/cloudtrace/v1"
	"google.golang.org/api/option"
	"google.golang.org/api/support/bundler"
	"google.golang.org/api/transport"

	"istio.io/mixer/adapter/sdtrace/config"
	"istio.io/mixer/pkg/adapter"
)

type (
	builder struct{ adapter.DefaultBuilder }
	tracer  struct {
		projectID string
		// apiKey    string
		// serviceAccountFile string
	}

	// Client is a client for uploading traces to the Google Stackdriver Trace server.
	client struct {
		service   *api.Service
		projectID string
		//		policy    SamplingPolicy
		bundler *bundler.Bundler
	}
)

// Register records the builders exposed by this adapter.
func Register(r adapter.Registrar) {
	b := builder{adapter.NewDefaultBuilder(
		"sdtrace",
		"Generates trace data from access-logs and sends them to Stackdriver",
		&config.Params{},
	)}

	r.RegisterAccessLogsBuilder(b)
}

func (builder) NewAccessLogsAspect(env adapter.Env, cfg adapter.Config) (adapter.AccessLogsAspect, error) {
	return newTracer(cfg)
}

func newTracer(cfg adapter.Config) (*tracer, error) {
	c := cfg.(*config.Params)

	return &tracer{projectID: c.ProjectId}, nil
}

func (t *tracer) LogAccess(entries []adapter.LogEntry) error {
	return t.trace(entries)
}

func (t *tracer) trace(entries []adapter.LogEntry) error {

	//option.WithServiceAccountFile(t.serviceAccountFile)
	traceClient, err := newClient(context.Background(), t.projectID) //, option.WithAPIKey(t.apiKey))
	if err != nil {
		return fmt.Errorf("building Stackdriver client: %v", err)
	}

	for _, entry := range entries {

		traceID, ok := entry.Labels["trace_id"].(string)
		if !ok {
			// consider logging here and/or returning error
			continue
		}

		tID, err := strconv.ParseInt(traceID, 16, 64)
		if err != nil {
			continue
		}

		//// TraceId: This identifier is a 128-bit
		//// numeric value formatted as a 32-byte hex string.
		traceID = fmt.Sprintf("%032x", tID)

		parentSpanID, ok := entry.Labels["parent_span_id"].(string)
		if !ok {
			// consider logging here and/or returning error
			continue
		}

		psID, err := strconv.ParseUint(parentSpanID, 16, 64)
		if err != nil {
			continue
		}

		sID := nextSpanID()

		svc, ok := entry.Labels["service"].(string)
		if !ok {
			// consider logging here and/or returning error
			continue
		}

		startTime, ok := entry.Labels["start"].(time.Time)
		if !ok {
			startTime = time.Now()
		}
		start := startTime.In(time.UTC).Format(time.RFC3339Nano)

		endTime, ok := entry.Labels["end"].(time.Time)
		if !ok {
			endTime = time.Now()
		}
		end := endTime.In(time.UTC).Format(time.RFC3339Nano)

		lbls := map[string]string{}
		if host, ok := entry.Labels["host"].(string); ok {
			lbls[labelHost] = host
		}
		if method, ok := entry.Labels["method"].(string); ok {
			lbls[labelMethod] = method
		}
		if url, ok := entry.Labels["url"].(string); ok {
			lbls[labelURL] = url
		}
		if statusCode, ok := entry.Labels["status_code"].(string); ok && statusCode != "0" {
			lbls[labelStatusCode] = statusCode
		}

		span := &api.TraceSpan{
			EndTime:      end,
			StartTime:    start,
			Kind:         spanKindServer,
			Name:         svc,
			ParentSpanId: psID,
			SpanId:       sID, //in  must be non-0 and unique
			Labels:       lbls,
		}

		//log.Printf("span: %+v", *span)

		trace := &api.Trace{
			ProjectId: t.projectID,
			Spans:     []*api.TraceSpan{span},
			TraceId:   traceID,
		}

		//log.Printf("adding trace: %+v\n", *trace)
		err = traceClient.bundler.Add(trace, 2)
		if err == bundler.ErrOversizedItem {
			err = traceClient.upload([]*api.Trace{trace})
			log.Println("uploaded")
		}
		if err != nil {
			log.Println("error uploading trace:", err)
		}
	}
	return nil
}

func (t *tracer) Close() error { return nil }

const (
	httpHeader          = `X-Cloud-Trace-Context`
	userAgent           = `gcloud-golang-trace/20160501`
	cloudPlatformScope  = `https://www.googleapis.com/auth/cloud-platform`
	spanKindClient      = `RPC_CLIENT`
	spanKindServer      = `RPC_SERVER`
	spanKindUnspecified = `SPAN_KIND_UNSPECIFIED`
	labelHost           = `trace.cloud.google.com/http/host`
	labelMethod         = `trace.cloud.google.com/http/method`
	labelStackTrace     = `trace.cloud.google.com/stacktrace`
	labelStatusCode     = `trace.cloud.google.com/http/status_code`
	labelURL            = `trace.cloud.google.com/http/url`
	labelSamplingPolicy = `trace.cloud.google.com/sampling_policy`
	labelSamplingWeight = `trace.cloud.google.com/sampling_weight`
)

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
