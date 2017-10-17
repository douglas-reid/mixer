// Copyright 2017 Istio Authors.
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

// Package census publishes metric values collected by Mixer.
package census // import "istio.io/mixer/adapter/census"

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/census-instrumentation/opencensus-go/stats"
	"github.com/census-instrumentation/opencensus-go/tags"
	multierror "github.com/hashicorp/go-multierror"

	"istio.io/mixer/adapter/census/config"
	"istio.io/mixer/pkg/adapter"
	"istio.io/mixer/template/metric"
)

type (
	builder struct {
		cfg *config.Params
	}

	handler struct {
		views    map[string]*stats.View
		measures map[string]stats.Measure
	}
)

var (
	_ metric.HandlerBuilder = &builder{}
	_ metric.Handler        = &handler{}
)

// GetInfo returns the Info associated with this adapter.
func GetInfo() adapter.Info {
	return adapter.Info{
		Name:        "census",
		Impl:        "istio.io/mixer/adapter/census",
		Description: "Publishes census metrics",
		SupportedTemplates: []string{
			metric.TemplateName,
		},
		NewBuilder:    func() adapter.HandlerBuilder { return &builder{} },
		DefaultConfig: &config.Params{},
	}
}

func (b *builder) SetMetricTypes(types map[string]*metric.Type) {}
func (b *builder) SetAdapterConfig(cfg adapter.Config)          { b.cfg = cfg.(*config.Params) }
func (b *builder) Validate() *adapter.ConfigErrors              { return nil }
func (b *builder) Build(ctx context.Context, env adapter.Env) (adapter.Handler, error) {

	var returnErr *multierror.Error
	cfg := b.cfg

	measures := make(map[string]stats.Measure, len(cfg.Views))
	views := make(map[string]*stats.View, len(cfg.Views))
	for _, view := range cfg.Views {
		dims := make([]tags.Key, 0, len(view.TagNames))
		for _, name := range view.TagNames {
			key, _ := tags.KeyStringByName(name)
			dims = append(dims, key)
		}

		m := view.Measure
		var measure stats.Measure
		var err error
		switch m.Kind {
		case config.INT:
			measure, err = stats.NewMeasureInt64(m.Name, m.DisplayName, m.Unit)
			if err != nil {
				returnErr = multierror.Append(returnErr, fmt.Errorf("could not create measure '%s': %v", m.Name, err))
			}
		case config.FLOAT:
			measure, err = stats.NewMeasureFloat64(m.Name, m.DisplayName, m.Unit)
			if err != nil {
				returnErr = multierror.Append(returnErr, fmt.Errorf("could not create measure '%s': %v", m.Name, err))
			}
		}

		var agg stats.Aggregation
		if view.GetCount() != nil {
			agg = stats.NewAggregationCount()
		} else {
			dist := view.GetDistribution()
			agg = stats.NewAggregationDistribution(dist.Bounds)
		}

		var wind stats.Window
		if view.GetCumulative() != nil {
			wind = stats.NewWindowCumulative()
		} else if view.GetSlidingTime() != nil {
			st := view.GetSlidingTime()
			wind = stats.NewWindowSlidingTime(st.WindowDuration, int(st.SubIntervals))
		} else {
			sc := view.GetSlidingCount()
			wind = stats.NewWindowSlidingCount(sc.Samples, int(sc.Subsets))
		}

		newView := stats.NewView(view.Name, view.DisplayName, dims, measure, agg, wind)

		if err := stats.RegisterView(newView); err != nil {
			returnErr = multierror.Append(returnErr, fmt.Errorf("could not create register view: %s. %v", newView.Name(), err))
		}

		if err := newView.ForceCollect(); err != nil {
			returnErr = multierror.Append(returnErr, fmt.Errorf("could not force collect view: %s. %v", newView.Name(), err))
		}

		views[m.InstanceName] = newView
		measures[m.InstanceName] = measure
	}

	addr := fmt.Sprintf(":%d", cfg.HandlerPort)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		returnErr = multierror.Append(returnErr, fmt.Errorf("could not start census server: %v", err))
	}
	srvMux := http.NewServeMux()
	srvMux.Handle("/views", handlerFn(views))
	srv := &http.Server{Addr: addr, Handler: srvMux}
	env.ScheduleDaemon(func() {
		env.Logger().Infof("serving census metrics on '%s'", addr)
		if err := srv.Serve(listener.(*net.TCPListener)); err != nil {
			if err == http.ErrServerClosed {
				env.Logger().Infof("HTTP server stopped")
			} else {
				_ = env.Logger().Errorf("census HTTP server error: %v", err) // nolint: gas
			}
		}
	})

	return &handler{views, measures}, returnErr.ErrorOrNil()
}

func handlerFn(views map[string]*stats.View) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		for _, v := range views {
			rows, err := v.RetrieveData()
			if err != nil {
				fmt.Println("BAD MOJO", err)
			}
			var buf bytes.Buffer
			for _, r := range rows {
				buf.WriteString(v.Name())
				buf.WriteString(" : ")
				buf.WriteString(r.String())
				buf.WriteString("\n")
			}
			w.Write(buf.Bytes())
		}
	}
}

func (h *handler) HandleMetric(origCtx context.Context, vals []*metric.Instance) error {
	var result *multierror.Error

	for _, val := range vals {
		m := h.measures[val.Name]
		if m == nil {
			result = multierror.Append(result, fmt.Errorf("could not find view info for instance '%s'", val.Name))
			continue
		}

		tsb := tags.NewTagSetBuilder(nil)
		for key, val := range val.Dimensions {
			key, _ := tags.KeyStringByName(key)
			tsb.UpsertString(key, fmt.Sprintf("%v", val))
		}

		ctx := tags.NewContext(origCtx, tsb.Build())

		switch t := val.Value.(type) {
		case int64:
			m.(*stats.MeasureInt64).Record(ctx, t)
		case float64:
			m.(*stats.MeasureFloat64).Record(ctx, t)
		case time.Duration:
			m.(*stats.MeasureInt64).Record(ctx, t.Nanoseconds()/1e6)
		}
	}

	return result.ErrorOrNil()
}

func (h *handler) Close() error { return nil }
