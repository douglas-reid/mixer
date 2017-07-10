package sdtrace

import (
	"testing"
	"time"

	"istio.io/mixer/pkg/adapter"
)

func TestTrace(t *testing.T) {
	labels := map[string]interface{}{
		"start":          time.Now(),
		"end":            time.Now().Add(50 * time.Millisecond),
		"trace_id":       "00005028a693a215",
		"parent_span_id": "0000000000000004",
		"service":        "productpage",
		"host":           "istio-bookstore-node-1",
		"method":         "/productpage",
		"url":            "http://ingress/productpage",
	}

	labels2 := map[string]interface{}{
		"start":          time.Now().Add(51 * time.Millisecond),
		"end":            time.Now().Add(350 * time.Millisecond),
		"trace_id":       "00005028a693a215",
		"parent_span_id": "0000000000000004",
		"service":        "reviews",
		"host":           "istio-reviews-node-3",
		"method":         "/reviews",
		"url":            "reviews-svc.default.svc.cluster.local:8080/reviews",
	}

	tracer := &tracer{projectID: "kubernetes-hello-world-1385"}

	err := tracer.trace([]adapter.LogEntry{{Labels: labels}, {Labels: labels2}})
	time.Sleep(5 * time.Second)
	if err != nil {
		t.Errorf("failure: %v", err)
	}
}
