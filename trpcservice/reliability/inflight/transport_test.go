package inflight

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestModelBarrierRunsAfterUpstreamHeadersAndScopesTenant(t *testing.T) {
	c := &Controller{}
	hit := c.ArmFor(PointP2DuringModelResponse, "tenant-a", "request-a")
	issued := make(chan struct{}, 2)
	transport := ModelTransport{Base: roundTripFunc(func(*http.Request) (*http.Response, error) {
		issued <- struct{}{}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("model result"))}, nil
	})}
	ctx := WithBarrier(WithScope(context.Background(), "tenant-b", "request-b", ""), c)
	request, _ := http.NewRequestWithContext(ctx, "POST", "http://model.test", nil)
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	<-issued
	if c.Snapshot(PointP2DuringModelResponse).Hit {
		t.Fatal("control tenant was paused")
	}
	ctx = WithBarrier(WithScope(context.Background(), "tenant-a", "request-a", ""), c)
	request, _ = http.NewRequestWithContext(ctx, "POST", "http://model.test", nil)
	done := make(chan error, 1)
	go func() {
		response, err := transport.RoundTrip(request)
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	select {
	case <-issued:
	case <-time.After(time.Second):
		t.Fatal("upstream was never invoked")
	}
	select {
	case <-hit:
	case <-time.After(time.Second):
		t.Fatal("barrier not reached")
	}
	if snapshot := c.Snapshot(PointP2DuringModelResponse); snapshot.TenantID != "tenant-a" || snapshot.RequestID != "request-a" || snapshot.Evidence != "model_http_status:200" {
		t.Fatal(snapshot)
	}
	select {
	case <-done:
		t.Fatal("model result escaped barrier")
	default:
	}
	c.Release(PointP2DuringModelResponse)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
