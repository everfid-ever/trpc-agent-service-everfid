package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type incompleteEntryBody struct{}

func (incompleteEntryBody) Read([]byte) (int, error) { return 0, errors.New("interrupted body") }
func (incompleteEntryBody) Close() error             { return nil }

func TestEntryDoesNotRetryIncompleteAuthenticationRejection(t *testing.T) {
	var callbacks atomic.Int32
	entry := testEntry(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/readyz" {
			return entryResponse(http.StatusOK, ""), nil
		}
		callbacks.Add(1)
		response := entryResponse(http.StatusUnauthorized, "")
		response.Body = incompleteEntryBody{}
		return response, nil
	})
	entry.probe(context.Background())
	w := httptest.NewRecorder()
	entry.serveHTTP(w, httptest.NewRequest("POST", "/callbacks/wecom", strings.NewReader("body")))
	if w.Code != http.StatusUnauthorized || callbacks.Load() != 1 || !entry.healthy(0) {
		t.Fatal("authentication rejection retried or incorrectly removed backend")
	}
}

type entryTransport func(*http.Request) (*http.Response, error)

func (f entryTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func entryResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"text/plain"}}, Body: io.NopCloser(strings.NewReader(body))}
}
func testEntry(t *testing.T, transport entryTransport) *wecomHAEntry {
	t.Helper()
	a, _ := url.Parse("http://node-a:8080")
	b, _ := url.Parse("http://node-b:8080")
	entry, err := newWeComHAEntry([]*url.URL{a, b}, &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	return entry
}
func TestEntryRetriesUnavailableBackendButPreservesAuthenticationRejection(t *testing.T) {
	for _, status := range []int{502, 503, 504, 401, 403} {
		var callsB atomic.Int32
		entry := testEntry(t, func(r *http.Request) (*http.Response, error) {
			if r.URL.Path == "/readyz" {
				return entryResponse(200, ""), nil
			}
			data, _ := io.ReadAll(r.Body)
			if string(data) != "signed-body" || r.URL.RawQuery != "msg_signature=original" || r.Header.Get("X-Signature") != "signature" {
				t.Error("changed signed callback")
			}
			if r.Host != "" || r.Header.Get("X-Hop") != "" {
				t.Error("hop header forwarded")
			}
			if r.URL.Hostname() == "node-a" {
				return entryResponse(status, "first backend error"), nil
			}
			callsB.Add(1)
			return entryResponse(200, "success"), nil
		})
		entry.probe(context.Background())
		r := httptest.NewRequest("POST", "/callbacks/wecom?msg_signature=original", strings.NewReader("signed-body"))
		r.Header.Set("X-Signature", "signature")
		r.Header.Set("Connection", "X-Hop")
		r.Header.Set("X-Hop", "strip")
		w := httptest.NewRecorder()
		entry.serveHTTP(w, r)
		if status >= 500 {
			if w.Code != 200 || w.Body.String() != "success" || callsB.Load() != 1 || entry.health[0].Load() {
				t.Fatalf("%d response=%d %s", status, w.Code, w.Body)
			}
		} else if w.Code != status || callsB.Load() != 0 {
			t.Fatal("retried an authentication rejection")
		}
	}
}
func TestEntryProbesHealthyPeerWithoutWaitingForSlowNode(t *testing.T) {
	fast := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entry := testEntry(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Hostname() == "node-a" {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		close(fast)
		return entryResponse(200, ""), nil
	})
	entry.probeTimeout = 2 * time.Second
	done := make(chan struct{})
	go func() { entry.probe(ctx); close(done) }()
	select {
	case <-fast:
	case <-time.After(time.Second):
		t.Fatal("healthy peer blocked by slow probe")
	}
	deadline := time.Now().Add(time.Second)
	for !entry.healthy(1) {
		if time.Now().After(deadline) {
			t.Fatal("peer never became ready")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("slow probe unexpectedly completed")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe cancellation failed")
	}
}
func TestEntryRequiresRecoveryStabilityAndRejectsStaleProbe(t *testing.T) {
	entry := testEntry(t, func(*http.Request) (*http.Response, error) { return entryResponse(200, ""), nil })
	entry.recordProbe(0, 0, true)
	if !entry.healthy(0) {
		t.Fatal("startup probe did not admit node")
	}
	entry.failBackend(0, "transport_failed")
	entry.recordProbe(0, 0, true)
	if entry.healthy(0) {
		t.Fatal("old probe resurrected failed node")
	}
	entry.recordProbe(0, 1, true)
	if entry.healthy(0) {
		t.Fatal("single success bypassed rejoin stability")
	}
	entry.recordProbe(0, 1, true)
	if !entry.healthy(0) {
		t.Fatal("stable node not readmitted")
	}
	entry.mu.Lock()
	entry.backends[0].CheckedAt = time.Now().Add(-entry.healthTTL - time.Second)
	entry.mu.Unlock()
	if entry.healthy(0) {
		t.Fatal("expired health remained eligible")
	}
	entry.recordProbe(0, 1, true)
	if entry.healthy(0) {
		t.Fatal("stale health bypassed recovery window")
	}
	entry.recordProbe(0, 1, true)
	if !entry.healthy(0) {
		t.Fatal("stale node never recovered")
	}
}
