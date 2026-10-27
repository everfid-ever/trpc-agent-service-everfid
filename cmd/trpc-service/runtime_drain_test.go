package main

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/drain"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeDrainRejectsHTTPAndStopsPollClaims(t *testing.T) {
	g := drain.New()
	health := newRuntimeHealth(map[string]string{"worker": "a"})
	health.observe("worker", nil)
	var claims atomic.Int32
	poll := drainPoll(g, func(context.Context, int) (int, error) { claims.Add(1); return 1, nil })
	if _, err := poll(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	health.beginDrain()
	g.Begin()
	health.observe("worker", nil)
	if ready, states := health.snapshot(); ready || states["worker"].State != "draining" {
		t.Fatal(states)
	}
	if _, err := poll(context.Background(), 1); err != errRuntimeDrained || claims.Load() != 1 {
		t.Fatal("claimed during drain")
	}
	called := false
	handler := drainingHTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(200) }), g)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("POST", "/callbacks/wecom", nil))
	if w.Code != 503 || called {
		t.Fatal("callback admitted during drain")
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/statusz", nil))
	if w.Code != 200 || !called {
		t.Fatal("lost drain diagnostics")
	}
}

func TestDrainedWorkerIsNotRestarted(t *testing.T) {
	signal := make(chan struct{})
	started := make(chan struct{})
	var runs atomic.Int32
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		done <- runUntilDrained(ctx, signal, "worker", func(ctx context.Context) error {
			runs.Add(1)
			close(started)
			<-signal
			if ctx.Err() != nil {
				t.Error("active work canceled before drain")
			}
			return nil
		}, testLogger())
	}()
	<-started
	close(signal)
	if err := <-done; err != nil || runs.Load() != 1 {
		t.Fatalf("runs=%d err=%v", runs.Load(), err)
	}
}
