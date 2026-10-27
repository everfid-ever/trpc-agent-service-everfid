package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/drain"
)

var errRuntimeDrained = errors.New("runtime admissions stopped")

func drainPoll(gate *drain.Gate, operation func(context.Context, int) (int, error)) func(context.Context, int) (int, error) {
	return func(ctx context.Context, limit int) (int, error) {
		leave, ok := gate.Enter()
		if !ok {
			return 0, errRuntimeDrained
		}
		defer leave()
		return operation(ctx, limit)
	}
}

// Keep retrying failed consumers while serving. A completed drain is final:
// do not resurrect the consumer after it has stopped admitting work.
func runUntilDrained(ctx context.Context, signal <-chan struct{}, name string, operation func(context.Context) error, logger *roleLogger) error {
	retryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := runRecoverableLoop(retryCtx, name, func(runCtx context.Context) error {
		err := operation(runCtx)
		if drain.Signaled(signal) {
			cancel()
		}
		return err
	}, 250*time.Millisecond, logger)
	if ctx.Err() == nil && drain.Signaled(signal) {
		return nil
	}
	return err
}

func drainingHTTP(next http.Handler, gate *drain.Gate) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gate.Draining() && r.URL.Path != "/livez" && r.URL.Path != "/readyz" && r.URL.Path != "/statusz" {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "instance draining", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}
