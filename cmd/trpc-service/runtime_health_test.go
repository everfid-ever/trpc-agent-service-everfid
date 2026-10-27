package main

import (
	"context"
	"errors"
	"testing"
)

func TestRuntimeHealthRequiresEveryComponentPollAndRecovers(t *testing.T) {
	h := newRuntimeHealth(map[string]string{"worker": "node-a-start-1", "relay": "node-a-start-1-relay"})
	if ready, _ := h.snapshot(); ready {
		t.Fatal("unstarted components ready")
	}
	h.observe("worker", nil)
	if ready, _ := h.snapshot(); ready {
		t.Fatal("partial startup ready")
	}
	h.observe("relay", nil)
	if ready, _ := h.snapshot(); !ready {
		t.Fatal("healthy idle components not ready")
	}
	h.observe("worker", errors.New("secret dependency failure"))
	ready, states := h.snapshot()
	if ready || states["worker"].Reason != "operation_failed" || states["worker"].LastSuccess.IsZero() || states["worker"].LastFailure.IsZero() {
		t.Fatal(states)
	}
	states["relay"] = componentHealth{}
	h.observe("worker", nil)
	if ready, states = h.snapshot(); !ready || states["relay"].State != "ready" || states["worker"].Failures != 1 {
		t.Fatal(states)
	}
	h.stop()
	h.observe("worker", nil)
	if ready, _ := h.snapshot(); ready {
		t.Fatal("shutdown became ready")
	}
}

func TestRuntimeHealthUnexpectedCleanExitDegrades(t *testing.T) {
	h := newRuntimeHealth(map[string]string{"worker": "node-a"})
	h.observe("worker", nil)
	if err := h.run("worker", func(context.Context) error { return nil })(context.Background()); err == nil {
		t.Fatal("clean exit accepted")
	}
	if ready, _ := h.snapshot(); ready {
		t.Fatal("stopped component ready")
	}
}

func TestRuntimeOwnersChangeOnProcessRejoin(t *testing.T) {
	a := webUILocalConfig{InstanceID: "node-a", ProcessStartID: "start-1"}
	b := a
	b.ProcessStartID = "start-2"
	if a.instanceName("worker") == b.instanceName("worker") || a.instanceName("worker") == a.instanceName("delivery") {
		t.Fatal("owner identity collision")
	}
}
