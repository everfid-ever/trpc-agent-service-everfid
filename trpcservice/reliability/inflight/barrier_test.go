package inflight

import (
	"context"
	"testing"
	"time"
)

func TestControllerBlocksUntilReleased(t *testing.T) {
	var controller Controller
	hit := controller.Arm(PointP2BeforeTerminalCommit)
	done := make(chan error, 1)
	go func() { done <- controller.Wait(context.Background(), PointP2BeforeTerminalCommit) }()
	select {
	case <-hit:
	case <-time.After(time.Second):
		t.Fatal("barrier was not hit")
	}
	if snapshot := controller.Snapshot(PointP2BeforeTerminalCommit); !snapshot.Armed || !snapshot.Hit || snapshot.Released {
		t.Fatalf("snapshot=%#v", snapshot)
	}
	if !controller.Release(PointP2BeforeTerminalCommit) {
		t.Fatal("release did not find armed barrier")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("barrier did not release")
	}
}

func TestControllerHonorsCancellation(t *testing.T) {
	var controller Controller
	controller.Arm(PointP2BeforeTerminalCommit)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := controller.Wait(ctx, PointP2BeforeTerminalCommit); err != context.Canceled {
		t.Fatalf("err=%v", err)
	}
}
