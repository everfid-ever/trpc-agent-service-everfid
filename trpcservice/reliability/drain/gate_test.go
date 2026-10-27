package drain

import (
	"sync"
	"testing"
)

func TestDrainRejectsNewAdmissionsAndWaitsForEveryOwner(t *testing.T) {
	g := New()
	leave, ok := g.Enter()
	if !ok {
		t.Fatal("not admitted")
	}
	g.Begin()
	g.Begin()
	if !Signaled(g.Signal()) {
		t.Fatal("no stop signal")
	}
	if _, ok := g.Enter(); ok {
		t.Fatal("admitted after drain")
	}
	if Signaled(g.Done()) {
		t.Fatal("active operation not drained")
	}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); leave() }()
	}
	wg.Wait()
	if !Signaled(g.Done()) {
		t.Fatal("drain did not finish")
	}
}
