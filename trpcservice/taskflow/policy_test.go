package taskflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
)

func TestEmptyPlanCannotSucceedAndStagnationSurvivesTakeover(t *testing.T) {
	ctx := context.Background()
	s, store, _ := session(t)
	if _, err := s.ValidateCompletion(ctx); !errors.Is(err, taskflow.ErrIncomplete) {
		t.Fatalf("empty plan passed: %v", err)
	}
	for attempt := 1; attempt <= 4; attempt++ {
		if attempt > 1 {
			s.Scope.Fence++
			if _, err := store.Open(ctx, s.Scope, taskflow.Digest([]byte("original input")), 1); err != nil {
				t.Fatal(err)
			}
		}
		p, err := s.BeginAttempt(ctx)
		if attempt < 4 {
			if err != nil {
				t.Fatal(err)
			}
		} else if taskflow.StopReason(err) != "task_no_progress" || p.Phase != "failed" || p.Attempts != 4 || p.NoProgress != 3 {
			t.Fatalf("did not converge: %+v %v", p, err)
		}
	}
	if _, err := s.Recover(ctx); taskflow.StopReason(err) != "task_no_progress" {
		t.Fatalf("failure lost: %v", err)
	}
}

func TestOnlyCompletedEvidenceResetsStagnation(t *testing.T) {
	ctx := context.Background()
	s, _, _ := session(t)
	p, err := s.BeginAttempt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.Define(ctx, p.Version, []taskflow.Step{{ID: "a", Title: "a", Criterion: "saved analysis", Kind: "analysis"}})
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.Start(ctx, p.Version, "a")
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.BeginAttempt(ctx)
	if err != nil || p.NoProgress != 1 {
		t.Fatalf("start reset stagnation: %+v %v", p, err)
	}
	p, err = s.CompleteAnalysis(ctx, p.Version, "a", "Saved calculation, 2+3=5.")
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.BeginAttempt(ctx)
	if err != nil || p.NoProgress != 0 {
		t.Fatalf("new evidence ignored: %+v %v", p, err)
	}
	if _, err = s.BeginAttempt(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptAndTimeLimitsCannotBeResetByNewOwner(t *testing.T) {
	for _, kind := range []string{"attempts", "deadline"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			s, store, _ := session(t)
			p, _ := store.Get(ctx, s.Scope.Key)
			p.Contract.MaxNoProgress = 100
			p.Contract.MaxAttempts = 2
			p.Contract.MaxAgeSeconds = 10
			if _, err := store.Save(ctx, s.Scope, p.Version, p); err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 10, 31, 1, 0, 0, 0, time.UTC)
			s.Now = func() time.Time { return now }
			if _, err := s.BeginAttempt(ctx); err != nil {
				t.Fatal(err)
			}
			if kind == "deadline" {
				now = now.Add(10 * time.Second)
			} else {
				if _, err := s.BeginAttempt(ctx); err != nil {
					t.Fatal(err)
				}
			}
			s.Scope.Fence++
			if _, err := store.Open(ctx, s.Scope, p.InputDigest, p.KeyVersion); err != nil {
				t.Fatal(err)
			}
			_, err := s.BeginAttempt(ctx)
			want := "task_attempts_exhausted"
			if kind == "deadline" {
				want = "task_deadline_exceeded"
			}
			if taskflow.StopReason(err) != want {
				t.Fatalf("%s: %v", want, err)
			}
		})
	}
}

func TestMissingOrWeakenedToolEvidenceCannotPass(t *testing.T) {
	ctx := context.Background()
	s, _, r := session(t)
	step := taskflow.Step{ID: "write", Title: "write", Criterion: "provider created item", Kind: "tool", ToolID: "write", ToolVersion: 1}
	if _, err := s.Define(ctx, 1, []taskflow.Step{step}); !errors.Is(err, taskflow.ErrEvidenceRequired) {
		t.Fatal("missing check accepted", err)
	}
	step.Check = &taskflow.ResultCheck{NonEmpty: true}
	p, err := s.Define(ctx, 1, []taskflow.Step{step})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Start(ctx, p.Version, "write"); err != nil {
		t.Fatal(err)
	}
	args := []byte(`{}`)
	r.r = execution.Record{Key: execution.Key{TenantID: "t", RequestID: "r", ToolCallID: "op"}, ToolID: "write", ToolVersion: 1, ArgsDigest: taskflow.Digest(args), Policy: "idempotent_key", State: execution.Running}
	if err = s.BeforeTool(ctx, r.r, args); !errors.Is(err, taskflow.ErrEvidenceRequired) {
		t.Fatal("missing implementation contract accepted", err)
	}
	if err = s.BeforeTool(ctx, r.r, args, &taskflow.ResultCheck{Path: []string{"status"}, EqualsJSON: `"created"`}); err != nil {
		t.Fatal(err)
	}
	r.r.State = execution.Succeeded
	r.b = []byte(`{"status":"failed","message":"not empty"}`)
	r.r.ResultRef = "tool-result://sha256/" + taskflow.Digest(r.b)
	if _, err = s.ValidateCompletion(ctx); !errors.Is(err, taskflow.ErrEvidenceRejected) {
		t.Fatalf("weak model check bypassed trusted check: %v", err)
	}
}

func TestPermanentOrManualUnknownStepStopsBeforeAnotherModelRun(t *testing.T) {
	for _, state := range []execution.State{execution.Failed, execution.EffectUnknown} {
		t.Run(string(state), func(t *testing.T) {
			ctx := context.Background()
			s, store, r := session(t)
			p, err := s.Define(ctx, 1, []taskflow.Step{{ID: "effect", Title: "effect", Criterion: "receipt", Kind: "tool", ToolID: "write", ToolVersion: 1, Check: &taskflow.ResultCheck{NonEmpty: true}}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Start(ctx, p.Version, "effect"); err != nil {
				t.Fatal(err)
			}
			args := []byte(`{}`)
			r.r = execution.Record{Key: execution.Key{TenantID: "t", RequestID: "r", ToolCallID: "op"}, ToolID: "write", ToolVersion: 1, ArgsDigest: taskflow.Digest(args), Policy: "manual", State: execution.Running}
			if err = s.BeforeTool(ctx, r.r, args, &taskflow.ResultCheck{NonEmpty: true}); err != nil {
				t.Fatal(err)
			}
			r.r.State = state
			_, err = s.Recover(ctx)
			want := "task_step_failed"
			if state == execution.EffectUnknown {
				want = "task_effect_unknown_manual"
			}
			if taskflow.StopReason(err) != want {
				t.Fatal(err)
			}
			p, _ = store.Get(ctx, s.Scope.Key)
			if p.Phase != "failed" || p.FailureReason != want {
				t.Fatal(p)
			}
			if _, err = s.Start(ctx, p.Version, "effect"); err == nil {
				t.Fatal("stopped step reopened")
			}
		})
	}
}

func TestLiveToolLeaseWaitDoesNotConsumeModelAttemptsButStillHasDeadline(t *testing.T) {
	ctx := context.Background()
	s, store, r := session(t)
	now := time.Date(2026, 10, 31, 2, 0, 0, 0, time.UTC)
	s.Now = func() time.Time { return now }
	p, err := s.BeginAttempt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.Contract.MaxAgeSeconds = 10
	p, err = store.Save(ctx, s.Scope, p.Version, p)
	if err != nil {
		t.Fatal(err)
	}
	p, err = s.Define(ctx, p.Version, []taskflow.Step{{ID: "read", Title: "read", Criterion: "receipt", Kind: "tool", ToolID: "read", ToolVersion: 1, Check: &taskflow.ResultCheck{NonEmpty: true}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Start(ctx, p.Version, "read"); err != nil {
		t.Fatal(err)
	}
	args := []byte(`{}`)
	r.r = execution.Record{Key: execution.Key{TenantID: "t", RequestID: "r", ToolCallID: "op"}, ToolID: "read", ToolVersion: 1, ArgsDigest: taskflow.Digest(args), Policy: "replay_safe", State: execution.Running, LeaseUntil: now.Add(time.Minute)}
	if err = s.BeforeTool(ctx, r.r, args); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_, err = s.Recover(ctx)
		var busy taskflow.ToolBusyError
		if !errors.As(err, &busy) {
			t.Fatalf("lease wait: %v", err)
		}
	}
	p, _ = store.Get(ctx, s.Scope.Key)
	if p.Attempts != 1 || p.NoProgress != 0 {
		t.Fatal("lease wait spent recovery budget", p)
	}
	now = now.Add(10 * time.Second)
	if _, err = s.Recover(ctx); taskflow.StopReason(err) != "task_deadline_exceeded" {
		t.Fatalf("wait escaped deadline: %v", err)
	}
}
