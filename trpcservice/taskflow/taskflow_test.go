package taskflow_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
)

type receipts struct {
	execution.Store
	r execution.Record
	b []byte
}

func (r *receipts) Get(_ context.Context, k execution.Key) (execution.Record, error) {
	if k != r.r.Key {
		return execution.Record{}, runtime.ErrNotFound
	}
	return r.r, nil
}
func (r *receipts) LoadResult(_ context.Context, k execution.Key) ([]byte, error) {
	if k != r.r.Key {
		return nil, runtime.ErrNotFound
	}
	return r.b, nil
}
func (*receipts) Complete(context.Context, execution.Record, []byte, int64) (execution.Record, error) {
	panic("not called")
}
func session(t *testing.T) (*taskflow.Session, *inmemory.Store, *receipts) {
	t.Helper()
	store := inmemory.New()
	scope := taskflow.Scope{Key: taskflow.Key{TenantID: "t", RequestID: "r"}, Fence: 7}
	if _, err := store.Open(context.Background(), scope, taskflow.Digest([]byte("original input")), 1); err != nil {
		t.Fatal(err)
	}
	r := &receipts{}
	return &taskflow.Session{Store: store, Tools: r, Results: r, Scope: scope}, store, r
}
func TestCrashAfterToolResultBeforeCheckpointResumesWithoutRepeatingEffect(t *testing.T) {
	ctx := context.Background()
	s, store, r := session(t)
	p, err := s.Define(ctx, 1, []taskflow.Step{
		{ID: "collect", Title: "collect", Criterion: "query receipt", Kind: "tool", ToolID: "query", ToolVersion: 1, Check: &taskflow.ResultCheck{NonEmpty: true}},
		{ID: "analyze", Title: "analyze", Criterion: "derive summary from receipt", Kind: "analysis", DependsOn: []string{"collect"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Start(ctx, p.Version, "analyze"); !errors.Is(err, taskflow.ErrIncomplete) {
		t.Fatalf("dependency: %v", err)
	}
	p, err = s.Start(ctx, p.Version, "collect")
	if err != nil {
		t.Fatal(err)
	}
	args := []byte(`{"query":"rates"}`)
	r.r = execution.Record{Key: execution.Key{TenantID: "t", RequestID: "r", ToolCallID: "op"}, ToolID: "query", ToolVersion: 1, ArgsDigest: taskflow.Digest(args), State: execution.Running}
	if err = s.BeforeTool(ctx, r.r, args, &taskflow.ResultCheck{NonEmpty: true}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ValidateCompletion(ctx); !errors.Is(err, taskflow.ErrIncomplete) {
		t.Fatal(err)
	}
	// External effect and encrypted tool result are now committed; the old
	// node dies without updating the task checkpoint.
	r.b = []byte(`{"rates":[3,5],"receipt":"R1"}`)
	r.r.State = execution.Succeeded
	r.r.ResultRef = "tool-result://sha256/" + taskflow.Digest(r.b)
	newScope := s.Scope
	newScope.Fence = 8
	if _, err = store.Open(ctx, newScope, taskflow.Digest([]byte("original input")), 1); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Recover(ctx); !errors.Is(err, runtime.ErrStaleFence) {
		t.Fatalf("old owner: %v", err)
	}
	next := &taskflow.Session{Store: store, Tools: r, Results: r, Scope: newScope}
	p, err = next.Recover(ctx)
	if err != nil || p.Steps[0].State != "succeeded" {
		t.Fatalf("recover: %+v %v", p, err)
	}
	text, err := next.ResumeInstruction(ctx)
	if err != nil || !strings.Contains(text, "R1") || !strings.Contains(text, "rates") {
		t.Fatalf("resume lacks evidence: %s %v", text, err)
	}
	p, err = next.Start(ctx, p.Version, "analyze")
	if err != nil {
		t.Fatal(err)
	}
	p, err = next.CompleteAnalysis(ctx, p.Version, "analyze", "Average rate = (3+5)/2 = 4; source R1.")
	if err != nil {
		t.Fatal(err)
	}
	if err = next.Seal(ctx, "result://answer"); err != nil {
		t.Fatal(err)
	}
	if err = next.Seal(ctx, "result://answer"); err != nil {
		t.Fatal("seal must replay", err)
	}
	if err = next.Seal(ctx, "result://changed"); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatal(err)
	}
	if _, err = next.Define(ctx, p.Version, []taskflow.Step{{ID: "extra", Title: "extra", Criterion: "x", Kind: "analysis"}}); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatal(err)
	}
	if len(store.History(newScope.Key)) < 5 {
		t.Fatal("missing checkpoint history")
	}
}
func TestPlanCannotForgeReceiptsRemoveObligationsOrChangeToolArguments(t *testing.T) {
	ctx := context.Background()
	s, _, r := session(t)
	p, err := s.Define(ctx, 1, []taskflow.Step{{ID: "write", Title: "write", Criterion: "durable receipt", Kind: "tool", ToolID: "write", ToolVersion: 1, State: "succeeded", ResultRef: "fake", Note: "done", Check: &taskflow.ResultCheck{NonEmpty: true}}})
	if err != nil || p.Steps[0].State != "pending" || p.Steps[0].ResultRef != "" {
		t.Fatal(p, err)
	}
	if _, err = s.Define(ctx, p.Version, []taskflow.Step{{ID: "write", Title: "replace", Criterion: "skip", Kind: "analysis"}}); err == nil {
		t.Fatal("replaced obligation")
	}
	if _, err = s.Define(ctx, p.Version, []taskflow.Step{{ID: "future", Title: "future", Criterion: "x", Kind: "analysis", DependsOn: []string{"missing"}}}); err == nil {
		t.Fatal("undefined dependency")
	}
	p, err = s.Start(ctx, p.Version, "write")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompleteAnalysis(ctx, p.Version, "write", "I say the write succeeded"); err == nil {
		t.Fatal("self-reported effect accepted")
	}
	args := []byte(`{}`)
	r.r = execution.Record{Key: execution.Key{TenantID: "t", RequestID: "r", ToolCallID: "op"}, ToolID: "write", ToolVersion: 1, ArgsDigest: taskflow.Digest(args), State: execution.EffectUnknown}
	if err = s.BeforeTool(ctx, r.r, args, &taskflow.ResultCheck{NonEmpty: true}); err != nil {
		t.Fatal(err)
	}
	changed := r.r
	changed.ToolCallID = "new-op"
	if err = s.BeforeTool(ctx, changed, args, &taskflow.ResultCheck{NonEmpty: true}); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatal(err)
	}
	if _, err = s.ValidateCompletion(ctx); !errors.Is(err, taskflow.ErrIncomplete) {
		t.Fatal(err)
	}
	r.r.State = execution.Succeeded
	r.b = []byte(`"real"`)
	r.r.ResultRef = "fake"
	if _, err = s.Recover(ctx); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatal("invalid receipt accepted", err)
	}
}
func TestTaskScopeVersionAndLeaseAreChecked(t *testing.T) {
	ctx := context.Background()
	s, store, _ := session(t)
	p, err := s.Define(ctx, 1, []taskflow.Step{{ID: "a", Title: "a", Criterion: "saved notes", Kind: "analysis"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Start(ctx, 1, "a"); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatal(err)
	}
	if _, err = store.Get(ctx, taskflow.Key{TenantID: "other", RequestID: "r"}); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatal(err)
	}
	s.BeforeWrite = func(context.Context) error { return runtime.ErrLeaseLost }
	if _, err = s.Start(ctx, p.Version, "a"); !errors.Is(err, runtime.ErrLeaseLost) {
		t.Fatal(err)
	}
	if _, err = (taskflow.Tool{}).Call(ctx, []byte(`{"action":"inspect"}`)); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatal(err)
	}
}

func TestEvidencePredicateRejectsBusinessFailureAndReadsLargeResults(t *testing.T) {
	ctx := context.Background()
	s, _, r := session(t)
	p, err := s.Define(ctx, 1, []taskflow.Step{{ID: "write", Title: "write", Criterion: "provider reports created", Kind: "tool", ToolID: "write", ToolVersion: 1, Check: &taskflow.ResultCheck{Path: []string{"status"}, EqualsJSON: `"created"`}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Start(ctx, p.Version, "write"); err != nil {
		t.Fatal(err)
	}
	args := []byte(`{}`)
	r.r = execution.Record{Key: execution.Key{TenantID: "t", RequestID: "r", ToolCallID: "op"}, ToolID: "write", ToolVersion: 1, ArgsDigest: taskflow.Digest(args), State: execution.Running}
	if err = s.BeforeTool(ctx, r.r, args, &taskflow.ResultCheck{NonEmpty: true}); err != nil {
		t.Fatal(err)
	}
	r.r.State = execution.Succeeded
	r.b = []byte(`{"status":"failed"}`)
	r.r.ResultRef = "tool-result://sha256/" + taskflow.Digest(r.b)
	if _, err = s.ValidateCompletion(ctx); !errors.Is(err, taskflow.ErrEvidenceRejected) {
		t.Fatalf("business failure accepted: %v", err)
	}
	r.b = []byte(`{"status":"created","body":"` + strings.Repeat("汉", 4000) + `"}`)
	r.r.ResultRef = "tool-result://sha256/" + taskflow.Digest(r.b)
	if _, err = s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	var content strings.Builder
	for offset := 0; offset < len(r.b); {
		page, err := s.ReadResult(ctx, "write", offset)
		if err != nil {
			t.Fatal(err)
		}
		v := page.(map[string]any)
		content.WriteString(v["content"].(string))
		offset = v["next_offset"].(int)
	}
	if content.String() != string(r.b) {
		t.Fatal("paginated result lost UTF-8 content")
	}
}
