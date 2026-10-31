package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/inflight"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
	taskmemory "github.com/liuzengh/trpc-agent-service/trpcservice/taskflow/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sdkmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// The script goes through the real SDK tool surface, then deliberately tries
// to finish before the declared obligation has evidence.
type checkpointModel struct {
	calls     int
	recovered bool
}

func (m *checkpointModel) GenerateContent(_ context.Context, r *model.Request) (<-chan *model.Response, error) {
	m.calls++
	if _, ok := r.Tools[taskflow.ToolName]; !ok {
		return nil, errors.New("checkpoint tool missing from SDK surface")
	}
	if m.calls == 3 {
		for _, msg := range r.Messages {
			if strings.Contains(msg.Content, "Durable checkpoint") && strings.Contains(msg.Content, "pending") {
				m.recovered = true
			}
		}
	}
	commands := map[int]string{
		1: `{"action":"define","expected_version":1,"steps":[{"id":"analyze","title":"analyze","criterion":"saved calculation","kind":"analysis"}]}`,
		3: `{"action":"start","expected_version":2,"step_id":"analyze"}`,
		4: `{"action":"complete_analysis","expected_version":3,"step_id":"analyze","note":"2+3=5"}`,
	}
	msg := model.NewAssistantMessage("answer: 5")
	if command, ok := commands[m.calls]; ok {
		msg = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: fmt.Sprintf("checkpoint-%d", m.calls), Type: "function", Function: model.FunctionDefinitionParam{Name: taskflow.ToolName, Arguments: []byte(command)}}}}
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{ID: fmt.Sprintf("response-%d", m.calls), Done: true, Choices: []model.Choice{{Message: msg}}}
	close(ch)
	return ch, nil
}
func (*checkpointModel) Info() model.Info { return model.Info{Name: "checkpoint-model"} }

type unusedToolResults struct{ execution.Store }

func (unusedToolResults) LoadResult(context.Context, execution.Key) ([]byte, error) {
	return nil, runtime.ErrNotFound
}
func (unusedToolResults) Complete(context.Context, execution.Record, []byte, int64) (execution.Record, error) {
	return execution.Record{}, runtime.ErrCapabilityUnsupported
}

type crashBeforeTerminal struct{ hit bool }

type taskRunGuard struct {
	confirmationRunGuard
	finishes int
}

func (g *taskRunGuard) Finish(ctx context.Context, p governance.RunPermit, u governance.Usage, b []byte) (governance.Decision, error) {
	g.finishes++
	return g.confirmationRunGuard.Finish(ctx, p, u, b)
}

func (b *crashBeforeTerminal) Wait(_ context.Context, p inflight.Point) error {
	if p == inflight.PointResultPersistedBeforeCommit && !b.hit {
		b.hit = true
		return runtime.ErrLeaseLost
	}
	return nil
}

func TestRunnerTaskCompletionGateAndFrozenRecoveryUseOriginalPlan(t *testing.T) {
	e := runtime.ExecutionEnvelope{SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1, RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1, PayloadRef: "payload://request", CreatedAt: time.Now().UTC()}
	k := profile.ExecutionProfileKey{TenantID: e.TenantID, TenantVersion: 1, AgentAppID: e.AgentAppID, AgentAppVersion: 1, AgentAppRevision: 1, ContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1}
	snapshot := profile.ExecutionProfileSnapshot{Key: k, TenantVersion: 1, AgentAppVersion: 1, ContentDigest: "digest", AppName: "tenant-a/app", AgentKind: agentapp.AgentKindLLM, Instruction: "solve", ModelProfileRef: profile.VersionedRef{ID: "mock", Version: 1}}
	profiles := profilememory.NewResolver(snapshot)
	m := &checkpointModel{}
	factory := serviceagent.Factory{Profiles: profiles, Models: staticModelResolver{model: m}, TaskPlans: true}
	bundles := profilememory.NewBundleManager(func(ctx context.Context, _ profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		root, err := factory.Build(ctx, snapshot)
		return &serviceagent.Bundle{AppName: snapshot.AppName, Root: root}, nil, err
	})
	payloads := messagingmemory.New()
	if err := payloads.PutPayload(context.Background(), messaging.PayloadRecord{TenantID: e.TenantID, RequestID: e.RequestID, PayloadRef: e.PayloadRef, Content: []byte(`{"text":"calculate 2+3, save the analysis, then answer"}`), ContentDigest: "digest", KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	plans := taskmemory.New()
	sessions := sessionmemory.New()
	barrier := &crashBeforeTerminal{}
	w := RunnerExecutor{Tasks: taskStub{envelope: e}, Profiles: profiles, Bundles: bundles, Sessions: sessions, SDKSessions: sdkmemory.NewSessionService(), Payloads: payloads, Inputs: JSONTextInputDecoder{}, TaskPlans: plans, ToolExecutions: unusedToolResults{}, Barrier: barrier}
	guard := &taskRunGuard{confirmationRunGuard: confirmationRunGuard{policy: governance.PolicySnapshot{TenantID: e.TenantID, Version: 1, Policy: governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow}}}}
	w.Governance = guard
	if err := w.ExecuteWithLease(context.Background(), e, 1, nil); !errors.Is(err, taskflow.ErrIncomplete) {
		t.Fatalf("premature success allowed: %v", err)
	}
	if guard.finishes != 0 {
		t.Fatal("incomplete task settled its reservation")
	}
	if _, err := payloads.GetResult(context.Background(), e.TenantID, e.RequestID); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("premature frozen result: %v", err)
	}
	if err := w.ExecuteWithLease(context.Background(), e, 2, nil); !errors.Is(err, runtime.ErrLeaseLost) {
		t.Fatalf("crash boundary: %v", err)
	}
	if !m.recovered {
		t.Fatal("new run did not receive durable pending plan")
	}
	calls := m.calls
	if err := w.ExecuteWithLease(context.Background(), e, 3, nil); err != nil {
		t.Fatal(err)
	}
	if m.calls != calls {
		t.Fatal("frozen answer regenerated")
	}
	p, err := plans.Get(context.Background(), taskflow.Key{TenantID: e.TenantID, RequestID: e.RequestID})
	if err != nil || p.Phase != "sealed" || p.Steps[0].Note != "2+3=5" || p.Fence != 3 {
		t.Fatalf("plan: %+v %v", p, err)
	}
	terminal, err := sessions.GetTerminalByInputSeq(context.Background(), sessionstore.TerminalKey{SessionKey: sessionstore.SessionKey{TenantID: e.TenantID, AgentAppID: e.AgentAppID, SessionID: e.SessionID}, InputSeq: 1})
	if err != nil || terminal.Outcome != runtime.OutcomeSucceeded || terminal.ResultRef != p.FinalRef {
		t.Fatal(terminal, err)
	}
}
