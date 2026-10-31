package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancememory "github.com/liuzengh/trpc-agent-service/trpcservice/governance/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/inflight"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	messagingmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	sdkmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type completionGuard struct {
	calls   int
	verdict governance.DLPVerdict
}

func (g *completionGuard) Inspect(context.Context, string, string, string, []byte) (governance.DLPVerdict, string, error) {
	g.calls++
	return g.verdict, "scan-v1", nil
}

type crashAfterSettlement struct{}

func (crashAfterSettlement) Wait(_ context.Context, p inflight.Point) error {
	if p == inflight.PointP2BeforeTerminalCommit {
		return runtime.ErrLeaseLost
	}
	return nil
}

func TestCompletionCandidateRecoversAcrossSettlementWithoutNewRun(t *testing.T) {
	for _, window := range []string{"before_settlement", "after_settlement", "stale_owner", "output_denied"} {
		t.Run(window, func(t *testing.T) {
			ctx := context.Background()
			e := runtime.ExecutionEnvelope{SchemaVersion: 1, TenantID: "tenant-a", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1, AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1, RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1, PayloadRef: "payload://request", CreatedAt: time.Now().UTC()}
			k := profile.ExecutionProfileKey{TenantID: e.TenantID, TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1, AgentAppRevision: 1, ContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1}
			profiles := profilememory.NewResolver(profile.ExecutionProfileSnapshot{Key: k, TenantVersion: 1, AgentAppVersion: 1, ContentDigest: "digest", AppName: "tenant-a/app", AgentKind: agentapp.AgentKindLLM, ModelProfileRef: profile.VersionedRef{ID: "model", Version: 1}})
			bundles := profilememory.NewBundleManager(func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
				t.Fatal("recovery entered model path")
				return nil, nil, runtime.ErrCapabilityUnsupported
			})
			payloads := messagingmemory.New()
			payload := messaging.PayloadRecord{TenantID: e.TenantID, RequestID: e.RequestID, PayloadRef: e.PayloadRef, Content: []byte(`{"text":"hello"}`), ContentDigest: "digest", KeyVersion: 1}
			if err := payloads.PutPayload(ctx, payload); err != nil {
				t.Fatal(err)
			}
			ledger := governancememory.New(0, 1000)
			policy := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow, AllowedModels: []governance.VersionedRef{{ID: "model", Version: 1}}, InputDLP: governance.DLPDisabled, OutputDLP: governance.DLPRequired, Budget: governance.BudgetPolicy{MaxInputTokens: 100, MaxOutputTokens: 100}}
			digest, _, _ := governance.PolicyDigest(policy)
			if err := ledger.PublishPolicy(governance.PolicySnapshot{TenantID: e.TenantID, Version: 1, SchemaVersion: 1, Policy: policy, ContentDigest: digest, PublishedAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
			guard := &completionGuard{verdict: governance.DLPVerdictClean}
			if window == "output_denied" {
				guard.verdict = governance.DLPVerdictRejected
			}
			service := governance.Service{Repository: ledger, Ledger: ledger, Decisions: ledger, OutputGuard: guard}
			permit, err := service.Begin(ctx, e, governance.VersionedRef{ID: "model", Version: 1}, payload.Content)
			if err != nil || permit.Decision.Action != governance.ActionAllow {
				t.Fatalf("permit=%+v err=%v", permit, err)
			}
			sessions := sessionmemory.New()
			w := RunnerExecutor{Tasks: taskStub{envelope: e}, Profiles: profiles, Bundles: bundles, Sessions: sessions, SDKSessions: sdkmemory.NewSessionService(), Payloads: payloads, Inputs: JSONTextInputDecoder{}, Governance: service}
			var qualify func(context.Context) error
			if window == "stale_owner" {
				qualify = func(context.Context) error { return runtime.ErrLeaseLost }
			}
			r, err := w.saveCompletion(ctx, e, 1, qualify, payload, permit, runnerResult{Usage: governance.Usage{InputTokens: 10, OutputTokens: 5}}, OutboundContent{Content: []byte("original answer"), ContentType: messaging.ContentTypeText})
			if window == "stale_owner" {
				if !errors.Is(err, runtime.ErrLeaseLost) {
					t.Fatal(err)
				}
				if _, err = payloads.GetCompletion(ctx, e.TenantID, e.RequestID); !errors.Is(err, runtime.ErrNotFound) {
					t.Fatal("stale candidate saved")
				}
				reservation, _ := ledger.GetReservation(ctx, e.TenantID, permit.Reservation.ReservationID)
				if reservation.State != governance.ReservationReserved {
					t.Fatal("stale owner settled")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if window == "after_settlement" {
				head, err := sessions.OpenForRun(ctx, sessionstore.OpenForRunRequest{SessionKey: sessionstore.SessionKey{TenantID: e.TenantID, AgentAppID: e.AgentAppID, SessionID: e.SessionID}, RequestID: e.RequestID, InputSeq: 1, Fence: 1})
				if err != nil {
					t.Fatal(err)
				}
				w.Barrier = crashAfterSettlement{}
				if err = w.resumeCompletion(ctx, nil, e, head, 1, nil, payload, r); !errors.Is(err, runtime.ErrLeaseLost) {
					t.Fatal(err)
				}
				reservation, _ := ledger.GetReservation(ctx, e.TenantID, permit.Reservation.ReservationID)
				if reservation.State != governance.ReservationSettled {
					t.Fatal("did not hit settled window")
				}
			}
			if _, err = payloads.GetResult(ctx, e.TenantID, e.RequestID); !errors.Is(err, runtime.ErrNotFound) {
				t.Fatal("public result appeared before approved promotion")
			}
			// A takeover must use the original reviewed decision, not scan again.
			guard.verdict = governance.DLPVerdictRejected
			w.Barrier = nil
			if err = w.ExecuteWithLease(ctx, e, 2, nil); err != nil {
				t.Fatal(err)
			}
			if guard.calls != 1 {
				t.Fatalf("output reviewed %d times", guard.calls)
			}
			result, err := payloads.GetResult(ctx, e.TenantID, e.RequestID)
			if window == "output_denied" {
				if !errors.Is(err, runtime.ErrNotFound) {
					t.Fatal("denied candidate leaked into public result")
				}
				return
			}
			if err != nil || string(result.Content) != "original answer" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			terminal, err := sessions.GetTerminalByInputSeq(ctx, sessionstore.TerminalKey{SessionKey: sessionstore.SessionKey{TenantID: e.TenantID, AgentAppID: e.AgentAppID, SessionID: e.SessionID}, InputSeq: 1})
			if err != nil || terminal.Outcome != runtime.OutcomeSucceeded {
				t.Fatalf("terminal=%+v err=%v", terminal, err)
			}
		})
	}
}
