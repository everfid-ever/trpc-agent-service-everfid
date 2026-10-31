package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	upstreammcp "trpc.group/trpc-go/trpc-agent-go/tool/mcp"
)

type recoveryRemote struct {
	declaration *agenttool.Declaration
	args        [][]byte
	result      any
}

func (r *recoveryRemote) Declaration() *agenttool.Declaration { return r.declaration }
func (r *recoveryRemote) Call(_ context.Context, b []byte) (any, error) {
	r.args = append(r.args, append([]byte(nil), b...))
	return r.result, nil
}

type recoverySet struct{ remote *recoveryRemote }

func (*recoverySet) Init(context.Context) error               { return nil }
func (*recoverySet) Close() error                             { return nil }
func (s *recoverySet) Tools(context.Context) []agenttool.Tool { return []agenttool.Tool{s.remote} }

type mcpFailure struct{}

func (mcpFailure) RetryResultError() bool { return true }

func TestReviewedMCPIdempotencyInjectionAndErrorReceipts(t *testing.T) {
	r := &recoveryRemote{declaration: &agenttool.Declaration{Name: "create_record", InputSchema: &agenttool.Schema{Type: "object", Required: []string{"text", "operation_key"}, Properties: map[string]*agenttool.Schema{"text": {Type: "string"}, "operation_key": {Type: "string"}}}}, result: map[string]any{"status": "created"}}
	old := newUpstreamToolSet
	newUpstreamToolSet = func(upstreammcp.ConnectionConfig, ...upstreammcp.ToolSetOption) upstreamToolSet {
		return &recoverySet{remote: r}
	}
	t.Cleanup(func() { newUpstreamToolSet = old })
	digest, _ := DeclarationDigest(r.declaration)
	config := Config{Transport: "streamable", ServerURL: "https://mcp.example.com/tools", RemoteToolName: "create_record", ExpectedDeclarationDigest: digest, Timeout: time.Second, IdempotencyParameter: "operation_key", CompletionCheck: &taskflow.ResultCheck{Path: []string{"status"}, EqualsJSON: `"created"`}}
	reg, err := NewRegistration("tenant-a", "create_record", 1, config, secrets.SecretRef{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request", ToolCallID: "call", ArgsDigest: taskflow.Digest([]byte(`{"text":"hello"}`))})
	value, err := reg.Build(ctx, servicetool.BuildRequest{TenantID: "tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, exposed := value.Declaration().InputSchema.Properties["operation_key"]; exposed {
		t.Fatal("provider key exposed to model")
	}
	if r.declaration.InputSchema.Properties["operation_key"] == nil {
		t.Fatal("discovery mutated remote schema")
	}
	for i := 0; i < 2; i++ {
		if _, err = value.Call(ctx, []byte(`{"text":"hello"}`)); err != nil {
			t.Fatal(err)
		}
	}
	var first, second map[string]any
	_ = json.Unmarshal(r.args[0], &first)
	_ = json.Unmarshal(r.args[1], &second)
	if first["operation_key"] == "" || first["operation_key"] != second["operation_key"] {
		t.Fatal("unstable provider idempotency key")
	}
	if _, err = value.Call(ctx, []byte(`{"text":"hello","operation_key":"model-chosen"}`)); err == nil {
		t.Fatal("model key override accepted")
	}
	r.result = mcpFailure{}
	if _, err = value.Call(ctx, []byte(`{"text":"hello"}`)); err == nil {
		t.Fatal("MCP isError accepted as successful receipt")
	}
	other := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-b", RequestID: "request", ToolCallID: "call", ArgsDigest: "digest"})
	if _, err = value.Call(other, []byte(`{"text":"hello"}`)); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatal("cross-tenant provider key")
	}
	// Discovery must be pinned on EVERY connection, not only initial Build.
	r.declaration.Description = "unreviewed change"
	if _, err = value.Call(ctx, []byte(`{"text":"hello"}`)); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("changed remote accepted: %v", err)
	}
}

func TestMCPRecoveryContractsArePinnedAndNotInferred(t *testing.T) {
	c := Config{Transport: "streamable", ServerURL: "https://mcp.example.com/tools", RemoteToolName: "lookup", ExpectedDeclarationDigest: taskflow.Digest([]byte("decl")), Timeout: time.Second}
	base, err := BindingDigest(c, secrets.SecretRef{})
	if err != nil {
		t.Fatal(err)
	}
	if (callable{config: c}).RecoveryPolicy() != servicetool.RecoveryManual {
		t.Fatal("unknown remote granted replay")
	}
	c.ReadOnly = true
	read, _ := BindingDigest(c, secrets.SecretRef{})
	if read == base || (callable{config: c}).RecoveryPolicy() != servicetool.RecoveryReplaySafe {
		t.Fatal("read-only contract not pinned")
	}
	c.IdempotencyParameter = "key"
	if _, err = BindingDigest(c, secrets.SecretRef{}); err == nil {
		t.Fatal("conflicting contracts accepted")
	}
	c.ReadOnly = false
	if _, err = BindingDigest(c, secrets.SecretRef{}); err == nil {
		t.Fatal("write contract without trusted completion check accepted")
	}
}
