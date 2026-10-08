package tool

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// RecoveryPolicy declares what a new owner may safely do after a tool lease
// expires while the previous owner was executing an external operation.
type RecoveryPolicy string

const (
	RecoveryReplaySafe    RecoveryPolicy = "replay_safe"
	RecoveryIdempotentKey RecoveryPolicy = "idempotent_key"
	RecoveryQueryable     RecoveryPolicy = "queryable"
	RecoveryManual        RecoveryPolicy = "manual"
)

// RecoveryDescriptor is implemented by tools with an explicit takeover
// contract. The key is stable for one tenant/request/tool-call/arguments
// tuple and is safe to pass to an external provider as its idempotency key.
type RecoveryDescriptor interface {
	RecoveryPolicy() RecoveryPolicy
	RecoveryKey(context.Context) (string, error)
}

// RecoveryQuerier lets a queryable tool resolve a previously accepted
// external operation by its stable idempotency key without replaying it.
type RecoveryQuerier interface {
	RecoveryDescriptor
	QueryRecovery(context.Context, string) (result any, completed bool, err error)
}

// RecoveryContext exposes only the durable execution identity to a tool.
func RecoveryContext(ctx context.Context) (runtime.ExecutionContext, bool) {
	return runtime.ExecutionContextFrom(ctx)
}
