package execution

import (
	"context"
	"time"
)

type State string

const (
	Pending       State = "pending"
	Running       State = "running"
	Succeeded     State = "succeeded"
	Failed        State = "failed"
	EffectUnknown State = "effect_unknown"
)

type Key struct{ TenantID, RequestID, ToolCallID string }
type Record struct {
	Key
	ToolID                               string
	ToolVersion                          int64
	ArgsDigest                           string
	Policy                               string
	IdempotencyKey                       string
	State                                State
	Attempt                              int
	Fence                                uint64
	LeaseOwner                           string
	LeaseUntil                           time.Time
	ExternalHandle, ResultRef, LastError string
}
type Claim struct {
	Owner string
	TTL   time.Duration
}
type Store interface {
	Claim(context.Context, Record, Claim) (Record, bool, error)
	Renew(context.Context, Record, time.Duration) (Record, error)
	Finish(context.Context, Record, State, string, string) (Record, error)
	Get(context.Context, Key) (Record, error)
}
