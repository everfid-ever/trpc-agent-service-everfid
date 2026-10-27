// Package coordination defines session ownership and fencing contracts.
package coordination

import (
	"context"
	"time"
)

type SessionKey struct{ TenantID, AgentAppID, SessionID string }
type Lease struct {
	Session   SessionKey
	WorkerID  string
	LeaseID   string
	Fence     uint64
	ExpiresAt time.Time
}

type LeaseManager interface {
	Acquire(context.Context, SessionKey, string, time.Duration) (Lease, error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
	EnsureFenceAtLeast(context.Context, SessionKey, uint64) error
}

// LeaseObservation deliberately omits LeaseID: inspection must not hand out
// the capability used by the lease owner to renew or release execution rights.
type LeaseObservation struct {
	Session         SessionKey `json:"session"`
	WorkerID        string     `json:"worker_id,omitempty"`
	Fence           uint64     `json:"fence"`
	Held            bool       `json:"held"`
	RemainingMillis int64      `json:"remaining_ms"`
	ObservedAt      time.Time  `json:"observed_at"`
}

type LeaseInspector interface {
	Inspect(context.Context, SessionKey) (LeaseObservation, error)
}
