package postgres

import (
	"context"
	"database/sql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
	"time"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db} }
func (s *Store) Get(ctx context.Context, k execution.Key) (r execution.Record, e error) {
	if s == nil || s.db == nil {
		return r, runtime.ErrCapabilityUnsupported
	}
	var fence int64
	e = s.db.QueryRowContext(ctx, `SELECT tool_id,tool_version,args_digest,recovery_policy,idempotency_key,state,attempt,fence,COALESCE(lease_owner,''),COALESCE(lease_until,'epoch'),COALESCE(external_handle,''),COALESCE(result_ref,''),COALESCE(last_error,'') FROM tool_execution WHERE tenant_id=$1 AND request_id=$2 AND tool_call_id=$3`, k.TenantID, k.RequestID, k.ToolCallID).Scan(&r.ToolID, &r.ToolVersion, &r.ArgsDigest, &r.Policy, &r.IdempotencyKey, &r.State, &r.Attempt, &fence, &r.LeaseOwner, &r.LeaseUntil, &r.ExternalHandle, &r.ResultRef, &r.LastError)
	r.Key = k
	r.Fence = uint64(fence)
	if e == sql.ErrNoRows {
		e = runtime.ErrNotFound
	}
	return
}

func (s *Store) Claim(ctx context.Context, in execution.Record, claim execution.Claim) (execution.Record, bool, error) {
	if s == nil || s.db == nil || in.TenantID == "" || in.RequestID == "" || in.ToolCallID == "" || in.ToolID == "" || in.ToolVersion < 1 || in.ArgsDigest == "" || in.IdempotencyKey == "" || claim.Owner == "" || claim.TTL <= 0 {
		return execution.Record{}, false, runtime.ErrInvariantViolation
	}
	var r execution.Record
	var fence int64
	var lease sql.NullTime
	err := s.db.QueryRowContext(ctx, `INSERT INTO tool_execution(tenant_id,request_id,tool_call_id,tool_id,tool_version,args_digest,recovery_policy,idempotency_key,state,attempt,fence,lease_owner,lease_until)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,'running',1,1,$9,now()+$10::interval)
ON CONFLICT (tenant_id,request_id,tool_call_id) DO UPDATE SET state='running',attempt=tool_execution.attempt+1,fence=tool_execution.fence+1,lease_owner=EXCLUDED.lease_owner,lease_until=EXCLUDED.lease_until,updated_at=now()
WHERE tool_execution.state='pending' OR (tool_execution.state='running' AND tool_execution.lease_until<now())
RETURNING tenant_id,request_id,tool_call_id,tool_id,tool_version,args_digest,recovery_policy,idempotency_key,state,attempt,fence,lease_owner,lease_until,COALESCE(external_handle,''),COALESCE(result_ref,''),COALESCE(last_error,'')`,
		in.TenantID, in.RequestID, in.ToolCallID, in.ToolID, in.ToolVersion, in.ArgsDigest, in.Policy, in.IdempotencyKey, claim.Owner, claim.TTL.String()).Scan(&r.TenantID, &r.RequestID, &r.ToolCallID, &r.ToolID, &r.ToolVersion, &r.ArgsDigest, &r.Policy, &r.IdempotencyKey, &r.State, &r.Attempt, &fence, &r.LeaseOwner, &lease, &r.ExternalHandle, &r.ResultRef, &r.LastError)
	if err == sql.ErrNoRows {
		return in, false, nil
	}
	if err != nil {
		return execution.Record{}, false, err
	}
	r.Fence = uint64(fence)
	if lease.Valid {
		r.LeaseUntil = lease.Time
	}
	return r, true, nil
}

func (s *Store) Renew(ctx context.Context, in execution.Record, ttl time.Duration) (execution.Record, error) {
	if s == nil || s.db == nil || in.LeaseOwner == "" || in.Fence == 0 || ttl <= 0 {
		return execution.Record{}, runtime.ErrInvariantViolation
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tool_execution SET lease_until=now()+$1::interval,updated_at=now() WHERE tenant_id=$2 AND request_id=$3 AND tool_call_id=$4 AND state='running' AND lease_owner=$5 AND fence=$6`, ttl.String(), in.TenantID, in.RequestID, in.ToolCallID, in.LeaseOwner, int64(in.Fence))
	if err != nil {
		return execution.Record{}, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return execution.Record{}, runtime.ErrVersionConflict
	}
	return s.Get(ctx, in.Key)
}

func (s *Store) Finish(ctx context.Context, in execution.Record, state execution.State, resultRef, lastError string) (execution.Record, error) {
	if state != execution.Succeeded && state != execution.Failed && state != execution.EffectUnknown {
		return execution.Record{}, runtime.ErrInvariantViolation
	}
	if in.LeaseOwner == "" || in.Fence == 0 {
		return execution.Record{}, runtime.ErrInvariantViolation
	}
	res, err := s.db.ExecContext(ctx, `UPDATE tool_execution SET state=$1,result_ref=NULLIF($2,''),last_error=NULLIF($3,''),lease_owner=NULL,lease_until=NULL,updated_at=now() WHERE tenant_id=$4 AND request_id=$5 AND tool_call_id=$6 AND state='running' AND lease_owner=$7 AND fence=$8`, state, resultRef, lastError, in.TenantID, in.RequestID, in.ToolCallID, in.LeaseOwner, int64(in.Fence))
	if err != nil {
		return execution.Record{}, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return execution.Record{}, runtime.ErrVersionConflict
	}
	return s.Get(ctx, in.Key)
}
