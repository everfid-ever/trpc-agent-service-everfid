package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func (s *TaskStore) BeginExecutionAttempt(ctx context.Context, key gateway.ExecutionKey, worker string, fence uint64) (gateway.ExecutionAttempt, error) {
	a := gateway.ExecutionAttempt{ExecutionKey: key, WorkerID: worker, Fence: fence}
	if key.TenantID == "" || key.RequestID == "" || worker == "" || fence == 0 || fence > math.MaxInt64 {
		return a, runtime.ErrInvalidEnvelope
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	var request string
	if err = tx.QueryRowContext(ctx, `SELECT request_id FROM execution_record WHERE tenant_id=$1 AND request_id=$2 FOR UPDATE`, key.TenantID, key.RequestID).Scan(&request); err != nil {
		return a, err
	}
	var maximum int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(max(fence),0) FROM execution_attempt WHERE tenant_id=$1 AND request_id=$2`, key.TenantID, key.RequestID).Scan(&maximum); err != nil {
		return a, err
	}
	if uint64(maximum) > fence {
		return a, runtime.ErrStaleFence
	}
	if _, err = tx.ExecContext(ctx, `UPDATE execution_attempt SET state='superseded',finished_at=now() WHERE tenant_id=$1 AND request_id=$2 AND state='running'`, key.TenantID, key.RequestID); err != nil {
		return a, err
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO execution_attempt(tenant_id,request_id,attempt_no,worker_id,fence,state)
SELECT $1,$2,COALESCE(max(attempt_no),0)+1,$3,$4,'running' FROM execution_attempt WHERE tenant_id=$1 AND request_id=$2 RETURNING attempt_no`, key.TenantID, key.RequestID, worker, int64(fence)).Scan(&a.Number)
	if err != nil {
		return a, err
	}
	return a, tx.Commit()
}
func (s *TaskStore) FinishExecutionAttempt(ctx context.Context, a gateway.ExecutionAttempt, completed bool) error {
	state := "retryable"
	if completed {
		state = "completed"
	}
	var version int64
	err := s.db.QueryRowContext(ctx, `UPDATE execution_attempt SET state=$6,finished_at=now() WHERE tenant_id=$1 AND request_id=$2 AND attempt_no=$3 AND worker_id=$4 AND fence=$5 AND state='running' RETURNING attempt_no`, a.TenantID, a.RequestID, a.Number, a.WorkerID, int64(a.Fence), state).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrVersionConflict
	}
	return err
}

func (s *TaskStore) RecordModelRequest(ctx context.Context, key gateway.ExecutionKey) error {
	if key.TenantID == "" || key.RequestID == "" {
		return runtime.ErrTenantScope
	}
	result, err := s.db.ExecContext(ctx, `UPDATE execution_record SET model_request_count=model_request_count+1 WHERE tenant_id=$1 AND request_id=$2`, key.TenantID, key.RequestID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return runtime.ErrNotFound
	}
	return nil
}
