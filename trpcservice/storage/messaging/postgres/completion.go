package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func completionAAD(r messaging.CompletionRecord) []byte {
	b, _ := json.Marshal([]any{"execution-completion-v1", r.TenantID, r.RequestID, r.EnvelopeDigest, r.ContentDigest, r.KeyVersion})
	return b
}
func (s *Store) PutCompletion(ctx context.Context, r messaging.CompletionRecord) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Fence > math.MaxInt64 {
		return runtime.ErrInvalidEnvelope
	}
	key, err := s.resolvePayloadKey(ctx, r.TenantID, r.KeyVersion)
	if err != nil {
		return err
	}
	defer clear(key)
	c, n, err := encryptPayload(key, completionAAD(r), r.Content)
	if err != nil {
		return err
	}
	var envelope, digest string
	var ciphertext, nonce []byte
	var version int64
	// Serialize against BeginExecutionAttempt's execution_record lock. A lease
	// check alone leaves a check/write race when a successor already took over.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var request string
	if err = tx.QueryRowContext(ctx, `SELECT request_id FROM execution_record WHERE tenant_id=$1 AND request_id=$2 FOR UPDATE`, r.TenantID, r.RequestID).Scan(&request); err != nil {
		return err
	}
	var maximum int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(max(fence),0) FROM execution_attempt WHERE tenant_id=$1 AND request_id=$2`, r.TenantID, r.RequestID).Scan(&maximum); err != nil {
		return err
	}
	if maximum > int64(r.Fence) {
		return runtime.ErrStaleFence
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO execution_completion(tenant_id,request_id,envelope_digest,content_digest,ciphertext,nonce,key_version,fence)
SELECT $1,$2,$3,$4,$5,$6,$7,$8 FROM execution_record e JOIN session_head h USING(tenant_id,agent_app_id,session_id)
WHERE e.tenant_id=$1 AND e.request_id=$2 AND e.input_seq=h.next_input_seq AND h.last_fence<=$8
ON CONFLICT(tenant_id,request_id) DO UPDATE SET request_id=EXCLUDED.request_id
RETURNING envelope_digest,content_digest,ciphertext,nonce,key_version`, r.TenantID, r.RequestID, r.EnvelopeDigest, r.ContentDigest, c, n, r.KeyVersion, int64(r.Fence)).Scan(&envelope, &digest, &ciphertext, &nonce, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrStaleFence
	}
	if err != nil {
		return err
	}
	if envelope != r.EnvelopeDigest || digest != r.ContentDigest || version != r.KeyVersion {
		return runtime.ErrIdempotencyCollision
	}
	content, err := decryptPayload(key, completionAAD(r), ciphertext, nonce)
	defer clear(content)
	if err != nil || !bytes.Equal(content, r.Content) {
		return runtime.ErrIdempotencyCollision
	}
	return tx.Commit()
}
func (s *Store) GetCompletion(ctx context.Context, tenant, request string) (messaging.CompletionRecord, error) {
	r := messaging.CompletionRecord{TenantID: tenant, RequestID: request}
	if tenant == "" || request == "" {
		return r, runtime.ErrTenantScope
	}
	var c, n []byte
	var fence int64
	err := s.db.QueryRowContext(ctx, `SELECT envelope_digest,content_digest,ciphertext,nonce,key_version,fence FROM execution_completion WHERE tenant_id=$1 AND request_id=$2`, tenant, request).Scan(&r.EnvelopeDigest, &r.ContentDigest, &c, &n, &r.KeyVersion, &fence)
	if errors.Is(err, sql.ErrNoRows) {
		return r, runtime.ErrNotFound
	}
	if err != nil {
		return r, err
	}
	r.Fence = uint64(fence)
	key, err := s.resolvePayloadKey(ctx, tenant, r.KeyVersion)
	if err != nil {
		return r, err
	}
	defer clear(key)
	r.Content, err = decryptPayload(key, completionAAD(r), c, n)
	if err != nil {
		return r, runtime.ErrInvariantViolation
	}
	return r, r.Validate()
}
