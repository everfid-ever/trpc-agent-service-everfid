package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
)

func resultAAD(key execution.Key, digest string, version int64) []byte {
	data, _ := json.Marshal([]any{"tool-execution-result-v1", key.TenantID, key.RequestID, key.ToolCallID, digest, version})
	return data
}
func (s *Store) aead(ctx context.Context, tenant string, version int64) (cipher.AEAD, error) {
	if s.keys == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	key, err := s.keys.ResolvePayloadKey(ctx, tenant, version)
	if err != nil {
		return nil, err
	}
	defer clear(key.Bytes)
	if key.Version != version || len(key.Bytes) != 32 {
		return nil, runtime.ErrVersionMismatch
	}
	block, err := aes.NewCipher(key.Bytes)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
func (s *Store) Complete(ctx context.Context, in execution.Record, content []byte, version int64) (execution.Record, error) {
	if len(content) == 0 || version < 1 || in.Fence == 0 || in.LeaseOwner == "" {
		return in, runtime.ErrInvalidEnvelope
	}
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])
	ref := "tool-result://sha256/" + digest
	aead, err := s.aead(ctx, in.TenantID, version)
	if err != nil {
		return in, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return in, err
	}
	encrypted := aead.Seal(nil, nonce, content, resultAAD(in.Key, digest, version))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return in, err
	}
	defer tx.Rollback()
	var fence int64
	err = tx.QueryRowContext(ctx, `UPDATE tool_execution SET state='succeeded',result_ref=$6,last_error=NULL,lease_owner=NULL,lease_until=NULL,updated_at=now()
WHERE tenant_id=$1 AND request_id=$2 AND tool_call_id=$3 AND state='running' AND lease_owner=$4 AND fence=$5 AND lease_until>now() RETURNING fence`, in.TenantID, in.RequestID, in.ToolCallID, in.LeaseOwner, int64(in.Fence), ref).Scan(&fence)
	if errors.Is(err, sql.ErrNoRows) {
		return in, runtime.ErrLeaseLost
	}
	if err != nil {
		return in, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO tool_execution_result(tenant_id,request_id,tool_call_id,result_ref,content_digest,ciphertext,nonce,key_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, in.TenantID, in.RequestID, in.ToolCallID, ref, digest, encrypted, nonce, version)
	if err != nil {
		return in, err
	}
	if err = tx.Commit(); err != nil {
		return in, err
	}
	in.State, in.ResultRef, in.LeaseOwner = execution.Succeeded, ref, ""
	return in, nil
}
func (s *Store) LoadResult(ctx context.Context, key execution.Key) ([]byte, error) {
	var digest string
	var encrypted, nonce []byte
	var version int64
	err := s.db.QueryRowContext(ctx, `SELECT r.content_digest,r.ciphertext,r.nonce,r.key_version FROM tool_execution_result r JOIN tool_execution e USING(tenant_id,request_id,tool_call_id) WHERE r.tenant_id=$1 AND r.request_id=$2 AND r.tool_call_id=$3 AND e.state='succeeded' AND e.result_ref=r.result_ref`, key.TenantID, key.RequestID, key.ToolCallID).Scan(&digest, &encrypted, &nonce, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, runtime.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	aead, err := s.aead(ctx, key.TenantID, version)
	if err != nil {
		return nil, err
	}
	if len(nonce) != aead.NonceSize() {
		return nil, runtime.ErrInvariantViolation
	}
	content, err := aead.Open(nil, nonce, encrypted, resultAAD(key, digest, version))
	if err != nil {
		return nil, runtime.ErrInvariantViolation
	}
	sum := sha256.Sum256(content)
	if hex.EncodeToString(sum[:]) != digest {
		clear(content)
		return nil, runtime.ErrVersionMismatch
	}
	return content, nil
}
