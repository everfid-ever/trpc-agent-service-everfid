package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"math"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
)

type Store struct {
	DB   *sql.DB
	Keys messaging.PayloadKeyResolver
}

func New(db *sql.DB, keys messaging.PayloadKeyResolver) *Store { return &Store{DB: db, Keys: keys} }
func (s *Store) aead(ctx context.Context, k taskflow.Key, v int64) (cipher.AEAD, error) {
	if s.Keys == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	key, err := s.Keys.ResolvePayloadKey(ctx, k.TenantID, v)
	if err != nil {
		return nil, err
	}
	defer clear(key.Bytes)
	if key.Version != v || len(key.Bytes) != 32 {
		return nil, runtime.ErrVersionMismatch
	}
	b, err := aes.NewCipher(key.Bytes)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}
func aad(p taskflow.Plan) []byte {
	b, _ := json.Marshal([]any{"task-plan-v1", p.TenantID, p.RequestID, p.InputDigest, p.Version, p.KeyVersion})
	return b
}
func (s *Store) encrypt(ctx context.Context, p taskflow.Plan) ([]byte, []byte, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, nil, err
	}
	defer clear(b)
	if len(b) > 256<<10 {
		return nil, nil, runtime.ErrInvalidEnvelope
	}
	a, err := s.aead(ctx, p.Key, p.KeyVersion)
	if err != nil {
		return nil, nil, err
	}
	n := make([]byte, a.NonceSize())
	if _, err = rand.Read(n); err != nil {
		return nil, nil, err
	}
	return a.Seal(nil, n, b, aad(p)), n, nil
}

type scanner interface{ Scan(...any) error }

func (s *Store) decode(ctx context.Context, k taskflow.Key, row scanner) (taskflow.Plan, error) {
	p := taskflow.Plan{Scope: taskflow.Scope{Key: k}}
	var encrypted, nonce []byte
	var fence int64
	var phase, ref string
	err := row.Scan(&fence, &p.Version, &p.InputDigest, &p.KeyVersion, &phase, &ref, &encrypted, &nonce)
	if errors.Is(err, sql.ErrNoRows) {
		return p, runtime.ErrNotFound
	}
	if err != nil {
		return p, err
	}
	p.Fence = uint64(fence)
	a, err := s.aead(ctx, k, p.KeyVersion)
	if err != nil {
		return p, err
	}
	if len(nonce) != a.NonceSize() {
		return p, runtime.ErrInvariantViolation
	}
	b, err := a.Open(nil, nonce, encrypted, aad(p))
	if err != nil {
		return p, runtime.ErrInvariantViolation
	}
	defer clear(b)
	var stored taskflow.Plan
	if err = json.Unmarshal(b, &stored); err != nil {
		return p, runtime.ErrInvariantViolation
	}
	if stored.Version != p.Version || stored.InputDigest != p.InputDigest || stored.Phase != phase || stored.FinalRef != ref {
		return p, runtime.ErrVersionMismatch
	}
	stored.Scope, stored.KeyVersion = p.Scope, p.KeyVersion
	return stored, nil
}

const columns = "fence,version,input_digest,key_version,phase,COALESCE(final_ref,''),ciphertext,nonce"

func (s *Store) Get(ctx context.Context, k taskflow.Key) (taskflow.Plan, error) {
	if k.TenantID == "" || k.RequestID == "" {
		return taskflow.Plan{}, runtime.ErrTenantScope
	}
	return s.decode(ctx, k, s.DB.QueryRowContext(ctx, "SELECT "+columns+" FROM task_plan WHERE tenant_id=$1 AND request_id=$2", k.TenantID, k.RequestID))
}
func (s *Store) Open(ctx context.Context, scope taskflow.Scope, digest string, keyVersion int64) (taskflow.Plan, error) {
	if scope.TenantID == "" || scope.RequestID == "" || scope.Fence == 0 || scope.Fence > math.MaxInt64 || len(digest) != 64 || keyVersion < 1 {
		return taskflow.Plan{}, runtime.ErrInvalidEnvelope
	}
	p := taskflow.Plan{Scope: scope, Version: 1, InputDigest: digest, KeyVersion: keyVersion, Phase: "open", Steps: []taskflow.Step{}, Contract: taskflow.DefaultContract()}
	encrypted, nonce, err := s.encrypt(ctx, p)
	if err != nil {
		return p, err
	}
	// The input must still own the session's current input gate. A new runner
	// advances the plan fence immediately, before any model/tool invocation.
	row := s.DB.QueryRowContext(ctx, `INSERT INTO task_plan(tenant_id,request_id,fence,version,input_digest,key_version,phase,ciphertext,nonce,admission_version,checkpoint_version)
SELECT $1,$2,$3,1,$4,$5,'open',$6,$7,1,1 FROM execution_record e JOIN session_head h USING(tenant_id,agent_app_id,session_id)
WHERE e.tenant_id=$1 AND e.request_id=$2 AND e.input_seq=h.next_input_seq AND h.last_fence<=$3
ON CONFLICT (tenant_id,request_id) DO UPDATE SET fence=EXCLUDED.fence,updated_at=now()
WHERE task_plan.fence<=EXCLUDED.fence AND task_plan.input_digest=EXCLUDED.input_digest AND task_plan.key_version=EXCLUDED.key_version
RETURNING `+columns, scope.TenantID, scope.RequestID, int64(scope.Fence), digest, keyVersion, encrypted, nonce)
	p, err = s.decode(ctx, scope.Key, row)
	if errors.Is(err, runtime.ErrNotFound) {
		err = runtime.ErrStaleFence
	}
	return p, err
}
func (s *Store) Save(ctx context.Context, scope taskflow.Scope, expected int64, p taskflow.Plan) (taskflow.Plan, error) {
	if p.Key != scope.Key || p.Fence != scope.Fence || expected < 1 || expected == math.MaxInt64 || scope.Fence == 0 || scope.Fence > math.MaxInt64 || p.Version != expected || (p.Phase != "open" && p.Phase != "sealed" && p.Phase != "failed") || (p.Phase == "sealed" && p.FinalRef == "") || (p.Phase == "failed" && (p.FailureReason == "" || p.FinalRef != "")) {
		return p, runtime.ErrInvalidEnvelope
	}
	p.Version = expected + 1
	verified := 0
	if p.Phase == "sealed" {
		for _, step := range p.Steps {
			if step.State != "succeeded" {
				return p, runtime.ErrInvariantViolation
			}
			verified++
		}
		if p.Contract.RequirePlan && verified == 0 {
			return p, taskflow.ErrIncomplete
		}
	}
	encrypted, nonce, err := s.encrypt(ctx, p)
	if err != nil {
		return p, err
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return p, err
	}
	defer tx.Rollback()
	var version int64
	err = tx.QueryRowContext(ctx, `UPDATE task_plan SET version=$5,phase=$6,final_ref=NULLIF($7,''),ciphertext=$8,nonce=$9,updated_at=now(),
admission_version=GREATEST(admission_version,$12),checkpoint_version=$5,verified_steps=$13
WHERE tenant_id=$1 AND request_id=$2 AND fence=$3 AND version=$4 AND (phase='open' OR (phase='sealed' AND $6='failed')) AND input_digest=$10 AND key_version=$11 RETURNING version`, scope.TenantID, scope.RequestID, int64(scope.Fence), expected, p.Version, p.Phase, p.FinalRef, encrypted, nonce, p.InputDigest, p.KeyVersion, p.Contract.Version, verified).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return p, runtime.ErrVersionConflict
	}
	if err != nil {
		return p, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO task_checkpoint(tenant_id,request_id,version,fence,input_digest,key_version,phase,final_ref,ciphertext,nonce)
VALUES($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),$9,$10)`, scope.TenantID, scope.RequestID, p.Version, int64(scope.Fence), p.InputDigest, p.KeyVersion, p.Phase, p.FinalRef, encrypted, nonce)
	if err != nil {
		return p, err
	}
	return p, tx.Commit()
}
