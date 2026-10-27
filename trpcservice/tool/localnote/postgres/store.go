package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"encoding/json"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/localnote"
)

type Store struct {
	DB   *sql.DB
	Keys messaging.PayloadKeyResolver
}

// Create commits the business effect before the tool execution result. A retry
// with the same business key returns the same note rather than another write.
func (s Store) Create(ctx context.Context, in localnote.Note) (localnote.Result, error) {
	if s.DB == nil || s.Keys == nil || in.TenantID == "" || in.BusinessKey == "" || in.RequestID == "" || in.KeyVersion < 1 {
		return localnote.Result{}, runtime.ErrCapabilityUnsupported
	}
	key, err := s.Keys.ResolvePayloadKey(ctx, in.TenantID, in.KeyVersion)
	if err != nil {
		return localnote.Result{}, err
	}
	defer clear(key.Bytes)
	if key.Version != in.KeyVersion || len(key.Bytes) != 32 {
		return localnote.Result{}, runtime.ErrVersionMismatch
	}
	block, err := aes.NewCipher(key.Bytes)
	if err != nil {
		return localnote.Result{}, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return localnote.Result{}, err
	}
	content, err := json.Marshal(in.Result)
	if err != nil {
		return localnote.Result{}, err
	}
	aad, _ := json.Marshal([]any{"local-note-v1", in.TenantID, in.RequestID, in.BusinessKey, in.ArgsDigest, in.KeyVersion})
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return localnote.Result{}, err
	}
	encrypted := aead.Seal(nil, nonce, content, aad)
	var request, digest, id string
	err = s.DB.QueryRowContext(ctx, `INSERT INTO local_note(tenant_id,business_key,request_id,note_id,args_digest,ciphertext,nonce,key_version)
VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(tenant_id,business_key) DO UPDATE SET business_key=EXCLUDED.business_key
RETURNING request_id,note_id,args_digest`, in.TenantID, in.BusinessKey, in.RequestID, in.Result.NoteID, in.ArgsDigest, encrypted, nonce, in.KeyVersion).Scan(&request, &id, &digest)
	if err != nil {
		return localnote.Result{}, err
	}
	if request != in.RequestID || digest != in.ArgsDigest || id != in.Result.NoteID {
		return localnote.Result{}, runtime.ErrIdempotencyCollision
	}
	return in.Result, nil
}
