package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// CompletionRecord is private, unexposed review material. Only an approved
// finalization may promote its output to result_payload / reply Outbox.
type CompletionRecord struct {
	TenantID, RequestID, EnvelopeDigest, ContentDigest string
	Content                                            []byte
	KeyVersion                                         int64
	Fence                                              uint64
}
type CompletionStore interface {
	PutCompletion(context.Context, CompletionRecord) error
	GetCompletion(context.Context, string, string) (CompletionRecord, error)
}

func (r CompletionRecord) Validate() error {
	sum := sha256.Sum256(r.Content)
	if r.TenantID == "" || r.RequestID == "" || len(r.EnvelopeDigest) != 64 || r.ContentDigest != hex.EncodeToString(sum[:]) || len(r.Content) == 0 || len(r.Content) > 2<<20 || r.KeyVersion < 1 || r.Fence == 0 {
		return runtime.ErrInvalidEnvelope
	}
	return nil
}
