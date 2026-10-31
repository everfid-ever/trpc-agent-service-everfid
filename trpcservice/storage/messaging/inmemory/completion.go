package inmemory

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func (s *Store) PutCompletion(ctx context.Context, r messaging.CompletionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completions == nil {
		s.completions = map[string]messaging.CompletionRecord{}
	}
	k := r.TenantID + "\x00" + r.RequestID
	if old, ok := s.completions[k]; ok {
		if old.EnvelopeDigest != r.EnvelopeDigest || old.ContentDigest != r.ContentDigest || old.KeyVersion != r.KeyVersion || string(old.Content) != string(r.Content) {
			return runtime.ErrIdempotencyCollision
		}
		return nil
	}
	r.Content = append([]byte(nil), r.Content...)
	s.completions[k] = r
	return nil
}
func (s *Store) GetCompletion(ctx context.Context, tenant, request string) (messaging.CompletionRecord, error) {
	if err := ctx.Err(); err != nil {
		return messaging.CompletionRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.completions[tenant+"\x00"+request]
	if !ok {
		return r, runtime.ErrNotFound
	}
	r.Content = append([]byte(nil), r.Content...)
	return r, nil
}
