package postgres

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/execution"
)

type keyResolver struct{}

func (keyResolver) ResolvePayloadKey(context.Context, string, int64) (messaging.PayloadCipherKey, error) {
	return messaging.PayloadCipherKey{Bytes: bytes.Repeat([]byte{7}, 32), Version: 1}, nil
}

type capturedBytes struct{ value []byte }

func (a *capturedBytes) Match(value driver.Value) bool {
	v, ok := value.([]byte)
	a.value = append([]byte(nil), v...)
	return ok
}
func TestToolResultCompletionEncryptsAndFencesAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewWithPayloadKeys(db, keyResolver{})
	record := execution.Record{Key: execution.Key{TenantID: "tenant", RequestID: "request", ToolCallID: "call"}, LeaseOwner: "owner", Fence: 3}
	content := []byte(`{"secret":"tool-output"}`)
	encrypted, nonce := &capturedBytes{}, &capturedBytes{}
	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE tool_execution").WithArgs("tenant", "request", "call", "owner", int64(3), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"fence"}).AddRow(3))
	mock.ExpectExec("INSERT INTO tool_execution_result").WithArgs("tenant", "request", "call", sqlmock.AnyArg(), sqlmock.AnyArg(), encrypted, nonce, int64(1)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	record, err = store.Complete(context.Background(), record, content, 1)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encrypted.value, []byte("tool-output")) || len(nonce.value) != 12 {
		t.Fatal("tool result was not encrypted")
	}
	digest := record.ResultRef[len("tool-result://sha256/"):]
	mock.ExpectQuery("SELECT r.content_digest").WithArgs("tenant", "request", "call").WillReturnRows(sqlmock.NewRows([]string{"digest", "ciphertext", "nonce", "key_version"}).AddRow(digest, encrypted.value, nonce.value, 1))
	decoded, err := store.LoadResult(context.Background(), record.Key)
	if err != nil || !bytes.Equal(decoded, content) {
		t.Fatalf("%s %v", decoded, err)
	}
	// A ciphertext transplanted across tenants must not decrypt successfully.
	mock.ExpectQuery("SELECT r.content_digest").WithArgs("other", "request", "call").WillReturnRows(sqlmock.NewRows([]string{"digest", "ciphertext", "nonce", "key_version"}).AddRow(digest, encrypted.value, nonce.value, 1))
	key := record.Key
	key.TenantID = "other"
	if _, err = store.LoadResult(context.Background(), key); err == nil {
		t.Fatal("cross-tenant ciphertext accepted")
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestToolResultRejectsStaleOwnerBeforeSavingPayload(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewWithPayloadKeys(db, keyResolver{})
	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE tool_execution").WillReturnRows(sqlmock.NewRows([]string{"fence"}))
	mock.ExpectRollback()
	_, err = store.Complete(context.Background(), execution.Record{Key: execution.Key{TenantID: "tenant", RequestID: "request", ToolCallID: "call"}, LeaseOwner: "old", Fence: 1}, []byte(`"stale"`), 1)
	if !errors.Is(err, runtime.ErrLeaseLost) {
		t.Fatal(err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
