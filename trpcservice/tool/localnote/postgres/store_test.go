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
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/localnote"
)

type keys struct{}

func (keys) ResolvePayloadKey(context.Context, string, int64) (messaging.PayloadCipherKey, error) {
	return messaging.PayloadCipherKey{Bytes: bytes.Repeat([]byte{3}, 32), Version: 1}, nil
}

type encryptedArgument struct{}

func (encryptedArgument) Match(v driver.Value) bool {
	data, ok := v.([]byte)
	return ok && len(data) > 16 && !bytes.Contains(data, []byte("private note"))
}
func TestNoteEffectUsesStableBusinessKeyAndRejectsCollision(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := Store{DB: db, Keys: keys{}}
	note := localnote.Note{TenantID: "tenant", RequestID: "request", BusinessKey: "business", ArgsDigest: "digest", KeyVersion: 1, Result: localnote.Result{NoteID: "note-1", Status: "created", Content: "private note"}}
	for i := 0; i < 2; i++ {
		mock.ExpectQuery("INSERT INTO local_note").WithArgs("tenant", "business", "request", "note-1", "digest", encryptedArgument{}, sqlmock.AnyArg(), int64(1)).WillReturnRows(sqlmock.NewRows([]string{"request_id", "note_id", "args_digest"}).AddRow("request", "note-1", "digest"))
		if result, err := store.Create(context.Background(), note); err != nil || result.NoteID != "note-1" {
			t.Fatalf("%+v %v", result, err)
		}
	}
	mock.ExpectQuery("INSERT INTO local_note").WillReturnRows(sqlmock.NewRows([]string{"request_id", "note_id", "args_digest"}).AddRow("request", "note-1", "other-digest"))
	if _, err := store.Create(context.Background(), note); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
