package postgres

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
)

func TestCompletionAADRejectsScopeAndReviewSubstitution(t *testing.T) {
	r := messaging.CompletionRecord{TenantID: "tenant-a", RequestID: "request", EnvelopeDigest: strings.Repeat("a", 64), ContentDigest: strings.Repeat("b", 64), KeyVersion: 1}
	key := bytes.Repeat([]byte{0x31}, 32)
	c, n, err := encryptPayload(key, completionAAD(r), []byte("private unpromoted answer"))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"tenant", "request", "envelope", "content", "key"} {
		x := r
		switch field {
		case "tenant":
			x.TenantID = "tenant-b"
		case "request":
			x.RequestID = "other"
		case "envelope":
			x.EnvelopeDigest = strings.Repeat("c", 64)
		case "content":
			x.ContentDigest = strings.Repeat("d", 64)
		case "key":
			x.KeyVersion = 2
		}
		if _, err := decryptPayload(key, completionAAD(x), c, n); err == nil {
			t.Fatalf("accepted substituted %s", field)
		}
	}
}
func TestCompletionRejectsSupersededDatabaseAttempt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := NewWithPayloadKeyResolver(db, keyedPayloadResolver{keys: map[string]map[int64][]byte{"tenant-a": {1: bytes.Repeat([]byte{0x31}, 32)}}})
	r := messaging.CompletionRecord{TenantID: "tenant-a", RequestID: "request", EnvelopeDigest: strings.Repeat("a", 64), Content: []byte("candidate"), KeyVersion: 1, Fence: 1}
	r.ContentDigest = taskflow.Digest(r.Content)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT request_id FROM execution_record").WithArgs("tenant-a", "request").WillReturnRows(sqlmock.NewRows([]string{"request_id"}).AddRow("request"))
	mock.ExpectQuery("SELECT COALESCE").WithArgs("tenant-a", "request").WillReturnRows(sqlmock.NewRows([]string{"fence"}).AddRow(2))
	mock.ExpectRollback()
	if err = s.PutCompletion(context.Background(), r); !errors.Is(err, runtime.ErrStaleFence) {
		t.Fatalf("stale write=%v", err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
