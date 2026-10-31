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
	"github.com/liuzengh/trpc-agent-service/trpcservice/taskflow"
)

type keys struct{}

func (keys) ResolvePayloadKey(_ context.Context, _ string, v int64) (messaging.PayloadCipherKey, error) {
	return messaging.PayloadCipherKey{Bytes: bytes.Repeat([]byte{4}, 32), Version: v}, nil
}

type capture struct{ b []byte }

func (c *capture) Match(v driver.Value) bool {
	b, ok := v.([]byte)
	c.b = append([]byte(nil), b...)
	return ok
}
func TestSaveCheckpointsEncryptedAndAtomicWithFenceAndVersion(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := New(db, keys{})
	p := taskflow.Plan{Scope: taskflow.Scope{Key: taskflow.Key{TenantID: "t", RequestID: "r"}, Fence: 7}, Version: 1, InputDigest: taskflow.Digest([]byte("input")), KeyVersion: 1, Phase: "open", Steps: []taskflow.Step{{ID: "a", Title: "private goal", Kind: "analysis", State: "pending"}}}
	c, n := &capture{}, &capture{}
	m.ExpectBegin()
	m.ExpectQuery("UPDATE task_plan SET version").WithArgs("t", "r", int64(7), int64(1), int64(2), "open", "", c, n, p.InputDigest, int64(1)).WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(2))
	m.ExpectExec("INSERT INTO task_checkpoint").WithArgs("t", "r", int64(2), int64(7), p.InputDigest, int64(1), "open", "", sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	m.ExpectCommit()
	p, err = s.Save(context.Background(), p.Scope, 1, p)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(c.b, []byte("private goal")) || len(n.b) != 12 {
		t.Fatal("checkpoint plaintext exposed")
	}
	rows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"fence", "version", "digest", "key", "phase", "ref", "cipher", "nonce"}).AddRow(8, 2, p.InputDigest, 1, "open", "", c.b, n.b)
	}
	// A new owner changes the fence, not the immutable checkpoint ciphertext.
	m.ExpectQuery("SELECT fence").WithArgs("t", "r").WillReturnRows(rows())
	got, err := s.Get(context.Background(), p.Key)
	if err != nil || got.Fence != 8 || got.Steps[0].Title != "private goal" {
		t.Fatal(got, err)
	}
	m.ExpectQuery("SELECT fence").WithArgs("other", "r").WillReturnRows(rows())
	if _, err = s.Get(context.Background(), taskflow.Key{TenantID: "other", RequestID: "r"}); err == nil {
		t.Fatal("cross-tenant ciphertext accepted")
	}
	if err = m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
func TestSaveRollsBackStaleOwnerAndHistoryFailure(t *testing.T) {
	for _, stale := range []bool{true, false} {
		t.Run(map[bool]string{true: "stale", false: "history failure"}[stale], func(t *testing.T) {
			db, m, _ := sqlmock.New()
			defer db.Close()
			s := New(db, keys{})
			p := taskflow.Plan{Scope: taskflow.Scope{Key: taskflow.Key{TenantID: "t", RequestID: "r"}, Fence: 1}, Version: 2, KeyVersion: 1, InputDigest: taskflow.Digest([]byte("input")), Phase: "open"}
			m.ExpectBegin()
			q := m.ExpectQuery("UPDATE task_plan SET version")
			if stale {
				q.WillReturnRows(sqlmock.NewRows([]string{"version"}))
			} else {
				q.WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(3))
				m.ExpectExec("INSERT INTO task_checkpoint").WillReturnError(errors.New("history unavailable"))
			}
			m.ExpectRollback()
			_, err := s.Save(context.Background(), p.Scope, p.Version, p)
			if err == nil || (stale && !errors.Is(err, runtime.ErrVersionConflict)) {
				t.Fatal(err)
			}
			if err = m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOpenAdoptsFenceWithoutReplacingFrozenPlan(t *testing.T) {
	db, m, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := New(db, keys{})
	p := taskflow.Plan{Scope: taskflow.Scope{Key: taskflow.Key{TenantID: "t", RequestID: "r"}, Fence: 8}, Version: 6, InputDigest: taskflow.Digest([]byte("original")), KeyVersion: 1, Phase: "sealed", FinalRef: "result://frozen", Steps: []taskflow.Step{{ID: "analysis", Kind: "analysis", State: "succeeded", Note: "saved"}}}
	c, n, err := s.encrypt(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	scope := p.Scope
	scope.Fence = 9
	m.ExpectQuery("(?s)INSERT INTO task_plan.*e.input_seq=h.next_input_seq.*task_plan.fence<=EXCLUDED.fence").WithArgs("t", "r", int64(9), p.InputDigest, int64(1), sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"fence", "version", "digest", "key", "phase", "ref", "cipher", "nonce"}).AddRow(9, 6, p.InputDigest, 1, "sealed", p.FinalRef, c, n))
	got, err := s.Open(context.Background(), scope, p.InputDigest, 1)
	if err != nil || got.Fence != 9 || got.Version != 6 || got.Phase != "sealed" || got.FinalRef != p.FinalRef || got.Steps[0].Note != "saved" {
		t.Fatal(got, err)
	}
	m.ExpectQuery("INSERT INTO task_plan").WillReturnRows(sqlmock.NewRows([]string{"fence", "version", "digest", "key", "phase", "ref", "cipher", "nonce"}))
	if _, err = s.Open(context.Background(), p.Scope, p.InputDigest, 1); !errors.Is(err, runtime.ErrStaleFence) {
		t.Fatalf("stale adoption: %v", err)
	}
	if err = m.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
