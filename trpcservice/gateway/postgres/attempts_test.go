package postgres

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"testing"
)

func TestExecutionAttemptSupersedesPriorOwnerAndFencesFinish(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := NewTaskStore(db)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT request_id FROM execution_record").WithArgs("tenant", "request").WillReturnRows(sqlmock.NewRows([]string{"request_id"}).AddRow("request"))
	mock.ExpectQuery("SELECT COALESCE").WillReturnRows(sqlmock.NewRows([]string{"fence"}).AddRow(1))
	mock.ExpectExec("UPDATE execution_attempt").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("INSERT INTO execution_attempt").WithArgs("tenant", "request", "worker-b", int64(2)).WillReturnRows(sqlmock.NewRows([]string{"attempt_no"}).AddRow(2))
	mock.ExpectCommit()
	attempt, err := store.BeginExecutionAttempt(context.Background(), gateway.ExecutionKey{TenantID: "tenant", RequestID: "request"}, "worker-b", 2)
	if err != nil || attempt.Number != 2 {
		t.Fatalf("%+v %v", attempt, err)
	}
	mock.ExpectQuery("UPDATE execution_attempt").WithArgs("tenant", "request", int64(2), "worker-b", int64(2), "completed").WillReturnRows(sqlmock.NewRows([]string{"attempt_no"}).AddRow(2))
	if err = store.FinishExecutionAttempt(context.Background(), attempt, true); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("UPDATE execution_attempt").WillReturnRows(sqlmock.NewRows([]string{"attempt_no"}))
	if err = store.FinishExecutionAttempt(context.Background(), attempt, true); !errors.Is(err, runtime.ErrVersionConflict) {
		t.Fatal(err)
	}
	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
