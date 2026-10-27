package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
)

func TestRuntimeOperationsRequireAdminAndExplicitScope(t *testing.T) {
	mux := http.NewServeMux()
	registerRuntimeOperations(mux, "admin-token", nil, nil, nil)
	for _, tc := range []struct {
		method, path, token, body string
		status                    int
	}{
		{"GET", "/ops/reliability?tenant_id=a", "", "", 401},
		{"GET", "/ops/reliability", "admin-token", "", 400},
		{"GET", "/ops/session/lease?tenant_id=a", "admin-token", "", 400},
		{"POST", "/ops/delivery/resolve", "", "{}", 401},
		{"POST", "/ops/delivery/resolve", "admin-token", `{"tenant_id":"a","unknown":true}`, 400},
		{"POST", "/ops/delivery/resolve", "admin-token", `{} {}`, 400},
		{"POST", "/ops/delivery/resolve", "admin-token", `{"decision":"retry"}`, 400},
	} {
		r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		r.Header.Set("X-TRPC-Local-Token", tc.token)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code)
		}
	}
}

func TestRuntimeOperationsScopesSnapshotToTenant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("SELECT jsonb_build_object").WithArgs("tenant-a").WillReturnRows(sqlmock.NewRows([]string{"snapshot"}).AddRow([]byte(`{"tenant_id":"tenant-a","deliveries":{"ambiguous":1}}`)))
	mux := http.NewServeMux()
	registerRuntimeOperations(mux, "admin-token", db, messagingpostgres.New(db), nil)
	r := httptest.NewRequest("GET", "/ops/reliability?tenant_id=tenant-a", nil)
	r.Header.Set("X-TRPC-Local-Token", "admin-token")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), "ambiguous") {
		t.Fatal(w)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
