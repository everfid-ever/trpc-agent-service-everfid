package main

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
)

// Operations routes use deployment-admin credentials, never Bot credentials.
// Every query requires an explicit tenant and excludes message bodies/secrets.
func registerRuntimeOperations(mux *http.ServeMux, token string, db *sql.DB, store *messagingpostgres.Store, leases coordination.LeaseInspector) {
	authorize := func(w http.ResponseWriter, r *http.Request) bool {
		if token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-TRPC-Local-Token")), []byte(token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return false
		}
		w.Header().Set("Cache-Control", "no-store")
		return true
	}
	mux.HandleFunc("GET /ops/session/lease", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		query := r.URL.Query()
		key := coordination.SessionKey{TenantID: query.Get("tenant_id"), AgentAppID: query.Get("agent_app_id"), SessionID: query.Get("session_id")}
		if key.TenantID == "" || key.AgentAppID == "" || key.SessionID == "" || len(key.TenantID) > 256 || len(key.AgentAppID) > 256 || len(key.SessionID) > 1024 {
			http.Error(w, "tenant_id, agent_app_id and session_id required", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		observation, err := leases.Inspect(ctx, key)
		if err != nil {
			http.Error(w, "lease status unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(observation)
	})
	mux.HandleFunc("GET /ops/reliability", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		tenant := r.URL.Query().Get("tenant_id")
		if tenant == "" || len(tenant) > 256 {
			http.Error(w, "tenant_id required", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		// One statement gives a consistent snapshot across all durable queues.
		var snapshot []byte
		err := db.QueryRowContext(ctx, `SELECT jsonb_build_object(
'tenant_id',$1::text,'observed_at',now(),
'inbox',(SELECT coalesce(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM inbox WHERE tenant_id=$1 GROUP BY state) q),
'executions',(SELECT coalesce(jsonb_object_agg(outcome,n),'{}') FROM (SELECT outcome,count(*) n FROM execution_record WHERE tenant_id=$1 GROUP BY outcome) q),
'preprocess_claims',(SELECT coalesce(jsonb_agg(q),'[]') FROM (SELECT request_id,session_id,lease_owner,lease_until,attempt,version FROM preprocess_job WHERE tenant_id=$1 AND lease_owner IS NOT NULL ORDER BY lease_until LIMIT 100) q),
'outbox',(SELECT coalesce(jsonb_object_agg(kind||':'||state,n),'{}') FROM (SELECT kind,state,count(*) n FROM outbox WHERE tenant_id=$1 GROUP BY kind,state) q),
'deliveries',(SELECT coalesce(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM delivery_ledger WHERE tenant_id=$1 GROUP BY state) q),
'delivery_incidents',(SELECT coalesce(jsonb_agg(q),'[]') FROM (SELECT delivery_key,segment_no,state,version,attempt,reconcile_attempt,last_error_class,claim_owner,claim_until,updated_at FROM delivery_ledger WHERE tenant_id=$1 AND (state='ambiguous' OR (state='failed' AND last_error_class='reconcile_exhausted')) ORDER BY updated_at,delivery_key,segment_no LIMIT 100) q),
'outbox_claims',(SELECT coalesce(jsonb_agg(q),'[]') FROM (SELECT kind,aggregate_id,claim_owner,claim_until,version FROM outbox WHERE tenant_id=$1 AND state='claimed' ORDER BY claim_until LIMIT 100) q),
'tool_executions',(SELECT coalesce(jsonb_object_agg(state,n),'{}') FROM (SELECT state,count(*) n FROM tool_execution WHERE tenant_id=$1 GROUP BY state) q),
'delivery_resolutions',(SELECT coalesce(jsonb_agg(q),'[]') FROM (SELECT delivery_key,segment_no,expected_version,actor,evidence_ref,decision,created_at FROM delivery_resolution WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 100) q)
)`, tenant).Scan(&snapshot)
		if err != nil {
			http.Error(w, "status unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(snapshot)
	})
	mux.HandleFunc("POST /ops/delivery/resolve", func(w http.ResponseWriter, r *http.Request) {
		if !authorize(w, r) {
			return
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8<<10))
		decoder.DisallowUnknownFields()
		var in messagingpostgres.DeliveryResolution
		if err := decoder.Decode(&in); err != nil {
			http.Error(w, "invalid resolution", 400)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF || in.Validate() != nil {
			http.Error(w, "invalid resolution", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		err := store.ResolveUncertainDelivery(ctx, in)
		if errors.Is(err, runtime.ErrVersionConflict) {
			http.Error(w, "delivery changed or not uncertain", 409)
			return
		}
		if err != nil {
			http.Error(w, "resolution unavailable", 503)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
