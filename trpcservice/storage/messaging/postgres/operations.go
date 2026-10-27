package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// DeliveryResolution requires external evidence, not a guess based on a timeout.
// NotDelivered closes the incident; retry_not_delivered explicitly authorizes
// retry only after the operator has established the provider did not deliver.
type DeliveryResolution struct {
	TenantID          string `json:"tenant_id"`
	DeliveryKey       string `json:"delivery_key"`
	SegmentNo         int    `json:"segment_no"`
	ExpectedVersion   int64  `json:"expected_version"`
	Actor             string `json:"actor"`
	EvidenceRef       string `json:"evidence_ref"`
	Decision          string `json:"decision"`
	ProviderMessageID string `json:"provider_message_id"`
}

func (in DeliveryResolution) Validate() error {
	if strings.TrimSpace(in.TenantID) == "" || strings.TrimSpace(in.DeliveryKey) == "" || in.SegmentNo < 0 || in.ExpectedVersion < 1 ||
		strings.TrimSpace(in.Actor) == "" || len(in.Actor) > 256 || strings.TrimSpace(in.EvidenceRef) == "" || len(in.EvidenceRef) > 1024 {
		return runtime.ErrInvalidEnvelope
	}
	if (in.Decision == "delivered" && strings.TrimSpace(in.ProviderMessageID) != "" && len(in.ProviderMessageID) <= 1024) ||
		((in.Decision == "not_delivered" || in.Decision == "retry_not_delivered") && in.ProviderMessageID == "") {
		return nil
	}
	return runtime.ErrInvalidEnvelope
}

// ResolveUncertainDelivery atomically closes an uncertain delivery and persists
// the operator's evidence. A version check fences both concurrent operators and
// the automatic reconciler. Sent deliveries can never be reopened or resent.
func (s *Store) ResolveUncertainDelivery(ctx context.Context, in DeliveryResolution) error {
	if err := in.Validate(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	state, class := "sent", "operator_confirmed_delivered"
	if in.Decision == "not_delivered" {
		state, class = "failed", "operator_confirmed_not_delivered"
	}
	if in.Decision == "retry_not_delivered" {
		state, class = "retry_wait", "operator_confirmed_not_delivered"
	}
	var version int64
	err = tx.QueryRowContext(ctx, `UPDATE delivery_ledger
SET state=$5,provider_message_id=NULLIF($6,''),last_error_class=$7,not_before=now(),version=version+1,updated_at=now()
WHERE tenant_id=$1 AND delivery_key=$2 AND segment_no=$3 AND version=$4
AND (state='ambiguous' OR (state='failed' AND last_error_class='reconcile_exhausted'))
RETURNING version`, in.TenantID, in.DeliveryKey, in.SegmentNo, in.ExpectedVersion, state, in.ProviderMessageID, class).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return runtime.ErrVersionConflict
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO delivery_resolution
(tenant_id,delivery_key,segment_no,expected_version,actor,evidence_ref,decision,provider_message_id)
VALUES($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''))`, in.TenantID, in.DeliveryKey, in.SegmentNo, in.ExpectedVersion, in.Actor, in.EvidenceRef, in.Decision, in.ProviderMessageID)
	if err != nil {
		return err
	}
	if in.Decision != "not_delivered" {
		// The Redis entry may already have been ACKed on reconcile exhaustion.
		// Republish the original frozen reply with its original delivery key;
		// sent segments remain ACK-only, including the externally confirmed one.
		var outboxID string
		err = tx.QueryRowContext(ctx, `UPDATE outbox SET state='pending',claim_owner=NULL,claim_until=NULL,
published_at=NULL,next_attempt_at=now(),version=version+1
WHERE tenant_id=$1 AND kind='reply' AND idempotency_key=$2 AND state<>'claimed'
RETURNING outbox_id`, in.TenantID, in.DeliveryKey).Scan(&outboxID)
		if errors.Is(err, sql.ErrNoRows) {
			return runtime.ErrVersionConflict
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
