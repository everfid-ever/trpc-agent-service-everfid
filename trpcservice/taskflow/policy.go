package taskflow

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// Contract is service-owned and persisted with the plan, never supplied by
// task_checkpoint arguments. A restart cannot reset or loosen these limits.
type Contract struct {
	Version           int  `json:"version"`
	RequirePlan       bool `json:"require_plan"`
	RequireToolChecks bool `json:"require_tool_checks"`
	MaxAttempts       int  `json:"max_attempts"`
	MaxNoProgress     int  `json:"max_no_progress"`
	MaxAgeSeconds     int  `json:"max_age_seconds"`
}

func DefaultContract() Contract {
	return Contract{Version: 1, RequirePlan: true, RequireToolChecks: true, MaxAttempts: 8, MaxNoProgress: 3, MaxAgeSeconds: 1800}
}

type StoppedError struct{ Reason string }

type ToolBusyError struct{ Until time.Time }

func (e ToolBusyError) Error() string {
	return "task tool lease is still held; defer recovery until lease expiry"
}

func (e StoppedError) Error() string { return e.Reason }
func StopReason(err error) string {
	var stopped StoppedError
	if errors.As(err, &stopped) {
		return stopped.Reason
	}
	if errors.Is(err, ErrEvidenceRejected) {
		return "task_evidence_rejected"
	}
	if errors.Is(err, ErrEvidenceRequired) {
		return "task_evidence_required"
	}
	return ""
}

var ErrEvidenceRequired = errors.New("tool steps require an explicit output check and effectful tools require a trusted evidence contract")

func (s *Session) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}
func progressDigest(p Plan) string {
	var facts []any
	for _, st := range p.Steps {
		if st.State == "succeeded" {
			facts = append(facts, []string{st.ID, st.EvidenceDigest})
		}
	}
	b, _ := json.Marshal(facts)
	return Digest(b)
}

// BeginAttempt counts actual model-run opportunities across owners. Only new
// verified completed steps reset the stagnation counter; edits/starts don't.
func (s *Session) BeginAttempt(ctx context.Context) (Plan, error) {
	p, err := s.load(ctx)
	if err != nil {
		return p, err
	}
	if p.Phase == "failed" {
		return p, StoppedError{p.FailureReason}
	}
	if p.Phase != "open" {
		return p, runtime.ErrInvariantViolation
	}
	if p.Contract.Version == 0 {
		p.Contract = DefaultContract()
	}
	if p.Contract.Version != 1 || p.Contract.MaxAttempts < 1 || p.Contract.MaxNoProgress < 1 || p.Contract.MaxAgeSeconds < 1 || p.Contract.MaxAgeSeconds > 86400 {
		return p, runtime.ErrVersionMismatch
	}
	if p.StartedAt.IsZero() {
		p.StartedAt = s.now()
	}
	digest := progressDigest(p)
	if p.Attempts > 0 && p.LastProgressDigest == digest {
		p.NoProgress++
	} else {
		p.NoProgress = 0
	}
	p.LastProgressDigest = digest
	p.Attempts++
	reason := ""
	switch {
	case !s.now().Before(p.StartedAt.Add(time.Duration(p.Contract.MaxAgeSeconds) * time.Second)):
		reason = "task_deadline_exceeded"
	case p.Attempts > p.Contract.MaxAttempts:
		reason = "task_attempts_exhausted"
	case p.NoProgress >= p.Contract.MaxNoProgress:
		reason = "task_no_progress"
	}
	if reason != "" {
		p.Phase, p.FailureReason = "failed", reason
	}
	p, err = s.save(ctx, p)
	if err != nil {
		return p, err
	}
	if reason != "" {
		return p, StoppedError{reason}
	}
	return p, nil
}

func (s *Session) Fail(ctx context.Context, reason string) (Plan, error) {
	p, err := s.load(ctx)
	if err != nil {
		return p, err
	}
	if p.Phase == "failed" {
		if p.FailureReason != reason {
			return p, runtime.ErrVersionConflict
		}
		return p, nil
	}
	if reason == "" || len(reason) > 128 {
		return p, runtime.ErrInvalidEnvelope
	}
	p.Phase, p.FinalRef, p.FailureReason = "failed", "", reason
	return s.save(ctx, p)
}

func (s *Session) Deadline(ctx context.Context) (time.Time, error) {
	p, err := s.load(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if p.StartedAt.IsZero() || p.Contract.MaxAgeSeconds <= 0 {
		return time.Time{}, runtime.ErrInvariantViolation
	}
	return p.StartedAt.Add(time.Duration(p.Contract.MaxAgeSeconds) * time.Second), nil
}

// EvidenceDescriptor pins a reviewed check to an implementation, not a model
// assertion. Effectful/manual tools without this contract fail before calling.
type EvidenceDescriptor interface{ TaskEvidenceCheck() *ResultCheck }
