package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/httpcallback"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	preprocesspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	serviceknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func envBool(getenv func(string) string, name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.ParseBool(value)
}

func envDuration(getenv func(string) string, name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return time.ParseDuration(value)
}

func envInt(getenv func(string) string, name string, fallback int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	return strconv.Atoi(value)
}

func newChannelEndpoint(adapter channel.HTTPAdapter, resolver ingress.Resolver, identityMapper identity.Mapper,
	intake *preprocesspostgres.Store, payloads *messagingpostgres.Store, keyVersion int64, maxBody int64,
	provider telemetry.Provider, configs ingress.ConfigSelector,
) (*httpcallback.Endpoint, error) {
	if adapter == nil || resolver.Store == nil || resolver.Secrets == nil || identityMapper.Secrets == nil || intake == nil || payloads == nil || keyVersion < 1 || maxBody < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	verification := ingress.Service{Adapter: adapter, Bindings: resolver, Telemetry: provider}
	pipeline := ingress.Pipeline{Verification: verification, Identity: identityMapper, Intake: intake, Payloads: payloads, KeyVersion: keyVersion, Telemetry: provider, Configs: configs}
	challenge := ingress.ChallengeService{Adapter: adapter, Bindings: resolver}
	endpoint, err := httpcallback.NewEndpoint(adapter, pipeline, challenge)
	if err != nil {
		return nil, err
	}
	endpoint.MaxBody = maxBody
	return endpoint, nil
}

func buildKnowledgeResolver(secretProvider secrets.Provider, configs serviceknowledge.ConfigSnapshotReader,
	profiles serviceknowledge.KnowledgeProfileReader, manifests serviceknowledge.IngestionStore,
) (serviceagent.KnowledgeResolver, error) {
	if secretProvider == nil || configs == nil || profiles == nil || manifests == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	factory := serviceknowledge.RuntimeFactory{Backends: serviceknowledge.BackendAdapterResolver{
		Configs: configs, Backends: profiles, Secrets: secretProvider, Subject: "worker-knowledge-qdrant"},
		Manifests: manifests, Embedders: serviceknowledge.EmbedderResolver{Profiles: profiles, Secrets: secretProvider, Subject: "worker-knowledge-embedder"}}
	return serviceknowledge.Resolver{Factory: factory, Limits: serviceknowledge.RetrievalLimits{MaxQueryBytes: 16 << 10, MaxResults: 20, MaxResultBytes: 1 << 20}}, nil
}

func runPreprocessLoop(ctx context.Context, runOnce func(context.Context, int) (int, error), interval time.Duration, batch int, logger *roleLogger) error {
	if ctx == nil || runOnce == nil || interval <= 0 || batch < 1 || logger == nil {
		return runtime.ErrInvariantViolation
	}
	for {
		if _, err := runOnce(ctx, batch); err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, runtime.ErrInvariantViolation) {
				return err
			}
			logger.Printf("preprocess worker degraded: %v", err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func runRecoverableLoop(ctx context.Context, name string, operation func(context.Context) error, retryDelay time.Duration, logger *roleLogger) error {
	if ctx == nil || name == "" || operation == nil || logger == nil {
		return runtime.ErrInvariantViolation
	}
	if retryDelay <= 0 {
		retryDelay = 250 * time.Millisecond
	}
	const maximumRetryDelay = 5 * time.Second
	for delay := retryDelay; ; {
		err := operation(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, runtime.ErrInvariantViolation) || errors.Is(err, runtime.ErrCapabilityUnsupported) {
			return err
		}
		if err == nil {
			logger.Printf("%s stopped unexpectedly; retrying", name)
		} else {
			logger.Printf("%s degraded; retrying in %s: %v", name, delay, err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
		if delay < maximumRetryDelay {
			delay *= 2
			if delay > maximumRetryDelay {
				delay = maximumRetryDelay
			}
		}
	}
}
