# 单主机多容器实例可用性 — 参考实现

## 复刻范围和共享契约

本能力不新建节点本地业务表。会话、执行、Outbox、Ledger、工具与处置表的逐字段设计分别见 [会话设计](../conversation-continuity/design.md)、[在途设计](../inflight-task-takeover/design.md)；原文DDL与Redis协议见相应参考实现。两个容器必须共享这些事实，不能部署两套独立环境来模拟接管。

## 当前入口完整源代码

以下为 `cmd/trpc-service/wecom_ha_entry_role.go`，使用真实 wecomHAEntry 标识符，含配置、并行probe、epoch、恢复窗口、错误切换和HTTP生命周期。


```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// wecomHAEntryConfig describes the small, stable callback entrypoint used by
// the local multi-container drill. Backends are private Compose addresses; no
// node address is exposed to the callback provider.
type wecomHAEntryConfig struct {
	ListenAddress     string
	Backends          []*url.URL
	ProbeInterval     time.Duration
	ProbeTimeout      time.Duration
	RequestTimeout    time.Duration
	HealthTTL         time.Duration
	RecoverySuccesses int
}

func loadWeComHAEntryConfig(getenv func(string) string) (wecomHAEntryConfig, error) {
	if getenv == nil {
		return wecomHAEntryConfig{}, errors.New("missing environment reader")
	}
	interval := time.Second
	if value := strings.TrimSpace(getenv("TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL")); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 100*time.Millisecond || parsed > time.Minute {
			return wecomHAEntryConfig{}, errors.New("invalid WeCom HA entry probe interval")
		}
		interval = parsed
	}
	parts := strings.Split(strings.TrimSpace(getenv("TRPC_WECOM_HA_ENTRY_BACKENDS")), ",")
	if len(parts) < 2 {
		return wecomHAEntryConfig{}, errors.New("at least two WeCom HA entry backends are required")
	}
	backends := make([]*url.URL, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		parsed, err := url.Parse(strings.TrimSpace(part))
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" {
			return wecomHAEntryConfig{}, errors.New("invalid WeCom HA entry backend")
		}
		if _, exists := seen[parsed.String()]; exists {
			return wecomHAEntryConfig{}, errors.New("duplicate WeCom HA entry backend")
		}
		seen[parsed.String()] = struct{}{}
		backends = append(backends, parsed)
	}
	listenAddress := valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080")
	if strings.TrimSpace(listenAddress) != listenAddress || listenAddress == "" {
		return wecomHAEntryConfig{}, errors.New("invalid WeCom HA entry listen address")
	}
	config := wecomHAEntryConfig{ListenAddress: listenAddress, Backends: backends, ProbeInterval: interval, ProbeTimeout: time.Second, RequestTimeout: 2 * time.Second, RecoverySuccesses: 2}
	for _, setting := range []struct {
		name   string
		target *time.Duration
	}{
		{"TRPC_WECOM_HA_ENTRY_PROBE_TIMEOUT", &config.ProbeTimeout}, {"TRPC_WECOM_HA_ENTRY_REQUEST_TIMEOUT", &config.RequestTimeout},
	} {
		if raw := strings.TrimSpace(getenv(setting.name)); raw != "" {
			duration, err := time.ParseDuration(raw)
			if err != nil || duration < 100*time.Millisecond || duration > 30*time.Second {
				return config, errors.New("invalid WeCom HA entry timeout")
			}
			*setting.target = duration
		}
	}
	config.HealthTTL = 3 * (interval + config.ProbeTimeout)
	if raw := strings.TrimSpace(getenv("TRPC_WECOM_HA_ENTRY_RECOVERY_SUCCESSES")); raw != "" {
		count, err := strconv.Atoi(raw)
		if err != nil || count < 1 || count > 10 {
			return config, errors.New("invalid entry recovery threshold")
		}
		config.RecoverySuccesses = count
	}
	return config, nil
}

type wecomHAEntryBackend struct {
	URL                  string    `json:"url"`
	Healthy              bool      `json:"healthy"`
	CheckedAt            time.Time `json:"checked_at,omitempty"`
	HealthySince         time.Time `json:"healthy_since,omitempty"`
	Failures             uint64    `json:"failures"`
	LastFailure          string    `json:"last_failure,omitempty"`
	ConsecutiveSuccesses int       `json:"consecutive_successes"`
}

type wecomHAEntry struct {
	backends                                []wecomHAEntryBackend
	health                                  []atomic.Bool
	next                                    atomic.Uint64
	client                                  *http.Client
	mu                                      sync.Mutex
	epoch                                   []uint64
	probeTimeout, requestTimeout, healthTTL time.Duration
	recoverySuccesses                       int
}

func newWeComHAEntry(backends []*url.URL, client *http.Client) (*wecomHAEntry, error) {
	if len(backends) < 2 {
		return nil, errors.New("at least two backends are required")
	}
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	entry := &wecomHAEntry{backends: make([]wecomHAEntryBackend, len(backends)), health: make([]atomic.Bool, len(backends)), client: client}
	entry.client = &clientCopy
	entry.epoch = make([]uint64, len(backends))
	entry.probeTimeout, entry.requestTimeout, entry.healthTTL, entry.recoverySuccesses = time.Second, 2*time.Second, 6*time.Second, 2
	for index, backend := range backends {
		if backend == nil {
			return nil, errors.New("nil backend")
		}
		entry.backends[index] = wecomHAEntryBackend{URL: backend.String()}
	}
	return entry, nil
}

func (entry *wecomHAEntry) probe(ctx context.Context) {
	var pending sync.WaitGroup
	for index := range entry.backends {
		pending.Add(1)
		go func(index int) {
			defer pending.Done()
			entry.mu.Lock()
			epoch := entry.epoch[index]
			entry.mu.Unlock()
			probeCtx, cancel := context.WithTimeout(ctx, entry.probeTimeout)
			defer cancel()
			request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, entry.backends[index].URL+"/readyz", nil)
			if err != nil {
				entry.recordProbe(index, epoch, false)
				return
			}
			response, err := entry.client.Do(request)
			if err == nil && response != nil && response.Body != nil {
				_, bodyErr := io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
				if bodyErr != nil {
					err = bodyErr
				}
				response.Body.Close()
			}
			entry.recordProbe(index, epoch, err == nil && probeCtx.Err() == nil && response != nil && response.Body != nil && response.StatusCode == http.StatusOK)
		}(index)
	}
	pending.Wait()
}

func (entry *wecomHAEntry) recordProbe(index int, epoch uint64, healthy bool) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.epoch[index] != epoch {
		return
	}
	backend := &entry.backends[index]
	if !backend.CheckedAt.IsZero() && time.Since(backend.CheckedAt) > entry.healthTTL {
		backend.Failures++
		backend.LastFailure = "health_expired"
		backend.ConsecutiveSuccesses = 0
		backend.HealthySince = time.Time{}
		entry.health[index].Store(false)
	}
	backend.CheckedAt = time.Now().UTC()
	if !healthy {
		entry.epoch[index]++
		backend.Failures++
		backend.LastFailure = "probe_failed"
		backend.ConsecutiveSuccesses = 0
		backend.HealthySince = time.Time{}
		entry.health[index].Store(false)
		return
	}
	backend.ConsecutiveSuccesses++
	if backend.Failures == 0 || backend.ConsecutiveSuccesses >= entry.recoverySuccesses {
		if !entry.health[index].Load() {
			backend.HealthySince = time.Now().UTC()
		}
		entry.health[index].Store(true)
		backend.LastFailure = ""
	}
}
func (entry *wecomHAEntry) failBackend(index int, reason string) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.epoch[index]++
	backend := &entry.backends[index]
	backend.Failures++
	backend.LastFailure = reason
	backend.ConsecutiveSuccesses = 0
	backend.HealthySince = time.Time{}
	entry.health[index].Store(false)
}
func (entry *wecomHAEntry) healthy(index int) bool {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.health[index].Load() && !entry.backends[index].CheckedAt.IsZero() && time.Since(entry.backends[index].CheckedAt) <= entry.healthTTL
}

func (entry *wecomHAEntry) anyHealthy() bool {
	for index := range entry.backends {
		if entry.healthy(index) {
			return true
		}
	}
	return false
}

func (entry *wecomHAEntry) nextHealthy(excluded map[int]struct{}) (int, bool) {
	start := int((entry.next.Add(1) - 1) % uint64(len(entry.backends)))
	for offset := range entry.backends {
		index := (start + offset) % len(entry.backends)
		if entry.healthy(index) {
			if _, skip := excluded[index]; !skip {
				return index, true
			}
		}
	}
	return 0, false
}

func (entry *wecomHAEntry) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case "/livez":
		writer.WriteHeader(http.StatusOK)
		return
	case "/readyz":
		if !entry.anyHealthy() {
			http.Error(writer, "no healthy callback backend", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		return
	case "/statusz":
		entry.mu.Lock()
		status := make([]wecomHAEntryBackend, len(entry.backends))
		copy(status, entry.backends)
		for index := range status {
			status[index].Healthy = entry.health[index].Load() && !status[index].CheckedAt.IsZero() && time.Since(status[index].CheckedAt) <= entry.healthTTL
		}
		entry.mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{"backends": status})
		return
	}
	if request.URL.Path != "/callbacks/wecom" {
		http.NotFound(writer, request)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 2<<20))
	if err != nil {
		http.Error(writer, "callback body too large", http.StatusRequestEntityTooLarge)
		return
	}
	excluded := make(map[int]struct{}, len(entry.backends))
	for range entry.backends {
		index, ok := entry.nextHealthy(excluded)
		if !ok {
			break
		}
		excluded[index] = struct{}{}
		forwardCtx, cancel := context.WithTimeout(request.Context(), entry.requestTimeout)
		response, forwardErr := entry.forward(request.WithContext(forwardCtx), body, entry.backends[index].URL)
		if forwardErr != nil {
			cancel()
			if request.Context().Err() != nil {
				return
			}
			entry.failBackend(index, "transport_failed")
			continue
		}
		if response == nil || response.Body == nil {
			cancel()
			entry.failBackend(index, "invalid_response")
			continue
		}
		if response.StatusCode == http.StatusBadGateway || response.StatusCode == http.StatusServiceUnavailable || response.StatusCode == http.StatusGatewayTimeout {
			response.Body.Close()
			cancel()
			entry.failBackend(index, "backend_unavailable")
			continue
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, (2<<20)+1))
		response.Body.Close()
		cancel()
		if request.Context().Err() != nil {
			return
		}
		if readErr != nil || len(responseBody) > 2<<20 {
			if response.StatusCode >= 400 && response.StatusCode < 500 {
				http.Error(writer, "callback rejected", response.StatusCode)
				return
			}
			entry.failBackend(index, "response_incomplete")
			continue
		}
		copyHeaders(writer.Header(), response.Header)
		writer.WriteHeader(response.StatusCode)
		_, _ = writer.Write(responseBody)
		return
	}
	http.Error(writer, "no healthy callback backend", http.StatusServiceUnavailable)
}

func (entry *wecomHAEntry) forward(inbound *http.Request, body []byte, backend string) (*http.Response, error) {
	target := backend + inbound.URL.RequestURI()
	request, err := http.NewRequestWithContext(inbound.Context(), inbound.Method, target, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	copyHeaders(request.Header, inbound.Header)
	request.Host = ""
	return entry.client.Do(request)
}

func copyHeaders(destination, source http.Header) {
	hop := map[string]bool{"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true, "Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true}
	for _, token := range strings.Split(source.Get("Connection"), ",") {
		hop[http.CanonicalHeaderKey(strings.TrimSpace(token))] = true
	}
	for key, values := range source {
		if hop[http.CanonicalHeaderKey(key)] {
			continue
		}
		destination.Del(key)
		for _, value := range values {
			destination.Add(key, value)
		}
	}
}

func runWeComHAEntryRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	config, err := loadWeComHAEntryConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	entry, err := newWeComHAEntry(config.Backends, nil)
	if err != nil {
		return err
	}
	entry.probeTimeout, entry.requestTimeout, entry.healthTTL, entry.recoverySuccesses = config.ProbeTimeout, config.RequestTimeout, config.HealthTTL, config.RecoverySuccesses
	entry.probe(parent)
	processCtx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		ticker := time.NewTicker(config.ProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-processCtx.Done():
				return
			case <-ticker.C:
				entry.probe(processCtx)
			}
		}
	}()
	server := &http.Server{Addr: config.ListenAddress, Handler: http.HandlerFunc(entry.serveHTTP), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	select {
	case runErr := <-errCh:
		if errors.Is(runErr, http.ErrServerClosed) {
			return nil
		}
		return runErr
	case <-processCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	}
}
```


| 配置 | 默认值 | 校验 / 行为 |
|---|---|---|
| TRPC_LISTEN_ADDRESS | :8080 | 非空，无首尾空白。 |
| TRPC_WECOM_HA_ENTRY_BACKENDS | 必填 | 至少两个不同 http origin；拒绝 user/query/fragment/path，含尾斜杠路径也拒绝。 |
| TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL | 1s | 100ms–1m。 |
| TRPC_WECOM_HA_ENTRY_PROBE_TIMEOUT | 1s | 100ms–30s。 |
| TRPC_WECOM_HA_ENTRY_REQUEST_TIMEOUT | 2s | 100ms–30s，每后端一次转发。 |
| TRPC_WECOM_HA_ENTRY_RECOVERY_SUCCESSES | 2 | 1–10，仅恢复阶段要求连续成功。 |
| HealthTTL | 派生值 | 3×(interval+probe timeout)，没有单独环境变量。 |

## 多实例 Compose 服务原文

下列片段逐字来自当前配置，包含应用/入口服务。它们依赖的 postgres、redis、qdrant、clamav、telemetry、卷初始化、named volumes与secrets由完整 `deploy/compose/docker-compose.local.yml` 提供；独立复刻时须建立等价依赖、镜像和卷权限，不能把片段当独立完整文件。


```yaml
services:
  webui-bootstrap:
    build:
      context: ../..
      dockerfile: deploy/compose/Dockerfile
    profiles: ["webui-multinode"]
    command: ["webui-local-bootstrap"]
    env_file:
      - path: .env.local
        required: false
    environment:
      TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY: "false"
      TRPC_POSTGRES_DSN: postgres://${TRPC_LOCAL_POSTGRES_USER:-postgres}:${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}@postgres:5432/${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}?sslmode=disable
      TRPC_REDIS_ADDRESS: redis:6379
      TRPC_WEBUI_DEEPSEEK_KEY_FILE: /run/secrets/deepseek_api_key
      TRPC_WEBUI_LOCAL_INSTANCE_ID: bootstrap
      TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT: /var/lib/trpc-webui-local/skills
    secrets:
      - deepseek_api_key
    volumes:
      - webui-local-skills:/var/lib/trpc-webui-local/skills
    depends_on:
      webui-local-skills-init:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      qdrant:
        condition: service_healthy

  webui-node-a:
    build:
      context: ../..
      dockerfile: deploy/compose/Dockerfile
    profiles: ["webui-multinode"]
    command: ["webui-local"]
    read_only: true
    stop_grace_period: ${TRPC_LOCAL_APP_STOP_GRACE_PERIOD:-45s}
    tmpfs:
      - /tmp:rw,nosuid,nodev,size=256m,mode=1777
    env_file:
      - path: .env.local
        required: false
    environment:
      TRPC_POSTGRES_DSN: postgres://${TRPC_LOCAL_POSTGRES_USER:-postgres}:${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}@postgres:5432/${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}?sslmode=disable
      TRPC_REDIS_ADDRESS: redis:6379
      TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS: clamav:3310
      TRPC_LISTEN_ADDRESS: ":8080"
      TRPC_WEBUI_DEEPSEEK_KEY_FILE: /run/secrets/deepseek_api_key
      TRPC_WEBUI_LOCAL_INSTANCE_ID: node-a
      TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY: "true"
      TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED: "true"
      TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT: /var/lib/trpc-webui-local/skills
      TRPC_OTEL_ENDPOINT: http://otel-collector:4318
      TRPC_OTEL_ALLOW_INSECURE: "true"
      TRPC_SERVICE_VERSION: ${TRPC_LOCAL_SERVICE_VERSION:-development}
    secrets:
      - deepseek_api_key
    volumes:
      - webui-local-skills:/var/lib/trpc-webui-local/skills:ro
    ports:
      - "${TRPC_LOCAL_MULTINODE_NODE_A_PORT:-58083}:8080"
    depends_on:
      webui-bootstrap:
        condition: service_completed_successfully
      clamav:
        condition: service_healthy
      otel-collector:
        condition: service_started

  webui-node-b:
    build:
      context: ../..
      dockerfile: deploy/compose/Dockerfile
    profiles: ["webui-multinode"]
    command: ["webui-local"]
    read_only: true
    stop_grace_period: ${TRPC_LOCAL_APP_STOP_GRACE_PERIOD:-45s}
    tmpfs:
      - /tmp:rw,nosuid,nodev,size=256m,mode=1777
    env_file:
      - path: .env.local
        required: false
    environment:
      TRPC_POSTGRES_DSN: postgres://${TRPC_LOCAL_POSTGRES_USER:-postgres}:${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}@postgres:5432/${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}?sslmode=disable
      TRPC_REDIS_ADDRESS: redis:6379
      TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS: clamav:3310
      TRPC_LISTEN_ADDRESS: ":8080"
      TRPC_WEBUI_DEEPSEEK_KEY_FILE: /run/secrets/deepseek_api_key
      TRPC_WEBUI_LOCAL_INSTANCE_ID: node-b
      TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY: "true"
      TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED: "true"
      TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT: /var/lib/trpc-webui-local/skills
      TRPC_OTEL_ENDPOINT: http://otel-collector:4318
      TRPC_OTEL_ALLOW_INSECURE: "true"
      TRPC_SERVICE_VERSION: ${TRPC_LOCAL_SERVICE_VERSION:-development}
    secrets:
      - deepseek_api_key
    volumes:
      - webui-local-skills:/var/lib/trpc-webui-local/skills:ro
    ports:
      - "${TRPC_LOCAL_MULTINODE_NODE_B_PORT:-58084}:8080"
    depends_on:
      webui-bootstrap:
        condition: service_completed_successfully
      clamav:
        condition: service_healthy
      otel-collector:
        condition: service_started

  # The HA profile keeps the WeCom callback URL stable at wecom-ha-entry while
  # two independently identified nodes share the durable authorities.
  wecom-ha-bootstrap:
    build:
      context: ../..
      dockerfile: deploy/compose/Dockerfile
    profiles: ["wecom-ha-local"]
    command: ["webui-local-bootstrap"]
    env_file:
      - path: .env.local
        required: false
      - path: secrets/wecom.env
        required: true
    environment:
      TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY: "false"
      TRPC_POSTGRES_DSN: postgres://${TRPC_LOCAL_POSTGRES_USER:-postgres}:${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}@postgres:5432/${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}?sslmode=disable
      TRPC_REDIS_ADDRESS: redis:6379
      TRPC_WEBUI_LOCAL_INSTANCE_ID: wecom-ha-bootstrap
      TRPC_WECOM_LOCAL_ENABLED: "true"
      TRPC_WECOM_SECONDARY_LOCAL_ENABLED: "true"
      TRPC_WEBUI_DEEPSEEK_KEY_FILE: /run/secrets/deepseek_api_key
      TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT: /var/lib/trpc-webui-local/skills
    secrets:
      - deepseek_api_key
    volumes:
      - webui-local-skills:/var/lib/trpc-webui-local/skills
    depends_on:
      webui-local-skills-init:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      qdrant:
        condition: service_healthy

  wecom-ha-node-a:
    build:
      context: ../..
      dockerfile: deploy/compose/Dockerfile
    profiles: ["wecom-ha-local"]
    command: ["wecom-local"]
    restart: "no"
    read_only: true
    stop_grace_period: ${TRPC_LOCAL_APP_STOP_GRACE_PERIOD:-45s}
    tmpfs:
      - /tmp:rw,nosuid,nodev,size=256m,mode=1777
    env_file:
      - path: .env.local
        required: false
      - path: secrets/wecom.env
        required: true
    environment:
      TRPC_POSTGRES_DSN: postgres://${TRPC_LOCAL_POSTGRES_USER:-postgres}:${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}@postgres:5432/${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}?sslmode=disable
      TRPC_REDIS_ADDRESS: redis:6379
      TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS: clamav:3310
      TRPC_LISTEN_ADDRESS: ":8080"
      TRPC_WEBUI_LOCAL_INSTANCE_ID: wecom-ha-node-a
      TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY: "true"
      TRPC_WEBUI_LOCAL_STATUS_ENABLED: "true"
      TRPC_WECOM_LOCAL_ENABLED: "true"
      TRPC_WECOM_SECONDARY_LOCAL_ENABLED: "true"
      TRPC_WEBUI_DEEPSEEK_KEY_FILE: /run/secrets/deepseek_api_key
      TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT: /var/lib/trpc-webui-local/skills
      TRPC_OTEL_ENDPOINT: http://otel-collector:4318
      TRPC_OTEL_ALLOW_INSECURE: "true"
      TRPC_SERVICE_VERSION: ${TRPC_LOCAL_SERVICE_VERSION:-development}
    secrets:
      - deepseek_api_key
    volumes:
      - webui-local-skills:/var/lib/trpc-webui-local/skills:ro
    ports:
      - "${TRPC_LOCAL_WECOM_HA_NODE_A_PORT:-58088}:8080"
    depends_on:
      wecom-ha-bootstrap:
        condition: service_completed_successfully
      clamav:
        condition: service_healthy
      otel-collector:
        condition: service_started

  wecom-ha-node-b:
    build:
      context: ../..
      dockerfile: deploy/compose/Dockerfile
    profiles: ["wecom-ha-local"]
    command: ["wecom-local"]
    restart: "no"
    read_only: true
    stop_grace_period: ${TRPC_LOCAL_APP_STOP_GRACE_PERIOD:-45s}
    tmpfs:
      - /tmp:rw,nosuid,nodev,size=256m,mode=1777
    env_file:
      - path: .env.local
        required: false
      - path: secrets/wecom.env
        required: true
    environment:
      TRPC_POSTGRES_DSN: postgres://${TRPC_LOCAL_POSTGRES_USER:-postgres}:${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}@postgres:5432/${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}?sslmode=disable
      TRPC_REDIS_ADDRESS: redis:6379
      TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS: clamav:3310
      TRPC_LISTEN_ADDRESS: ":8080"
      TRPC_WEBUI_LOCAL_INSTANCE_ID: wecom-ha-node-b
      TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY: "true"
      TRPC_WEBUI_LOCAL_STATUS_ENABLED: "true"
      TRPC_WECOM_LOCAL_ENABLED: "true"
      TRPC_WECOM_SECONDARY_LOCAL_ENABLED: "true"
      TRPC_WEBUI_DEEPSEEK_KEY_FILE: /run/secrets/deepseek_api_key
      TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT: /var/lib/trpc-webui-local/skills
      TRPC_OTEL_ENDPOINT: http://otel-collector:4318
      TRPC_OTEL_ALLOW_INSECURE: "true"
      TRPC_SERVICE_VERSION: ${TRPC_LOCAL_SERVICE_VERSION:-development}
    secrets:
      - deepseek_api_key
    volumes:
      - webui-local-skills:/var/lib/trpc-webui-local/skills:ro
    ports:
      - "${TRPC_LOCAL_WECOM_HA_NODE_B_PORT:-58089}:8080"
    depends_on:
      wecom-ha-bootstrap:
        condition: service_completed_successfully
      clamav:
        condition: service_healthy
      otel-collector:
        condition: service_started

  wecom-ha-entry:
    build:
      context: ../..
      dockerfile: deploy/compose/Dockerfile
    profiles: ["wecom-ha-local"]
    command: ["wecom-ha-entry"]
    restart: "no"
    environment:
      TRPC_LISTEN_ADDRESS: ":8080"
      TRPC_WECOM_HA_ENTRY_BACKENDS: http://wecom-ha-node-a:8080,http://wecom-ha-node-b:8080
      TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL: 1s
    ports:
      - "${TRPC_LOCAL_WECOM_HA_ENTRY_PORT:-58087}:8080"
    depends_on:
      wecom-ha-node-a:
        condition: service_started
      wecom-ha-node-b:
        condition: service_started

volumes:
```


## 运行时排空和文件契约

## SIGTERM 排空顺序

`signalCtx` 接收 SIGTERM/父 context 取消；活动工作使用独立 `processCtx`。主运行时先 `health.beginDrain()`，再 `drainGate.Begin()`，同时调用 HTTP Shutdown 并等待参与排空的角色退出。业务 HTTP 在排空期间返回 503，live/ready/status 路径可检查；ready 不会被迟到的成功轮询恢复成 true。

Worker、Delivery 和 Wakeup 的读取 context 先取消，活动工作 context 保留。预处理和 Relay 通过 drainPoll 停止下一批；Delivery Supervisor 停止 refresh/新建消费者，runUntilDrained 不重启已排空的消费者。活动进度发布在最终 processCtx 取消时退出。

默认排空 30 秒；Worker/Delivery 的局部超时装配为应用时限加 1 秒，让主运行时先控制全局期限。正常结束后取消 processCtx；超时则报告排空超时、取消工作并退出。未完成的 durable 工作留给存活节点。排空不是共享队列清零，也不会人工删除 lease。SIGKILL 完全跳过本流程。


```go
package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/reliability/drain"
)

var errRuntimeDrained = errors.New("runtime admissions stopped")

func drainPoll(gate *drain.Gate, operation func(context.Context, int) (int, error)) func(context.Context, int) (int, error) {
	return func(ctx context.Context, limit int) (int, error) {
		leave, ok := gate.Enter()
		if !ok {
			return 0, errRuntimeDrained
		}
		defer leave()
		return operation(ctx, limit)
	}
}

// Keep retrying failed consumers while serving. A completed drain is final:
// do not resurrect the consumer after it has stopped admitting work.
func runUntilDrained(ctx context.Context, signal <-chan struct{}, name string, operation func(context.Context) error, logger *roleLogger) error {
	retryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	err := runRecoverableLoop(retryCtx, name, func(runCtx context.Context) error {
		err := operation(runCtx)
		if drain.Signaled(signal) {
			cancel()
		}
		return err
	}, 250*time.Millisecond, logger)
	if ctx.Err() == nil && drain.Signaled(signal) {
		return nil
	}
	return err
}

func drainingHTTP(next http.Handler, gate *drain.Gate) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gate.Draining() && r.URL.Path != "/livez" && r.URL.Path != "/readyz" && r.URL.Path != "/statusz" {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "instance draining", http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}
```


```go
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func ensureSkillDirectory(path string, readOnly bool) error {
	if readOnly {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("shared skill directory unavailable: %w", err)
		}
		if !info.IsDir() {
			return errors.New("shared skill path is not a directory")
		}
		return nil
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return os.Chmod(path, 0o700)
}

// Only the bootstrap writer may publish immutable package content. Publication
// is atomic and never overwrites an existing version, even across processes.
func ensureSkillFile(stagingRoot, path string, content []byte, readOnly bool) error {
	check := func() error {
		existing, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !bytes.Equal(existing, content) {
			return errors.New("published skill content does not match this binary")
		}
		return nil
	}
	if err := check(); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if readOnly {
		return errors.New("shared skill package is missing; run bootstrap before starting readers")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// Keep unfinished files outside every published package's digest root.
	publishing := filepath.Join(stagingRoot, ".publishing")
	if err := os.MkdirAll(publishing, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(publishing, ".skill-publish-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(content); err != nil {
		file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Link(file.Name(), path); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	return check()
}
```


## 配置契约

| 参数 | 当前值/要求 |
|---|---|
| TRPC_POSTGRES_DSN / TRPC_REDIS_ADDRESS | 共享数据库与 Redis，两个实例须相同。 |
| TRPC_REDIS_ENVIRONMENT | 默认 local-runtime；同一部署协调命名空间相同，各环境不得误共用。 |
| TRPC_WEBUI_LOCAL_INSTANCE_ID | standalone 默认；Compose 每节点不同。 |
| TRPC_WEBUI_LOCAL_STATUS_ENABLED | true 才注册 status；owner 启动身份始终生成。 |
| TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT | 默认 /tmp/trpc-webui-skills；多实例 /var/lib/trpc-webui-local/skills。 |
| TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY | 节点 true；bootstrap false。 |
| TRPC_WEBUI_LOCAL_SECRET_ROOT | 默认 /tmp/trpc-webui-secrets，每容器私有。 |
| TRPC_RUNTIME_DRAIN_TIMEOUT | 默认30s，允许1s–5m。 |
| TRPC_RUNTIME_OPERATIONS_TOKEN | 独立管理 token，至少32字符，不复用 WebUI token；为空不注册 ops。 |
| TRPC_WECOM_LOCAL_ENABLED | 主 Bot 渠道开关。 |
| TRPC_WECOM_SECONDARY_LOCAL_ENABLED | 次 Bot 渠道开关；需要独立 Corp/Agent 组合与全套凭据。 |
| TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED | 仅隔离演练环境 true；默认不开故障控制面。 |

Bot 凭据通过 Compose secrets/wecom.env 注入，模型 key 通过 secrets/deepseek-api-key。配置名和鉴权严格以组合根为准；不要把脚本的 assumed 当成运行时配置或业务成功状态。

## 启动与端口

先建共享依赖并应用全部迁移，再运行bootstrap；bootstrap成功后节点启动并真实轮询就绪，最后入口纳入后端。外部Bot只登记入口58087，A58088/B58089为本地观测端口；容器内均8080。WebUI multinode使用58083/58084，与WeCom拓扑区分。

| 参数 | 当前本地组合根装配 | 意义 |
|---|---|---|
| Worker lease / renew | 30s / 10s | 失权取消执行；lease 到期才可换 owner。 |
| Worker reclaim / limit | 5s / 100 | 扫描 pending；不代表 5 秒内必完成恢复。 |
| Relay claim / renew / poll | 30s / 10s / 100ms | 先发布，再标记 published；发布后崩溃可重发同一事件。 |
| Delivery claim / renew | 30s / 10s | 每分段发送权。 |
| Delivery reclaim / limit | 5s / 100 | 回收账号回复队列 pending。 |
| Delivery retry | 初始 1s，最大 1m，最多 8 次 | 明确可重试错误的有界退避。 |
| Delivery reconcile | 最多 8 次 | 无查询能力或核对失败最终保留 failed/reconcile_exhausted 事件。 |
| 工具 lease / renew | 默认 30s / TTL÷3 | PostgreSQL 工具 lease，与会话 lease 是两套执行权。 |
| 应用排空 | 默认 30s，可配置 1s–5m | 停止新领取，保留已接纳工作，超时再取消。 |
| Compose 停止宽限 | 默认 45s | 必须比应用排空更长并留退出余量。 |

这些值是当前 `webui-local`/`wecom-local` 装配值，不能与 Consumer 未配置时的包级兜底值混用。恢复耗时还包含排队、上游延迟、退避和采样误差，不能把 TTL 相加写成已验证的 RTO。

## 脚本接口

生命周期脚本 `bash scripts/e2e/single-host-multicontainer.sh`，需要Docker Compose v2、curl和真实密钥文件。wecom-ha-failover.sh直接exec同脚本。

| 脚本参数 | 默认 |
|---|---|
| TRPC_SINGLE_HOST_PROJECT | 随机项目名 |
| TRPC_LOCAL_WECOM_HA_ENTRY_PORT / NODE_A_PORT / NODE_B_PORT | 58087 / 58088 / 58089 |
| TRPC_SINGLE_HOST_TIMEOUT_SECONDS | 120 |
| TRPC_SINGLE_HOST_STABILITY_SECONDS | 5 |
| TRPC_SINGLE_HOST_KEEP_ENVIRONMENT | false |
| TRPC_WECOM_REAL_ACCEPTANCE | assumed；recorded仅声明外部已存真实证据 |

这些是脚本环境变量，不是运行时“已通过”开关。脚本按轮询等待，timeout_seconds是尝试次数口径，包含curl时长后不等于精确秒数RTO。执行完成或失败均保留独立诊断目录；未keep时会清理脚本创建的测试项目及卷。

## 管理接口与不确定投递处置

设置独立的 `TRPC_RUNTIME_OPERATIONS_TOKEN`（至少 32 字符、不得复用 WebUI token）才注册管理接口。使用请求头 `X-TRPC-Local-Token`，直接访问节点；稳定 WeCom 入口不转发 `/ops/*`。响应 `Cache-Control: no-store`，查询显式带 tenant，不返回正文或密钥。

| 接口 | 参数 / 作用 |
|---|---|
| GET /ops/reliability | tenant_id；单条 SQL 一致快照，含 Inbox/执行/Outbox/Ledger/工具计数、最近执行尝试和异常/处置；明细各最多 100 条。 |
| GET /ops/session/lease | tenant_id、agent_app_id、session_id；原子读 owner/fence/剩余租期，不返回 LeaseID。 |
| POST /ops/delivery/resolve | 外部证据核实后的版本化处置；冲突 409、输入错误 400、未授权 401、成功 204。 |

处置输入为 `tenant_id, delivery_key, segment_no, expected_version, actor, evidence_ref, decision`。decision=delivered 必须带 provider_message_id；另外两种 decision 必须不带。actor 是管理调用方声明的操作人。

| decision | 状态与调度 |
|---|---|
| delivered | 分段置 sent；重调度原 reply Outbox 以继续剩余段，当前段只跳过发送。 |
| not_delivered | 分段置 failed，关闭事件，不补发。 |
| retry_not_delivered | 有未送达证据后置 retry_wait，重调度原 reply，沿用 DeliveryKey/ClientRequestID/内容与累计尝试数。 |

只能处置 ambiguous 或 failed/reconcile_exhausted。Ledger CAS、delivery_resolution 审计和必要的原 Outbox 重调度同事务提交；Outbox 正被领取时返回冲突，不能手改状态绕过。超时不能证明未送达。

## 组合根完整配置校验原文

以下为当前 loadWebUILocalConfig；它给出全部本地运行时环境变量、默认值、开关、密钥路径及合法组合，不包含运行环境注入的实际密钥值。

```go
func loadWebUILocalConfig(getenv func(string) string) (webUILocalConfig, error) {
	if strings.EqualFold(strings.TrimSpace(getenv("TRPC_FEISHU_LOCAL_ENABLED")), "true") {
		return webUILocalConfig{}, errors.New("Feishu local runtime is no longer supported")
	}
	value := webUILocalConfig{PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		DrainTimeout:    30 * time.Second,
		ReadOnlySkills:  strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_SKILLS_READ_ONLY")), "true"),
		OperationsToken: getenv("TRPC_RUNTIME_OPERATIONS_TOKEN"),
		RedisAddress:    strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")), ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		RedisEnvironment:             valueOr(getenv("TRPC_REDIS_ENVIRONMENT"), "local-runtime"),
		SecretRoot:                   valueOr(getenv("TRPC_WEBUI_LOCAL_SECRET_ROOT"), "/tmp/trpc-webui-secrets"),
		SkillStagingRoot:             valueOr(getenv("TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT"), "/tmp/trpc-webui-skills"),
		QdrantEndpoint:               valueOr(getenv("TRPC_WEBUI_LOCAL_QDRANT_ENDPOINT"), "http://qdrant:6333"),
		APIKeyFile:                   valueOr(getenv("TRPC_WEBUI_DEEPSEEK_KEY_FILE"), "/run/secrets/deepseek_api_key"),
		RouteKey:                     valueOr(getenv("TRPC_WEBUI_LOCAL_ROUTE_KEY"), webUILocalRouteKey),
		Token:                        valueOr(getenv("TRPC_WEBUI_LOCAL_TOKEN"), webUILocalToken),
		InstanceID:                   valueOr(getenv("TRPC_WEBUI_LOCAL_INSTANCE_ID"), "standalone"),
		ClamAVAddress:                valueOr(getenv("TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS"), "clamav:3310"),
		ExclusiveRuntime:             strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_EXCLUSIVE_RUNTIME")), "true"),
		FailoverTestEnabled:          strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED")), "true"),
		StatusEnabled:                strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_STATUS_ENABLED")), "true"),
		FeishuEnabled:                strings.EqualFold(strings.TrimSpace(getenv("TRPC_FEISHU_LOCAL_ENABLED")), "true"),
		FeishuAppID:                  strings.TrimSpace(getenv("FEISHU_APP_ID")),
		FeishuAppSecret:              strings.TrimSpace(getenv("FEISHU_APP_SECRET")),
		FeishuVerificationToken:      strings.TrimSpace(getenv("FEISHU_VERIFICATION_TOKEN")),
		FeishuEncryptKey:             strings.TrimSpace(getenv("FEISHU_ENCRYPT_KEY")),
		FeishuBotOpenID:              strings.TrimSpace(getenv("FEISHU_BOT_OPEN_ID")),
		WeComEnabled:                 strings.EqualFold(strings.TrimSpace(getenv("TRPC_WECOM_LOCAL_ENABLED")), "true"),
		WeComSecondaryEnabled:        strings.EqualFold(strings.TrimSpace(getenv("TRPC_WECOM_SECONDARY_LOCAL_ENABLED")), "true"),
		WeComCorpID:                  strings.TrimSpace(getenv("WECOM_CORP_ID")),
		WeComAppSecret:               strings.TrimSpace(getenv("WECOM_APP_SECRET")),
		WeComCallbackToken:           strings.TrimSpace(getenv("WECOM_CALLBACK_TOKEN")),
		WeComEncodingAESKey:          strings.TrimSpace(getenv("WECOM_ENCODING_AES_KEY")),
		WeComSecondaryCorpID:         strings.TrimSpace(getenv("WECOM_SECONDARY_CORP_ID")),
		WeComSecondaryAppSecret:      strings.TrimSpace(getenv("WECOM_SECONDARY_APP_SECRET")),
		WeComSecondaryCallbackToken:  strings.TrimSpace(getenv("WECOM_SECONDARY_CALLBACK_TOKEN")),
		WeComSecondaryEncodingAESKey: strings.TrimSpace(getenv("WECOM_SECONDARY_ENCODING_AES_KEY")),
	}
	if raw := strings.TrimSpace(getenv("TRPC_RUNTIME_DRAIN_TIMEOUT")); raw != "" {
		duration, err := time.ParseDuration(raw)
		if err != nil || duration < time.Second || duration > 5*time.Minute {
			return webUILocalConfig{}, errors.New("invalid runtime drain timeout")
		}
		value.DrainTimeout = duration
	}
	if value.OperationsToken != "" && (len(value.OperationsToken) < 32 || strings.TrimSpace(value.OperationsToken) != value.OperationsToken || value.OperationsToken == value.Token) {
		return webUILocalConfig{}, errors.New("operations token must be independent and contain at least 32 characters")
	}
	if value.PostgresDSN == "" || value.RedisAddress == "" || strings.TrimSpace(value.Token) != value.Token || len(value.Token) < 16 ||
		strings.TrimSpace(value.RouteKey) != value.RouteKey || value.RouteKey == "" || strings.TrimSpace(value.ClamAVAddress) != value.ClamAVAddress || value.ClamAVAddress == "" || !filepath.IsAbs(value.APIKeyFile) || !filepath.IsAbs(value.SecretRoot) ||
		!filepath.IsAbs(value.SkillStagingRoot) || filepath.Clean(value.SkillStagingRoot) != value.SkillStagingRoot || value.SkillStagingRoot == value.SecretRoot || value.QdrantEndpoint != "http://qdrant:6333" {
		return webUILocalConfig{}, errors.New("required WebUI local configuration is missing or invalid")
	}
	if !validWebUILocalInstanceID(value.InstanceID) {
		return webUILocalConfig{}, errors.New("WebUI local instance ID is invalid")
	}
	if value.FeishuEnabled && (value.FeishuAppID == "" || value.FeishuAppSecret == "" || value.FeishuVerificationToken == "" || value.FeishuEncryptKey == "") {
		return webUILocalConfig{}, errors.New("Feishu local configuration is incomplete")
	}
	if value.WeComEnabled {
		agentID, agentIDErr := strconv.ParseInt(strings.TrimSpace(getenv("WECOM_AGENT_ID")), 10, 64)
		if agentIDErr != nil || agentID <= 0 || value.WeComCorpID == "" || value.WeComAppSecret == "" || value.WeComCallbackToken == "" ||
			value.WeComEncodingAESKey == "" {
			return webUILocalConfig{}, errors.New("WeCom local configuration is incomplete")
		}
		value.WeComAgentID = agentID
	}
	secondaryConfigured := value.WeComSecondaryCorpID != "" || strings.TrimSpace(getenv("WECOM_SECONDARY_AGENT_ID")) != "" || value.WeComSecondaryAppSecret != "" ||
		value.WeComSecondaryCallbackToken != "" || value.WeComSecondaryEncodingAESKey != ""
	if !value.WeComSecondaryEnabled && secondaryConfigured {
		return webUILocalConfig{}, errors.New("secondary WeCom credentials require explicit enablement")
	}
	if value.WeComSecondaryEnabled {
		if !value.WeComEnabled {
			return webUILocalConfig{}, errors.New("secondary WeCom requires the primary WeCom local configuration")
		}
		agentID, agentIDErr := strconv.ParseInt(strings.TrimSpace(getenv("WECOM_SECONDARY_AGENT_ID")), 10, 64)
		if agentIDErr != nil || agentID <= 0 || value.WeComSecondaryCorpID == "" || value.WeComSecondaryAppSecret == "" ||
			value.WeComSecondaryCallbackToken == "" || value.WeComSecondaryEncodingAESKey == "" ||
			(value.WeComSecondaryCorpID == value.WeComCorpID && agentID == value.WeComAgentID) {
			return webUILocalConfig{}, errors.New("secondary WeCom local configuration is incomplete or incompatible")
		}
		value.WeComSecondaryAgentID = agentID
	}
	return value, nil
}
```
