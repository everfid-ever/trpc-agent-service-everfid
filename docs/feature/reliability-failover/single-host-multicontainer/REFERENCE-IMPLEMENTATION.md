# 单主机多容器实例可用性 — 自足复现参考实现

> **本文是自足复现材料**：无需阅读仓库源码，仅照抄本章即可搭出等价拓扑并通过同等验收。所有标识符、端口、环境变量、HTTP 路径、状态码均与实现代码一致（可选核对：`cmd/trpc-service/wecom_ha_entry_role.go`、`cmd/trpc-service/webui_local_role.go`、`deploy/compose/docker-compose.local.yml`、`scripts/e2e/single-host-multicontainer.sh`）。
>
> **唯一对外端口**：`wecom-ha-entry` 默认宿主 `58087`。两个 Bot 回调 URL 都填这个 origin，靠 route key（`local-wecom` / `local-wecom-secondary`）区分。**node-a `58088` / node-b `58089` 仅本地观测，不得登记为微信回调。**

---

## 1. 完整入口参考实现（Go）

下面是 `wecom-ha-entry` 的**完整可落地**参考实现，覆盖全部 HTTP 路径与状态码语义。其结构与仓库 `wecom_ha_entry_role.go` 一致。

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
	"strings"
	"sync/atomic"
	"time"
)

type weComHAEntryConfig struct {
	ListenAddress string
	Backends      []*url.URL
	ProbeInterval time.Duration
	RequestLimit  int64
}

type weComHAEntryBackend struct {
	URL     *url.URL
	healthy atomic.Bool
}

type weComHAEntryPool struct {
	backends []*weComHAEntryBackend
	client   *http.Client
	next     atomic.Uint64
	limit    int64
}

type weComHAEntryStatus struct {
	Backends []weComHAEntryBackendStatus `json:"backends"`
}

type weComHAEntryBackendStatus struct {
	URL     string `json:"url"`
	Healthy bool   `json:"healthy"`
}

// 配置加载：TRPC_LISTEN_ADDRESS 默认 :8080；
// TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL 默认 1s，范围 [100ms,1m]；
// TRPC_WECOM_HA_ENTRY_BACKENDS 必须 >=2、http、无 user/query/fragment、去重。
func loadWeComHAEntryConfig(getenv func(string) string) (weComHAEntryConfig, error) {
	if getenv == nil {
		return weComHAEntryConfig{}, errors.New("entry environment is unavailable")
	}
	value := weComHAEntryConfig{
		ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		ProbeInterval: time.Second,
		RequestLimit:  2 << 20, // 2 MiB
	}
	if strings.TrimSpace(value.ListenAddress) != value.ListenAddress || value.ListenAddress == "" {
		return weComHAEntryConfig{}, errors.New("entry listen address is invalid")
	}
	if raw := strings.TrimSpace(getenv("TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL")); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval < 100*time.Millisecond || interval > time.Minute {
			return weComHAEntryConfig{}, errors.New("entry probe interval is invalid")
		}
		value.ProbeInterval = interval
	}
	seen := map[string]struct{}{}
	for _, raw := range strings.Split(getenv("TRPC_WECOM_HA_ENTRY_BACKENDS"), ",") {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || parsed.Scheme != "http" || parsed.Host == "" ||
			parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return weComHAEntryConfig{}, errors.New("entry backend is invalid")
		}
		parsed.Path = strings.TrimSuffix(parsed.Path, "/")
		key := parsed.String()
		if _, ok := seen[key]; ok {
			return weComHAEntryConfig{}, errors.New("entry backend is duplicated")
		}
		seen[key] = struct{}{}
		value.Backends = append(value.Backends, parsed)
	}
	if len(value.Backends) < 2 {
		return weComHAEntryConfig{}, errors.New("entry requires at least two backends")
	}
	return value, nil
}

func newWeComHAEntryPool(config weComHAEntryConfig, client *http.Client) (*weComHAEntryPool, error) {
	if len(config.Backends) < 2 || config.RequestLimit < 1 {
		return nil, errors.New("entry pool configuration is invalid")
	}
	if client == nil {
		client = &http.Client{
			Timeout:         10 * time.Second,
			CheckRedirect:   func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	pool := &weComHAEntryPool{
		client:   client,
		limit:    config.RequestLimit,
		backends: make([]*weComHAEntryBackend, 0, len(config.Backends)),
	}
	for _, backend := range config.Backends {
		if backend == nil {
			return nil, errors.New("entry backend is invalid")
		}
		copyURL := *backend
		pool.backends = append(pool.backends, &weComHAEntryBackend{URL: &copyURL})
	}
	return pool, nil
}

// probe：对每个 backend 请求 <backend>/readyz；HTTP 200 → healthy。
func (p *weComHAEntryPool) probe(ctx context.Context) {
	if p == nil || p.client == nil {
		return
	}
	for _, backend := range p.backends {
		probeURL := *backend.URL
		probeURL.Path = strings.TrimSuffix(probeURL.Path, "/") + "/readyz"
		probeURL.RawQuery = ""
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL.String(), nil)
		if err != nil {
			backend.healthy.Store(false)
			continue
		}
		response, err := p.client.Do(request)
		if err != nil {
			backend.healthy.Store(false)
			continue
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		backend.healthy.Store(response.StatusCode == http.StatusOK)
	}
}

// healthyBackends：从原子轮转下标开始按序返回健康后端（round-robin 起点）。
func (p *weComHAEntryPool) healthyBackends() []*weComHAEntryBackend {
	if p == nil || len(p.backends) == 0 {
		return nil
	}
	ordered := make([]*weComHAEntryBackend, 0, len(p.backends))
	start := int(p.next.Add(1)-1) % len(p.backends)
	for offset := range p.backends {
		candidate := p.backends[(start+offset)%len(p.backends)]
		if candidate.healthy.Load() {
			ordered = append(ordered, candidate)
		}
	}
	return ordered
}

func (p *weComHAEntryPool) status() weComHAEntryStatus {
	status := weComHAEntryStatus{Backends: make([]weComHAEntryBackendStatus, 0, len(p.backends))}
	for _, backend := range p.backends {
		status.Backends = append(status.Backends, weComHAEntryBackendStatus{
			URL:     backend.URL.String(),
			Healthy: backend.healthy.Load(),
		})
	}
	return status
}

// serveCallback：仅 GET/POST；MaxBytesReader 超限 413；遍历健康后端转发；
// forward 出错或返回 502/503/504 → 标 unhealthy 试下一个；全失败 → 503。
func (p *weComHAEntryPool) serveCallback(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.Body == nil {
		request.Body = http.NoBody
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, p.limit))
	if err != nil {
		http.Error(writer, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	for _, backend := range p.healthyBackends() {
		response, forwardErr := p.forward(request, body, backend)
		if forwardErr != nil {
			backend.healthy.Store(false)
			continue
		}
		if response.StatusCode == http.StatusBadGateway ||
			response.StatusCode == http.StatusServiceUnavailable ||
			response.StatusCode == http.StatusGatewayTimeout {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			backend.healthy.Store(false)
			continue
		}
		copyResponse(writer, response)
		return
	}
	http.Error(writer, "no ready callback backend", http.StatusServiceUnavailable)
}

func (p *weComHAEntryPool) forward(request *http.Request, body []byte, backend *weComHAEntryBackend) (*http.Response, error) {
	target := *backend.URL
	target.Path = strings.TrimSuffix(target.Path, "/") + request.URL.Path
	target.RawQuery = request.URL.RawQuery
	forwarded, err := http.NewRequestWithContext(request.Context(), request.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	forwarded.Header = request.Header.Clone()
	forwarded.Header.Del("Connection")
	forwarded.Header.Del("Proxy-Connection")
	forwarded.Header.Del("Transfer-Encoding")
	forwarded.Header.Set("X-Forwarded-For", request.RemoteAddr)
	forwarded.Header.Set("X-Forwarded-Proto", "http")
	forwarded.ContentLength = int64(len(body))
	return p.client.Do(forwarded)
}

func copyResponse(writer http.ResponseWriter, response *http.Response) {
	defer response.Body.Close()
	for key, values := range response.Header {
		if strings.EqualFold(key, "Connection") || strings.EqualFold(key, "Transfer-Encoding") {
			continue
		}
		for _, value := range values {
			writer.Header().Add(key, value)
		}
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = io.Copy(writer, response.Body)
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

// runWeComHAEntryRole：启动探测循环并注册路由。
func runWeComHAEntryRole(parent context.Context, getenv func(string) string, logger interface{ Printf(string, ...any) }) error {
	config, err := loadWeComHAEntryConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	pool, err := newWeComHAEntryPool(config, nil)
	if err != nil {
		return err
	}
	probeCtx, cancelProbe := context.WithTimeout(parent, 3*time.Second)
	pool.probe(probeCtx)
	cancelProbe()
	go func() {
		ticker := time.NewTicker(config.ProbeInterval)
		defer ticker.Stop()
		for {
			select {
			case <-parent.Done():
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(parent, 3*time.Second)
				pool.probe(ctx)
				cancel()
			}
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, _ *http.Request) {
		if len(pool.healthyBackends()) == 0 {
			http.Error(writer, "no ready callback backend", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/statusz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(writer).Encode(pool.status())
	})
	mux.HandleFunc("/callbacks/wecom", pool.serveCallback)
	server := &http.Server{
		Addr: config.ListenAddress, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	go func() { _ = server.ListenAndServe() }()
	<-parent.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	return nil
}
```

### 1.1 入口 HTTP 路径与状态码语义

| 路径 | 方法 | 行为 | 状态码 |
|---|---|---|---|
| `/livez` | any | 恒活 | 200 |
| `/readyz` | GET | 仅当存在健康后端 | 200 / 503 `no ready callback backend` |
| `/statusz` | GET | 输出 `{"backends":[{"url","healthy"}]}` | 200 JSON |
| `/callbacks/wecom` | GET/POST | 按健康后端轮转转发 | 405（方法不符）/ 413（超限）/ 503（无健康后端或后端 502/503/504）/ 后端原始码 |

> 入口重试**不替代业务幂等**：只在本次 callback 尚未返回响应前换后端；真正去重靠后端 durable Inbox（`claim_inbox` / `prepare_dispatch` 唯一键收敛）。

---

## 2. 节点侧 `/statusz` 契约（复现时需保证同形输出）

节点（`wecom-local`）暴露：

| 路径 | 方法 | 行为 | 状态码 |
|---|---|---|---|
| `/livez` | any | 恒活 | 200 |
| `/readyz` | GET | `db.PingContext` + `redis.Ping` + `malware.Probe` 全成功 | 200 / 503 `not ready` |
| `/statusz` | GET | 实例身份 + 7 组件 owner + barrier 状态 | 200 JSON |
| `/callbacks/wecom` | POST | WeCom 回调入口（存在 endpoint 时） | 业务码 |
| `/test/failover/barrier/release` | POST | 仅 barrier 非 nil；须 `X-TRPC-Local-Token` 匹配 | 204 / 404 |

`/statusz` 固定 JSON 形状：

```json
{
  "instance_id": "wecom-ha-node-a",
  "process_start_id": "9f3c1a2b4d5e6f07",
  "owners": {
    "worker": "webui-local-worker-wecom-ha-node-a-9f3c1a2b4d5e6f07",
    "preprocess": "webui-local-preprocess-wecom-ha-node-a-9f3c1a2b4d5e6f07",
    "dispatch-relay": "webui-local-dispatch-relay-wecom-ha-node-a-9f3c1a2b4d5e6f07",
    "reply-relay": "webui-local-reply-relay-wecom-ha-node-a-9f3c1a2b4d5e6f07",
    "wakeup-relay": "webui-local-wakeup-relay-wecom-ha-node-a-9f3c1a2b4d5e6f07",
    "wakeup": "webui-local-wakeup-wecom-ha-node-a-9f3c1a2b4d5e6f07",
    "delivery": "webui-local-delivery-wecom-ha-node-a-9f3c1a2b4d5e6f07"
  },
  "inflight_test_barrier": {"enabled": false}
}
```

复现要点：

- `instance_id` 必须等于该节点 `TRPC_WEBUI_LOCAL_INSTANCE_ID`。
- `process_start_id` 每次进程启动必须**重新随机**（8 字节 → 16 hex），这是"重新加入 ≠ 旧 owner 复活"的判定依据。
- `owners` 的 7 个 key 固定为 `worker`/`preprocess`/`dispatch-relay`/`reply-relay`/`wakeup-relay`/`wakeup`/`delivery`，值前缀 `webui-local-<component>-<instance_id>-<process_start_id>`。
- `inflight_test_barrier.enabled` 默认 `false`（生产安全默认）。

---

## 3. 完整 Compose 服务定义（可直接照抄）

以下是从 `deploy/compose/docker-compose.local.yml`（`wecom-ha-local` profile）抽取并补全的服务定义。**只需把本段放入你的 Compose 文件并启用 profile `wecom-ha-local` 即可。**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: ${TRPC_LOCAL_POSTGRES_USER:-postgres}
      POSTGRES_PASSWORD: ${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}
      POSTGRES_DB: ${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}
    ports: ["${TRPC_LOCAL_POSTGRES_PORT:-55432}:5432"]
    volumes: [local-postgres:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 3s; timeout: 3s; retries: 20

  redis:
    image: redis:7
    ports: ["${TRPC_LOCAL_REDIS_PORT:-56379}:6379"]
    volumes: [local-redis:/data]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 3s; timeout: 3s; retries: 20

  qdrant:
    image: qdrant/qdrant:v1.13.6
    profiles: ["wecom-ha-local"]
    volumes: [local-qdrant:/qdrant/storage]
    healthcheck:
      test: ["CMD-SHELL", "bash -ec 'exec 3<>/dev/tcp/127.0.0.1/6333; printf \"GET /healthz HTTP/1.1\\r\\nHost: localhost\\r\\nConnection: close\\r\\n\\r\\n\" >&3; IFS= read -r line <&3; [[ \"$$line\" == *\" 200 \"* ]]'"]
      interval: 2s; timeout: 3s; retries: 30

  clamav:
    image: clamav/clamav:1.4.6-debian13-slim
    profiles: ["wecom-ha-local"]
    healthcheck:
      test: ["CMD-SHELL", "clamdscan --ping=1 >/dev/null 2>&1"]
      interval: 5s; timeout: 5s; retries: 36

  otel-collector:
    image: otel/opentelemetry-collector-contrib:0.108.0
    profiles: ["wecom-ha-local"]
    command: ["--config=/etc/otelcol-contrib/config.yml"]
    volumes: [./otel-collector.yml:/etc/otelcol-contrib/config.yml:ro]
    ports: ["${TRPC_LOCAL_OTEL_HTTP_PORT:-54318}:4318", "${TRPC_LOCAL_OTEL_PROMETHEUS_PORT:-59464}:9464"]
    depends_on: { jaeger: { condition: service_started } }

  jaeger:
    image: jaegertracing/all-in-one:1.60
    profiles: ["wecom-ha-local"]
    environment: { COLLECTOR_OTLP_ENABLED: "true" }
    ports: ["${TRPC_LOCAL_JAEGER_PORT:-56686}:16686"]

  webui-local-skills-init:
    image: busybox:1.36.1
    profiles: ["wecom-ha-local"]
    user: "0:0"
    command: ["sh", "-ec", "chown 65532:65532 /var/lib/trpc-webui-local/skills && chmod 0700 /var/lib/trpc-webui-local/skills"]
    volumes: [webui-local-skills:/var/lib/trpc-webui-local/skills]

  # 一次性初始化共享 tenant/config fixture
  wecom-ha-bootstrap:
    build: { context: ../.., dockerfile: deploy/compose/Dockerfile }
    profiles: ["wecom-ha-local"]
    command: ["webui-local-bootstrap"]
    env_file:
      - path: .env.local
        required: false
      - path: secrets/wecom.env
        required: true
    environment:
      TRPC_POSTGRES_DSN: postgres://${TRPC_LOCAL_POSTGRES_USER:-postgres}:${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}@postgres:5432/${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}?sslmode=disable
      TRPC_REDIS_ADDRESS: redis:6379
      TRPC_WECOM_LOCAL_ENABLED: "true"
      TRPC_WECOM_SECONDARY_LOCAL_ENABLED: "true"
      TRPC_WEBUI_DEEPSEEK_KEY_FILE: /run/secrets/deepseek_api_key
      TRPC_WEBUI_LOCAL_INSTANCE_ID: wecom-ha-bootstrap
      TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT: /var/lib/trpc-webui-local/skills
    secrets: [deepseek_api_key]
    volumes: [webui-local-skills:/var/lib/trpc-webui-local/skills]
    depends_on:
      webui-local-skills-init: { condition: service_completed_successfully }
      postgres: { condition: service_healthy }
      redis: { condition: service_healthy }
      qdrant: { condition: service_healthy }

  # N1
  wecom-ha-node-a:
    build: { context: ../.., dockerfile: deploy/compose/Dockerfile }
    profiles: ["wecom-ha-local"]
    command: ["wecom-local"]
    restart: "no"
    env_file:
      - path: .env.local
        required: false
      - path: secrets/wecom.env
        required: true
    environment: &wecom-ha-runtime-environment
      TRPC_POSTGRES_DSN: postgres://${TRPC_LOCAL_POSTGRES_USER:-postgres}:${TRPC_LOCAL_POSTGRES_PASSWORD:-postgres}@postgres:5432/${TRPC_LOCAL_POSTGRES_DB:-trpc_agent_service_test}?sslmode=disable
      TRPC_REDIS_ADDRESS: redis:6379
      TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS: clamav:3310
      TRPC_LISTEN_ADDRESS: ":8080"
      TRPC_WECOM_LOCAL_ENABLED: "true"
      TRPC_WECOM_SECONDARY_LOCAL_ENABLED: "true"
      TRPC_WEBUI_DEEPSEEK_KEY_FILE: /run/secrets/deepseek_api_key
      TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT: /var/lib/trpc-webui-local/skills
      TRPC_OTEL_ENDPOINT: http://otel-collector:4318
      TRPC_OTEL_ALLOW_INSECURE: "true"
      TRPC_SERVICE_VERSION: ${TRPC_LOCAL_SERVICE_VERSION:-development}
      TRPC_WEBUI_LOCAL_INSTANCE_ID: wecom-ha-node-a
      TRPC_INFLIGHT_TEST_BARRIER: ${TRPC_INFLIGHT_TEST_BARRIER:-}
      TRPC_INFLIGHT_TEST_BARRIER_INSTANCE_ID: ${TRPC_INFLIGHT_TEST_BARRIER_INSTANCE_ID:-}
      TRPC_INFLIGHT_TEST_BARRIER_TENANT_ID: ${TRPC_INFLIGHT_TEST_BARRIER_TENANT_ID:-}
    secrets: [deepseek_api_key]
    volumes: [webui-local-skills:/var/lib/trpc-webui-local/skills]
    ports: ["${TRPC_LOCAL_WECOM_HA_NODE_A_PORT:-58088}:8080"]
    depends_on:
      wecom-ha-bootstrap: { condition: service_completed_successfully }
      clamav: { condition: service_healthy }
      otel-collector: { condition: service_started }

  # N2（继承 &wecom-ha-runtime-environment，仅改 INSTANCE_ID）
  wecom-ha-node-b:
    build: { context: ../.., dockerfile: deploy/compose/Dockerfile }
    profiles: ["wecom-ha-local"]
    command: ["wecom-local"]
    restart: "no"
    env_file:
      - path: .env.local
        required: false
      - path: secrets/wecom.env
        required: true
    environment:
      <<: *wecom-ha-runtime-environment
      TRPC_WEBUI_LOCAL_INSTANCE_ID: wecom-ha-node-b
    secrets: [deepseek_api_key]
    volumes: [webui-local-skills:/var/lib/trpc-webui-local/skills]
    ports: ["${TRPC_LOCAL_WECOM_HA_NODE_B_PORT:-58089}:8080"]
    depends_on:
      wecom-ha-bootstrap: { condition: service_completed_successfully }
      clamav: { condition: service_healthy }
      otel-collector: { condition: service_started }

  # 唯一对外端口
  wecom-ha-entry:
    build: { context: ../.., dockerfile: deploy/compose/Dockerfile }
    profiles: ["wecom-ha-local"]
    command: ["wecom-ha-entry"]
    restart: "no"
    environment:
      TRPC_LISTEN_ADDRESS: ":8080"
      TRPC_WECOM_HA_ENTRY_BACKENDS: http://wecom-ha-node-a:8080,http://wecom-ha-node-b:8080
      TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL: 1s
    ports: ["${TRPC_LOCAL_WECOM_HA_ENTRY_PORT:-58087}:8080"]
    depends_on:
      wecom-ha-node-a: { condition: service_started }
      wecom-ha-node-b: { condition: service_started }

volumes:
  local-postgres:
  local-redis:
  local-qdrant:
  webui-local-skills:

secrets:
  deepseek_api_key:
    file: ./secrets/deepseek-api-key
```

> 说明：节点 `env_file: secrets/wecom.env` 提供两个 Bot 的凭据（`WECOM_*` / `WECOM_SECONDARY_*`）。入口**不**需要该文件，仅依赖 `TRPC_WECOM_HA_ENTRY_*` 环境变量。两者都依赖 `webui-local-skills` 卷与 `deepseek_api_key` secret。

---

## 4. 全部环境变量与默认值（本子系统实际使用的）

| 变量 | 作用 | 默认/约束 |
|---|---|---|
| `TRPC_WEBUI_LOCAL_INSTANCE_ID` | 部署槽位身份 | N1=`wecom-ha-node-a`，N2=`wecom-ha-node-b`，必须不同 |
| `ProcessStartID`（进程内） | 每次启动随机 8 字节 hex | 不可配置，自动生成 |
| `TRPC_POSTGRES_DSN` | 业务权威库 | 两节点必须相同 |
| `TRPC_REDIS_ADDRESS` | 队列与协调 | 两节点必须相同（`redis:6379`） |
| `TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS` | 媒体安全 | `clamav:3310` |
| `TRPC_LISTEN_ADDRESS` | 监听地址 | 默认 `:8080` |
| `TRPC_WECOM_LOCAL_ENABLED` | 主 Bot | `true` |
| `TRPC_WECOM_SECONDARY_LOCAL_ENABLED` | 次 Bot | `true` 且 (Corp ID, Agent ID) 必须与主 Bot 不同 |
| 主 Bot：`WECOM_CORP_ID`、`WECOM_AGENT_ID`、`WECOM_APP_SECRET`、`WECOM_CALLBACK_TOKEN`、`WECOM_ENCODING_AES_KEY` | 主 Bot 凭据 | 缺失即启动失败；`WECOM_AGENT_ID` 必须可解析为正整数 |
| 次 Bot：`WECOM_SECONDARY_CORP_ID`、`WECOM_SECONDARY_AGENT_ID`、`WECOM_SECONDARY_APP_SECRET`、`WECOM_SECONDARY_CALLBACK_TOKEN`、`WECOM_SECONDARY_ENCODING_AES_KEY` | 第二 Bot 凭据 | 任一缺失 → `secondary WeCom local configuration is incomplete`；`(Corp ID, Agent ID)` 与主 Bot 相同 → `... is incompatible` |
| `TRPC_WECOM_HA_ENTRY_BACKENDS` | 入口后端 | `http://wecom-ha-node-a:8080,http://wecom-ha-node-b:8080`，≥2，http，去重 |
| `TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL` | 探测周期 | 默认 `1s`，范围 `[100ms, 1m]` |
| `TRPC_LISTEN_ADDRESS`（入口） | 入口监听 | `:8080` |
| `TRPC_INFLIGHT_TEST_BARRIER` | P1–P4 暂停点 | 空=关闭（生产默认） |
| `TRPC_INFLIGHT_TEST_BARRIER_INSTANCE_ID` | 命中哪个槽位 | `auto` 或具体 instance id |
| `TRPC_INFLIGHT_TEST_BARRIER_TENANT_ID` | 限定租户 | 空=任意租户 |
| `TRPC_WEBUI_LOCAL_TOKEN` / `X-TRPC-Local-Token` | 控制面鉴权 | 仅本地演练 |
| `TRPC_WECOM_REAL_ACCEPTANCE` | 真实 WeCom 验收状态 | `assumed`（默认）/ `recorded` |
| `TRPC_LOCAL_WECOM_HA_ENTRY_PORT` | 入口宿主端口 | `58087` |
| `TRPC_LOCAL_WECOM_HA_NODE_A_PORT` | N1 宿主端口 | `58088` |
| `TRPC_LOCAL_WECOM_HA_NODE_B_PORT` | N2 宿主端口 | `58089` |

---

## 5. 端口分配与网络规则

| 端口（宿主） | 服务 | 容器端口 | 网络语义 | 登记为微信回调 |
|---|---|---|---|---|
| `58087` | `wecom-ha-entry` | `:8080` | **唯一对外**：HTTPS tunnel / 企业微信控制台指向此处 | ✅ 唯一允许 |
| `58088` | `wecom-ha-node-a` | `:8080` | 仅本地观测（`/statusz`/`/readyz` 调试） | ❌ 否 |
| `58089` | `wecom-ha-node-b` | `:8080` | 仅本地观测 | ❌ 否 |
| `55432` | `postgres` | `5432` | 共享权威态 | ❌ 否 |
| `56379` | `redis` | `6379` | 共享队列/协调 | ❌ 否 |

网络规则：

1. 企业微信控制台将两个 Bot 回调 URL 都设为 `https://<tunnel>/callbacks/wecom`（同一 public origin = 入口 `58087`）。
2. 同一 origin 下，主/次 Bot 靠 durable route key（`local-wecom` / `local-wecom-secondary`）区分，不依赖不同端口。
3. 入口与两节点在同一 Docker 用户自定义网络内，使用服务名 `wecom-ha-node-a:8080` / `wecom-ha-node-b:8080` 互访。
4. 节点宿主机端口只在本机 `127.0.0.1` 观测用；不要在防火墙/NAT 上将其暴露为企业微信可达地址。

---

## 6. 健康判定矩阵

| 探测方 | 探测目标 | 成功条件 | 失败后果 |
|---|---|---|---|
| 入口 `probe` | `<backend>/readyz` | HTTP 200 | `healthy=false`，从轮转集合摘除 |
| 入口 `/readyz` | `len(healthyBackends())>0` | 至少 1 健康后端 | 503 |
| 入口 `/livez` | 进程存活 | 恒 200 | — |
| 节点 `/readyz` | `db.PingContext` && `redis.Ping` && `malware.Probe` | 三者全成功 | 503 `not ready` |
| 节点 `/livez` | 进程存活 | 恒 200 | — |
| 脚本 `assert_stopped` | `docker inspect .State.Running` | `false` | 失败=受害被重启（违反接管前提） |
| 脚本 `wait_entry_backend` | 入口 `/statusz` 含 `"url":"http://<node>:8080","healthy":<bool>` | 子串匹配 | 超时失败 |

---

## 7. 从零搭建的分步命令

```bash
# 0) 进入仓库（或准备等价目录结构与上面第 3 节的 Compose）
cd /path/to/trpc-agent-service

# 1) 准备本地密钥（非空）
mkdir -p deploy/compose/secrets
printf 'your-deepseek-key' > deploy/compose/secrets/deepseek-api-key
# wecom.env 需包含两组 Bot 凭据（主 Bot）：
#   WECOM_CORP_ID / WECOM_AGENT_ID / WECOM_APP_SECRET / WECOM_CALLBACK_TOKEN / WECOM_ENCODING_AES_KEY
test -s deploy/compose/secrets/wecom.env || { echo "wecom.env required"; exit 2; }

# 2) 构建并拉起（profile wecom-ha-local）
export TRPC_LOCAL_WECOM_HA_ENTRY_PORT=58087
export TRPC_LOCAL_WECOM_HA_NODE_A_PORT=58088
export TRPC_LOCAL_WECOM_HA_NODE_B_PORT=58089
docker compose --profile wecom-ha-local up --detach --build

# 3) 等依赖与两节点 ready
for p in 58088 58089 58087; do
  until curl --fail --silent --show-error --max-time 3 "http://127.0.0.1:$p/readyz" >/dev/null; do sleep 1; done
done

# 4) 确认入口两后端健康
curl --silent "http://127.0.0.1:58087/statusz"
# 期望：{"backends":[{"url":"http://wecom-ha-node-a:8080","healthy":true},{"url":"http://wecom-ha-node-b:8080","healthy":true}]}

# 5) 在企业微信控制台把两个 Bot 回调设为 https://<你的tunnel>/callbacks/wecom

# 6) 故障演练见 testing-and-acceptance.md；或一键：
#    bash scripts/e2e/single-host-multicontainer.sh
```

---

## 8. 验证清单（最低通过集）

- [ ] `docker compose --profile wecom-ha-local ps` 显示 `wecom-ha-bootstrap` 已完成、`node-a`/`node-b`/`entry` 运行。
- [ ] `node-a:58088/readyz`、`node-b:58089/readyz`、`entry:58087/readyz` 均 200。
- [ ] `entry:58087/statusz` 中两后端 `healthy:true`。
- [ ] `node-a:58088/statusz` 的 `instance_id == "wecom-ha-node-a"` 且 `owners.worker` 以 `webui-local-worker-wecom-ha-node-a-` 开头。
- [ ] 强杀 `node-a` 后，`entry/statusz` 中 `wecom-ha-node-a` 变 `healthy:false`、`wecom-ha-node-b` 仍 `true`；`docker inspect node-a` 的 `State.Running==false` 持续。
- [ ] 显式 `start node-a` 后，`node-a/statusz` 的 `process_start_id` 与基线**不同**；`entry/statusz` 中 `node-a` 重新 `healthy:true`。
- [ ] 反向强杀 `node-b` 后，`entry/statusz` 只剩重新加入的 `node-a` 健康；`node-b` `State.Running==false` 持续。
- [ ] 真实/录制的两个 Bot 原会话最终回复唯一且 tenant 正确（由 `TRPC_WECOM_REAL_ACCEPTANCE` 控制 assumed/recorded）。
- [ ] 全程未修改任何企业微信回调 URL（公开地址不随实例切换）。
