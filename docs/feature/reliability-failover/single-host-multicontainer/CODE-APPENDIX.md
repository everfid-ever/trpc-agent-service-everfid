# 单主机多容器实例可用性 — 实现代码附录

> 本文逐段摘录稳定 callback 入口、双实例编排和生命周期演练的关键代码，并解释语义。重点不是"多启动一个容器"，而是公开入口、实例身份、健康判定和故障动作都具有可验证语义。所有摘录来自仓库真实文件（可选核对：`cmd/trpc-service/wecom_ha_entry_role.go`、`cmd/trpc-service/webui_local_role.go`、`deploy/compose/docker-compose.local.yml`、`scripts/e2e/single-host-multicontainer.sh`）。完整可照抄材料见 `REFERENCE-IMPLEMENTATION.md`。

## 1. 入口配置校验（至少两个合规后端）

```go
func loadWeComHAEntryConfig(getenv func(string) string) (weComHAEntryConfig, error) {
	value := weComHAEntryConfig{
		ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		ProbeInterval: time.Second,
		RequestLimit:  2 << 20, // 2 MiB
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
```

**语义**：入口拒绝重复或异常 URL，并要求至少两个内部后端。探测间隔有上下限（`[100ms, 1m]`），防止配置过快拖垮依赖，或过慢造成不可接受的故障窗口。`RequestLimit = 2<<20`（2 MiB）是单 callback 请求体上限。

## 2. readiness 才是是否接收 callback 的判据（probe）

```go
func (p *weComHAEntryPool) probe(ctx context.Context) {
	for _, backend := range p.backends {
		probeURL := *backend.URL
		probeURL.Path = strings.TrimSuffix(probeURL.Path, "/") + "/readyz"
		probeURL.RawQuery = ""
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, probeURL.String(), nil)
		if err != nil { backend.healthy.Store(false); continue }
		response, err := p.client.Do(request)
		if err != nil { backend.healthy.Store(false); continue }
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		backend.healthy.Store(response.StatusCode == http.StatusOK)
	}
}
```

**语义**：探测应用的 `/readyz`，而非 Docker 进程状态。实例即使还活着，只要无法访问数据库、队列或必需依赖，也会被入口摘除。健康状态以原子布尔值保存，转发和状态接口可并发读取。

## 3. healthyBackends 轮转（round-robin 起点）

```go
func (p *weComHAEntryPool) healthyBackends() []*weComHAEntryBackend {
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
```

**语义**：每次调用从原子下标 `next` 自增后开始按序收集健康后端。这保证多健康后端下 callback 负载分布，且单点失败时自然切换到下一个。

## 4. callback 在尚未返回时尝试另一健康后端（serveCallback）

```go
func (p *weComHAEntryPool) serveCallback(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.Body == nil { request.Body = http.NoBody }
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, p.limit))
	if err != nil {
		http.Error(writer, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	for _, backend := range p.healthyBackends() {
		response, forwardErr := p.forward(request, body, backend)
		if forwardErr != nil { backend.healthy.Store(false); continue }
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
```

**语义**：入口只在尚未返回企业微信响应时切换后端；一旦后端返回业务响应，不会自行进行第二次业务调用。前端切换不能替代后端 Inbox 幂等：callback 超时、入口切换或平台重传最终仍由后端的 provider message identity 归并。

## 5. forward：路径/查询串/header 处理

```go
func (p *weComHAEntryPool) forward(request *http.Request, body []byte, backend *weComHAEntryBackend) (*http.Response, error) {
	target := *backend.URL
	target.Path = strings.TrimSuffix(target.Path, "/") + request.URL.Path
	target.RawQuery = request.URL.RawQuery
	forwarded, err := http.NewRequestWithContext(request.Context(), request.Method, target.String(), bytes.NewReader(body))
	if err != nil { return nil, err }
	forwarded.Header = request.Header.Clone()
	forwarded.Header.Del("Connection")
	forwarded.Header.Del("Proxy-Connection")
	forwarded.Header.Del("Transfer-Encoding")
	forwarded.Header.Set("X-Forwarded-For", request.RemoteAddr)
	forwarded.Header.Set("X-Forwarded-Proto", "http")
	forwarded.ContentLength = int64(len(body))
	return p.client.Do(forwarded)
}
```

**语义**：路径拼接保留原 `request.URL.Path`（因此 `/callbacks/wecom` 原样转发到后端同路径），查询串完整保留；删除 hop-by-hop header，注入代理头；`ContentLength` 显式设置避免 chunked 误判。

## 6. 入口自身的 live、ready 与状态接口

```go
mux.HandleFunc("/livez", func(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
})
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
server := &http.Server{Addr: config.ListenAddress, Handler: mux,
	ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
	WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
```

**语义**：当入口进程仍活着但 N1/N2 都不可用时，`/livez` 成功、`/readyz` 失败。这样监控能区分入口故障和无可用业务后端两种场景；`/statusz` 则给出演练时需要的后端健康集合（`{"backends":[{"url":...,"healthy":bool}]}`）。

## 7. 节点侧实例身份与 process_start_id

```go
func (value webUILocalConfig) instanceName(component string) string {
	return "webui-local-" + component + "-" + value.InstanceID + "-" + value.ProcessStartID
}

type webUILocalRuntimeStatus struct {
	InstanceID      string            `json:"instance_id"`
	ProcessStartID  string            `json:"process_start_id"`
	Owners          map[string]string `json:"owners"`
	InflightBarrier inflight.Status   `json:"inflight_test_barrier"`
}

func (value webUILocalConfig) runtimeStatus(barrier ...*inflight.Controller) webUILocalRuntimeStatus {
	components := []string{"worker", "preprocess", "dispatch-relay", "reply-relay", "wakeup-relay", "wakeup", "delivery"}
	owners := make(map[string]string, len(components))
	for _, component := range components {
		owners[component] = value.instanceName(component)
	}
	status := inflight.Status{}
	if len(barrier) == 1 && barrier[0] != nil { status = barrier[0].Status() }
	return webUILocalRuntimeStatus{InstanceID: value.InstanceID, ProcessStartID: value.ProcessStartID, Owners: owners, InflightBarrier: status}
}

func newWebUILocalProcessStartID() (string, error) {
	var value [8]byte
	if _, err := rand.Read(value[:]); err != nil { return "", err }
	return hex.EncodeToString(value[:]), nil // 8 字节 → 16 字符 hex
}
```

**语义**：owner/consumer 名 = `webui-local-<component>-<InstanceID>-<ProcessStartID>`。`ProcessStartID` 每次启动随机 8 字节 hex（16 字符），不可配置。重新加入时该值改变，使所有 owner 名随之改变——旧 lease 在 TTL 后过期，旧 consumer 的 pending 被新进程或 peer reclaim。这是"重新加入 ≠ 旧 owner 复活"的代码基础。

## 8. 节点侧 statusz / readyz / livez / barrier-release

```go
mux.HandleFunc("/livez", func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) })
mux.HandleFunc("/statusz", func(writer http.ResponseWriter, _ *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(writer).Encode(configValue.runtimeStatus(inflightBarrier))
})
if inflightBarrier != nil {
	mux.HandleFunc("/test/failover/barrier/release", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.Header.Get("X-TRPC-Local-Token") != configValue.Token {
			http.NotFound(writer, request)
			return
		}
		inflightBarrier.Release()
		writer.WriteHeader(http.StatusNoContent)
	})
}
mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
	if db.PingContext(request.Context()) != nil || redis.Ping(request.Context()).Err() != nil || malware.Probe(request.Context()) != nil {
		http.Error(writer, "not ready", http.StatusServiceUnavailable)
		return
	}
	writer.WriteHeader(http.StatusOK)
})
```

**语义**：`/statusz` 输出实例身份、7 个组件的 owner 与 barrier 状态；`/readyz` 覆盖 db+redis+malware 三个依赖，任一失败即 503；`/test/failover/barrier/release` 仅在 barrier 非 nil 时注册，且须 `POST` + `X-TRPC-Local-Token` 匹配，否则 404——释放 barrier 只是恢复演练所代表的下游依赖响应，不是业务重试或租约变更。

## 9. 双实例编排：相同业务能力，不同实例身份（Compose 片段）

```yaml
wecom-ha-node-a:
  command: ["wecom-local"]
  restart: "no"
  environment: &wecom-ha-runtime-environment
    TRPC_POSTGRES_DSN: postgres://postgres:postgres@postgres:5432/trpc_agent_service_test?sslmode=disable
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
    # barrier 透传（空=生产安全默认）
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

wecom-ha-node-b:
  command: ["wecom-local"]
  restart: "no"
  environment:
    <<: *wecom-ha-runtime-environment
    TRPC_WEBUI_LOCAL_INSTANCE_ID: wecom-ha-node-b
  # ... 其余同 node-a
  ports: ["${TRPC_LOCAL_WECOM_HA_NODE_B_PORT:-58089}:8080"]

wecom-ha-entry:
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
```

**语义**：N1/N2 共享版本、tenant 配置、数据库和协调服务，但部署身份不同。`restart: "no"` 是接管验收的关键：强杀 N1 后，运行时不得暗中重建 N1 来掩盖 N2 是否真的接管。自动重启是另一个独立场景，不应混入本测试。完整服务定义见 `REFERENCE-IMPLEMENTATION.md` 第 3 节。

## 10. 生命周期脚本检查的是故障语义，不只是命令成功

```bash
# 变量（来自 single-host-multicontainer.sh）
project="${TRPC_SINGLE_HOST_PROJECT:-trpc-single-host-${RANDOM}${RANDOM}}"
entry_port="${TRPC_LOCAL_WECOM_HA_ENTRY_PORT:-58087}"
node_a_port="${TRPC_LOCAL_WECOM_HA_NODE_A_PORT:-58088}"
node_b_port="${TRPC_LOCAL_WECOM_HA_NODE_B_PORT:-58089}"
timeout_seconds="${TRPC_SINGLE_HOST_TIMEOUT_SECONDS:-120}"
stability_seconds="${TRPC_SINGLE_HOST_STABILITY_SECONDS:-5}"

compose() { docker compose --project-name "${project}" -f "${compose_file}" --profile wecom-ha-local "$@"; }

wait_entry_backend() {
  local node=$1 healthy=$2
  local expected="\"url\":\"http://${node}:8080\",\"healthy\":${healthy}"
  while true; do
    if curl --fail --silent --show-error "http://127.0.0.1:${entry_port}/statusz" >"${diagnostics}/entry-probe.json" 2>/dev/null &&
       grep -Fq "${expected}" "${diagnostics}/entry-probe.json"; then return 0; fi
    (( SECONDS >= deadline )) && return 1
    sleep 1
  done
}

process_start_id() { sed -nE 's/.*"process_start_id":"([^"]+)".*/\1/p' "$1" | head -n 1; }

assert_stopped() {
  [[ "$(docker inspect --format '{{.State.Running}}' "$1")" == "false" ]] || { echo "victim restarted unexpectedly: $1" >&2; return 1; }
}

# 核心流程
compose up --detach --build
wait_http "http://127.0.0.1:${node_a_port}/readyz" "node A readiness"
wait_http "http://127.0.0.1:${node_b_port}/readyz" "node B readiness"
wait_http "http://127.0.0.1:${entry_port}/readyz" "stable entry readiness"
wait_entry_backend "wecom-ha-node-a" true
wait_entry_backend "wecom-ha-node-b" true
node_a_start_before="$(process_start_id "${diagnostics}/baseline-node-a.json")"

node_a_container="$(compose ps -q wecom-ha-node-a)"
docker kill --signal=KILL "${node_a_container}"
wait_entry_backend "wecom-ha-node-a" false
wait_entry_backend "wecom-ha-node-b" true
assert_stopped "${node_a_container}"; sleep 2; assert_stopped "${node_a_container}"

compose start wecom-ha-node-a
wait_entry_backend "wecom-ha-node-a" true
node_a_start_after="$(process_start_id "${diagnostics}/node-a-rejoined-node-a.json")"
[[ -n "${node_a_start_after}" && "${node_a_start_after}" != "${node_a_start_before}" ]] || { echo "no new process identity"; exit 1; }
sleep "${stability_seconds}"

node_b_container="$(compose ps -q wecom-ha-node-b)"
docker kill --signal=KILL "${node_b_container}"
wait_entry_backend "wecom-ha-node-a" true
wait_entry_backend "wecom-ha-node-b" false
assert_stopped "${node_b_container}"
```

**语义**：演练依次验证——N1 被杀后确实保持停止、入口只保留 N2；N1 显式重新加入后进程身份变化；N2 被反向强杀后入口只保留 N1。`wait_entry_backend` 通过 grep `/statusz` 中的 `"url":"http://<node>:8080","healthy":<bool>` 子串判定入口健康集合；`process_start_id` 比较判定重新加入产生新 owner；`assert_stopped` 二次确认不是瞬时。`restart: "no"` 保证 `docker kill` 后容器不会自动复活，使"存活实例接管"可被严格证明。HTTP 检查不能代替真实 Bot 原会话和最终回复验证，但能证明实例生命周期与入口接管逻辑实际发生。

## 11. 节点侧 owner 装配代码（摘录自 webui_local_role.go）

```go
// worker：owner 同时作为 ConsumerID；Shards 固定 [0,1,2,3]
workerConsumer := worker.Consumer{
	WorkerID: configValue.instanceName("worker"),
	Shards:   []broker.Shard{0, 1, 2, 3},
	Broker:   streamBroker, Leases: leases, Sessions: sessionStore, Parker: tasks, Statuses: tasks,
	Executor: executor,
	LeaseTTL: 30 * time.Second, RenewInterval: 10 * time.Second, RetryWait: 250 * time.Millisecond,
	ReclaimInterval: 5 * time.Second, ReclaimLimit: 100, DrainTimeout: 30 * time.Second,
}

// relay / delivery：ClaimTTL 30s, ClaimRenewInterval 10s, PollInterval 100ms
dispatchRelay := relay.DispatchRelay{Outbox: inbox, Tasks: tasks, Broker: streamBroker,
	Owner: configValue.instanceName("dispatch-relay"), ShardCount: 4,
	ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond}
replyRelay := relay.ReplyRelay{Outbox: inbox, Results: payloads, Routes: inbox, Replies: publisher,
	Owner: configValue.instanceName("reply-relay"),
	ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond}
wakeupRelay := relay.WakeupRelay{Outbox: inbox, Wakeups: publisher,
	Owner: configValue.instanceName("wakeup-relay"),
	ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond}
wakeupDispatcher := relay.WakeupDispatcher{ConsumerID: configValue.instanceName("wakeup"),
	Wakeups: wakeupQueue, Store: tasks, Dispatch: streamBroker, ShardCount: 4,
	ReclaimInterval: 5 * time.Second, ReclaimLimit: 100}
deliveryService := channeldelivery.Service{Results: payloads, Ledger: inbox, Adapters: deliveryCatalog,
	Owner: configValue.instanceName("delivery"),
	ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second,
	DefaultRetryDelay: time.Second, MaxRetryDelay: time.Minute, MaxAttempts: 8, MaxReconcileAttempts: 8}
```

**语义**：7 个组件的 owner/consumer 名均由 `instanceName(component)` 生成，内含 `InstanceID` 与 `process_start_id`，因此重新加入时全部改变，旧占用由存活实例按 lease/claim 过期规则回收。

## 12. 入口与节点 `/statusz` 示例快照

入口 `GET /statusz`（基线，两后端健康）：

```json
{"backends":[
  {"url":"http://wecom-ha-node-a:8080","healthy":true},
  {"url":"http://wecom-ha-node-b:8080","healthy":true}
]}
```

入口 `GET /statusz`（N1 强杀后）：

```json
{"backends":[
  {"url":"http://wecom-ha-node-a:8080","healthy":false},
  {"url":"http://wecom-ha-node-b:8080","healthy":true}
]}
```

节点 `GET /statusz`（N1 重新加入前后对比）：

```json
// 基线
{"instance_id":"wecom-ha-node-a","process_start_id":"9f3c1a2b4d5e6f07",
 "owners":{"worker":"webui-local-worker-wecom-ha-node-a-9f3c1a2b4d5e6f07", ...},
 "inflight_test_barrier":{"enabled":false}}
// 重新加入后（process_start_id 已变）
{"instance_id":"wecom-ha-node-a","process_start_id":"1b2c3d4e5f6a7b8c",
 "owners":{"worker":"webui-local-worker-wecom-ha-node-a-1b2c3d4e5f6a7b8c", ...},
 "inflight_test_barrier":{"enabled":false}}
```

`process_start_id` 前后对比的判定（脚本逻辑等价）：

```bash
before="9f3c1a2b4d5e6f07"; after="1b2c3d4e5f6a7b8c"
[[ -n "$after" && "$after" != "$before" ]] && echo "rejoin produced new owner" || echo "FAIL"
```

## 13. 代码与设计结论的对应关系

| 设计结论 | 本文代码证据 |
|---|---|
| 公开地址不随 N1/N2 切换 | 第 1–6 节独立入口 |
| 只有可处理业务的实例接收回调 | 第 2 节 readiness 探测 |
| 前端转发失败可换健康后端 | 第 4 节 callback 轮询 |
| 强杀不会被自动重启掩盖 | 第 9 节 `restart: "no"` |
| 重新加入不是旧 owner 复活 | 第 7、10 节 process identity 比较 |
| 同一会话单有效提交者 | Redis lease + `commit_turn` fence 拒绝（见本包 `FULL-GUIDE.md` §14 与 `REFERENCE-IMPLEMENTATION.md` 的不变量表） |
