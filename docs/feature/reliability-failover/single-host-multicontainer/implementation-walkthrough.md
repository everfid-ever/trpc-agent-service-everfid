# 单主机多容器实例可用性 — 实现走读

> 本文按模块顺序走读生命周期与接管语义；稳定入口、Compose 和演练脚本的**直接代码摘录**见 [CODE-APPENDIX.md](./CODE-APPENDIX.md)，可照抄落地材料见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)。所有标识符与实现代码一致（可选核对：`cmd/trpc-service/wecom_ha_entry_role.go`、`cmd/trpc-service/webui_local_role.go`、`deploy/compose/docker-compose.local.yml`、`scripts/e2e/single-host-multicontainer.sh`）。

## 一、模块 A：入口配置加载（`loadWeComHAEntryConfig`）

入口是独立二进制 `wecom-ha-entry`，其配置加载函数为 `loadWeComHAEntryConfig(getenv)`：

1. `TRPC_LISTEN_ADDRESS`：默认 `:8080`；要求非空前导/尾随空白，否则"entry listen address is invalid"。
2. `TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL`：默认 `1s`；如设置需可被 `time.ParseDuration` 解析且位于 `[100ms, 1m]`，否则"entry probe interval is invalid"。
3. `TRPC_WECOM_HA_ENTRY_BACKENDS`：按逗号切分；每个元素 `url.Parse` 后必须 `scheme=http`、`host≠""`、无 `user`、无 `query`、无 `fragment`；路径尾部 `/` 归一化；**去重**；至少 **2 个**，否则"entry requires at least two backends"。

语义含义：入口强制"至少两个内部健康后端"且地址形态受限，避免误把单点或不合规 URL 当作高可用后端。完整代码见 CODE-APPENDIX 第 1 节。

## 二、模块 B：探测循环与轮转（`probe` / `healthyBackends` / `status`）

`runWeComHAEntryRole` 启动后先做一次 3s 超时初始 `probe`，再启动 goroutine 按 `ProbeInterval` 周期 `probe(parent, 3s)`：

- `probe()`：对每个 backend 请求 `<backend>/readyz`；HTTP 200 → `healthy.Store(true)`，否则 `healthy.Store(false)`。网络错误同样标 false。
- `healthyBackends()`：从原子轮转下标 `next.Add(1)` 开始按序返回 `healthy==true` 的后端。这是 round-robin 起点，保证多健康后端下 callback 负载分布且单点失败时切换到下一个。
- `status()`：输出 `weComHAEntryStatus{Backends: [{URL, Healthy}]}`，即演练所需的 `/statusz` 健康集合。

关键点：探测的是应用 `/readyz`（覆盖 db+redis+malware），**不是 Docker 进程状态**。实例即使还活着，只要依赖不可用也会被摘除。

## 三、模块 C：callback 轮询转发（`serveCallback` / `forward`）

`/callbacks/wecom` 绑定 `serveCallback`：

1. 仅允许 GET/POST，否则 405。
2. `http.MaxBytesReader(writer, body, p.limit)`（`limit = 2<<20` = 2 MiB），超限 413。
3. 遍历 `healthyBackends()`：对每个后端 `forward(request, body, backend)`。
4. `forward` 出错 → 标该后端 unhealthy，尝试下一个。
5. 后端返回 502/503/504 → 标 unhealthy，尝试下一个。
6. 否则 `copyResponse` 并 return（不再重试，避免重复业务调用）。
7. 所有健康后端均失败 → 503 `no ready callback backend`。

`forward()`：路径拼接（保留原 `request.URL.Path`、去掉尾部 `/` 再拼）、查询串 `RawQuery` 保留；克隆 header，删除 `Connection`/`Proxy-Connection`/`Transfer-Encoding`，设置 `X-Forwarded-For`/`X-Forwarded-Proto`，设 `ContentLength` 后转发。

> 入口重试**不替代业务幂等**：只在本次 callback 尚未返回响应前换后端；真正去重仍靠后端 durable Inbox（`claim_inbox`/`prepare_dispatch` 唯一键收敛）。完整代码见 CODE-APPENDIX 第 2–3 节。

## 四、模块 D：入口自身 live/ready/status（`/livez`、`/readyz`、`/statusz`）

```go
mux.HandleFunc("/livez",   ... writer.WriteHeader(200))
mux.HandleFunc("/readyz",  ... if len(healthyBackends())==0 → 503 else 200)
mux.HandleFunc("/statusz", ... json( pool.status() ))
mux.HandleFunc("/callbacks/wecom", pool.serveCallback)
```

服务超时：ReadHeader 5s、Read 10s、Write 10s、Idle 60s、MaxHeaderBytes 16KiB。

当入口进程仍活着但 N1/N2 都不可用时：`/livez` 成功、`/readyz` 失败。这对监控区分"入口故障"与"无可用业务后端"两类场景至关重要。

## 五、模块 E：节点侧实例身份与 owner（webui_local_role）

`loadWebUILocalConfig` 中：

```go
InstanceID:     valueOr(getenv("TRPC_WEBUI_LOCAL_INSTANCE_ID"), "standalone")
...
processStartID, err := newWebUILocalProcessStartID()  // 8 字节 crypto/rand → 16 hex
value.ProcessStartID = processStartID
```

`instanceName(component)` 返回 `webui-local-<component>-<InstanceID>-<ProcessStartID>`。实际 owner 装配（行 ~383–414）包含 7 个组件：

```go
worker         := configValue.instanceName("worker")            // 同时作 ConsumerID
preprocess     := configValue.instanceName("preprocess")
dispatchRelay  := configValue.instanceName("dispatch-relay")
replyRelay     := configValue.instanceName("reply-relay")
wakeupRelay    := configValue.instanceName("wakeup-relay")
wakeup         := configValue.instanceName("wakeup")
delivery       := configValue.instanceName("delivery")
```

- Worker：`LeaseTTL 30s`、`RenewInterval 10s`、`RetryWait 250ms`、`ReclaimInterval 5s`、`ReclaimLimit 100`、`DrainTimeout 30s`，`Shards [0,1,2,3]`。
- Delivery/relay：`ClaimTTL 30s`、`ClaimRenewInterval 10s`、`PollInterval 100ms`；delivery `DefaultRetryDelay 1s`/`MaxRetryDelay 1m`/`MaxAttempts 8`/`MaxReconcileAttempts 8`。

owner 写入 lease、ledger claim、日志和 metrics；consumer ID 写入 Redis consumer group。两者均携带 `process_start_id`，因此重新加入的新进程不会与旧 owner 混淆。

节点 `/statusz` 输出 JSON：

```json
{
  "instance_id": "wecom-ha-node-a",
  "process_start_id": "9f3c1a2b4d5e6f07"
}
```

HTTP 面（行 ~431–452）：

- `/callbacks/wecom`（存在 WeCom endpoint 时）
- `/livez` → 200
- `/statusz` → `runtimeStatus` JSON
- `/test/failover/{p1|p2|p3|p4}/{arm,status,release}`（仅启用 failover test 时）→ 每个操作要求正确 `X-TRPC-Local-Token`，否则 403
- `/readyz` → `db.PingContext` 与 `redis.Ping` 与 `malware.Probe` 任一失败即 503

## 六、模块 F：编排启动顺序与依赖条件

```text
postgres/redis/qdrant/clamav/otel healthy
   → wecom-ha-bootstrap (webui-local-bootstrap, 一次性, service_completed_successfully)
        → wecom-ha-node-a / wecom-ha-node-b (restart:"no", depends_on bootstrap completed + clamav healthy + otel started)
             → wecom-ha-entry (depends_on node-a/node-b service_started)
```

Bootstrap 是单例操作（初始化共享 tenant/config fixture），应用节点可以多副本。`wecom-ha-local` profile 刻意**不带** `TRPC_WEBUI_LOCAL_EXCLUSIVE_RUNTIME`（与单实例 `webui-local` 不同），因为多实例用显式不同身份，不需要排他 advisory lock。

N1/N2 使用同 image digest 和 config version；实例差异只由部署注入的身份（`TRPC_WEBUI_LOCAL_INSTANCE_ID`）、端口和进程启动身份决定。

## 七、模块 G：正常消费（owner/consumer 驱动）

节点启动后并发运行 8 个后台 loop：preprocess、progress publisher、confirmation expiry reconciler、dispatch relay、worker、reply relay、wakeup relay、wakeup dispatcher、delivery、http server。每个 relay/worker 都以 `instanceName(component)` 作为 owner/consumer ID 参与 Redis/PostgreSQL 的协调，使任一实例的占用都可追责、可被 peer reclaim。

## 八、模块 H：非优雅故障（SIGKILL 路径）

```text
docker kill --signal=KILL wecom-ha-node-a
  → N1 进程立即消失：无法续租、不能 ACK、不能 finish delivery
  → 入口 readiness probe 摘除 N1（/readyz 不再 200）
  → N2 在 LeaseTTL(30s)/ReclaimInterval 条件满足后 reclaim pending work/reply
  → N2 取得更高 fence 或过期 claim，继续服务原入口与两个 Bot
```

接管轮必须禁用 restart policy（`restart: "no"`）。自动重启是另一个被测机制，不能用"新建 N1 完成时间"冒充"N2 接管时间"——否则本包核心结论不可证。

## 九、模块 I：重新加入

```text
docker compose --profile wecom-ha-local start wecom-ha-node-a
  → 新进程：newWebUILocalProcessStartID() 生成新 psid
  → readiness checks（db/redis/malware）通过
  → recovery scan only for reclaimable work（绝不重发已 sent）
  → observe ownership/backlog/delivery stability
  → 之后才能对 node-b 做反向故障
```

## 十、模块 J：演练脚本断言逻辑（single-host-multicontainer.sh）

仓库脚本（可选核对：`scripts/e2e/single-host-multicontainer.sh`）的断言函数：

- `wait_http(url, desc)`：`curl --fail` 轮询直到 200 或超时（`TRPC_SINGLE_HOST_TIMEOUT_SECONDS`，默认 120）。
- `wait_entry_backend(node, healthy)`：轮询入口 `/statusz`，`grep -F` 子串 `"url":"http://<node>:8080","healthy":<bool>` 是否出现，直到匹配或超时。这是判断"入口是否把某节点列入/移出健康集合"的核心断言。
- `process_start_id(file)`：`sed -nE 's/.*"process_start_id":"([^"]+)".*/\1/p'`。
- `assert_stopped(container)`：`docker inspect --format '{{.State.Running}}'` 必须等于 `false`，否则"victim restarted unexpectedly"。

流程断言要点（逐行）：

1. `up --detach --build` → 等 node-a/node-b `/readyz`、入口 `/readyz`。
2. `wait_entry_backend wecom-ha-node-a true` / `node-b true`：两后端均健康。
3. `capture baseline` 并取 `node_a_start_before = process_start_id(baseline-node-a.json)`。
4. `docker kill --signal=KILL` node-a → 等 node-b `/readyz`、入口 `/readyz`。
5. `wait_entry_backend node-a false` / `node-b true`：入口只剩 N2。
6. `assert_stopped node-a` 并 `sleep 2` 再断言一次（确认不是瞬时）。
7. `compose start wecom-ha-node-a` → 等 node-a `/readyz`、`wait_entry_backend node-a true`。
8. 取 `node_a_start_after`，断言 `非空 且 != node_a_start_before`（重新加入 = 新 owner）。
9. `sleep ${stability_seconds}`（默认 5）。
10. `docker kill --signal=KILL` node-b → 等 node-a `/readyz`、入口 `/readyz`。
11. `wait_entry_backend node-a true` / `node-b false`：入口只剩重新加入的 N1。
12. `assert_stopped node-b`。

脚本通过 `trap cleanup EXIT` 在结束时 `compose down --volumes --remove-orphans`（除非 `TRPC_SINGLE_HOST_KEEP_ENVIRONMENT=true`），并保留诊断目录（含每阶段 `*-node-a.json` / `*-node-b.json` / `*-entry.json`、`compose-ps.txt`、`compose.log`）。

## 十一、代码审阅重点

- 入口后端选择是否基于 readiness 而非"容器进程存在"（模块 B/D）。
- worker/delivery reclaim 是否因 owner/consumer ID 唯一而可追责（模块 E/G）。
- 强杀后是否禁用自动重启，以免掩盖接管（模块 H）。
- 重新加入是否产生新 `process_start_id`、旧回复不重发（模块 I/E）。
- 若由多个 role 容器组成一个逻辑节点，故障脚本是否覆盖该节点定义的全部职责。

## 十二、模块 K：在途屏障（P1–P4）与 worker 消费回收

当演练需要证明"故障瞬间的在途任务"语义时，`webui-multinode` 节点通过 `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true` 注册四个可按 HTTP arm 的挂点（见相邻 `inflight-task-takeover/design.md`）：

| Point | 位置 | 阻塞时刻 |
|---|---|---|
| P1 | `preprocess/worker.go` `dispatch()` 前 | durable job 已存在、尚未成为 execution |
| P2 | `worker/runner.go` | 模型/工具已跑完、尚未 commit |
| P3 | `relay/reply.go` `ReplyRelay.handle()` | reply event 已构造、尚未 publish |
| P4 | `delivery/service.go` `deliverSegment()` | 已 claim delivery、尚未调 provider |

未 arm 的 point 上 `barrier.Wait` 直接返回；已 arm 的 point 置 `hit=true` 并阻塞至 `Release` 或 `ctx.Done()`。Snapshot 不含 tenant/request。控制端点为 `/test/failover/{p1|p2|p3|p4}/{arm,status,release}`；标准 E2E 命中 node-a 后直接 SIGKILL，进程取消该 wait，存活节点从 durable state 接管，绝不靠 release 修复业务状态。

Worker 消费回收（`worker/consumer.go`）：`Run()` 的 reclaim goroutine 每秒 `Broker.Reclaim(consumerID, limit=100)`，对每条走 `process`；`handle()` 中 `acquire` 循环 `Acquire`，`ErrVersionConflict` 按 `RetryWait(250ms)` 重试；续租失败 `markLeaseLost()` 关闭 `leaseLost` 并取消执行 context；`executeErr != nil` 时**不 ACK**，保留 pending 供存活节点 reclaim；Consume 回调执行失败故意返回 nil 以免 worker 退出，让空闲 delivery 可被 reclaim。

## 十三、模块 L：重加入后的恢复扫描与稳定窗口

N1 被 `compose start` 后，新进程：

1. `newWebUILocalProcessStartID()` 生成新 `process_start_id`，所有 owner 名随之改变。
2. 通过 `db.PingContext` + `redis.Ping` + `malware.Probe` 的 `/readyz`。
3. 启动时从 PostgreSQL 读 `last_fence` 校准 Redis（`EnsureFenceAtLeast`）；只接管过期 `pending/retry_wait/sending`，绝不重发 `sent`。
4. 进入稳定窗口观察：owner 不应高频迁移、不应出现 `stale fence` 写回、不应连接振荡或 backlog 增长。

只有稳定窗口通过，才允许进入反向故障（强杀 N2）。这一步是"重新加入安全"的验收闸门。

## 十四、模块 M：一次完整故障演练的时序痕迹（示例）

```text
T0   compose up --detach --build
T0+  wait_http node-a/readyz, node-b/readyz, entry/readyz  (均 200)
T0+  capture baseline → node_a_start_before = "9f3c1a2b4d5e6f07"
T1   docker kill --signal=KILL <node-a>
T1+  entry probe: node-a /readyz 失败 → backends[node-a].healthy=false
T1+  wait_entry_backend node-a false, node-b true   ✓
T1+  assert_stopped node-a → State.Running=false ✓ ; sleep 2 → 仍 false ✓
     (此时 N2 reclaim node-a 遗留 work/reply，fence 校准到更高值)
T2   compose start wecom-ha-node-a
T2+  wait_http node-a/readyz (200) ; wait_entry_backend node-a true ✓
T2+  node_a_start_after = "1b2c3d4e5f6a7b8c"  ≠ baseline ✓
T2+  sleep ${stability_seconds}  (稳定窗口：无旧 reply、无振荡、无 backlog 增长)
T3   docker kill --signal=KILL <node-b>
T3+  wait_entry_backend node-a true, node-b false ✓
T3+  assert_stopped node-b → false ✓
T4   cleanup: compose down --volumes --remove-orphans (除非 KEEP=true)
```

每一步的"✓"对应 `testing-and-acceptance.md` 第 5 节的判定表 P1–P7。

> 仓库脚本先保存两个节点与入口的 `/statusz`，强杀 N1 并确认入口只保留 N2；随后显式 `start` N1、比较前后 `process_start_id`、等待稳定窗口，再强杀 N2 并确认入口只保留重新加入的 N1。它不把 Docker 重启当作接管，也不把真实 Bot 会话伪造成 HTTP 探测。

---

## 十五、生命周期每一步的 durable truth 与"此刻被杀会怎样"

| # | 阶段 | durable truth | 此刻进程被杀会怎样 |
|---|---|---|---|
| 1 | `compose up` 起共享依赖 | PostgreSQL/Redis 各自的持久化数据 | 依赖重启，应用未启动；无业务影响 |
| 2 | `wecom-ha-bootstrap` 初始化 fixture | `tenant` / `channel_binding` / `channel_public_route` 等配置行 | 幂等：重启后重跑 `Up` 迁移，收敛到同一 fixture |
| 3 | 两节点启动，生成 `process_start_id` | 无（进程内） | 该节点不可服务；入口探测摘除；无数据损失 |
| 4 | 节点 `/readyz` 通过（db/redis/malware 均可用） | 无（探针结果） | 入口不再转发；已在处理的工作保留在 Redis pending |
| 5 | 入口探测两后端 | 入口内存中的 `healthy` 原子布尔 | 入口重启后重新探测；公开地址不变 |
| 6 | callback 转发到健康节点 | 后端 `inbox` 行（四元组唯一键） | 未提交则平台重传；已提交则收敛 |
| 7 | Worker 消费 Work Stream，取 session lease | Redis lease key + fence key | 未 ACK 的 entry 保持 pending，由存活节点 reclaim |
| 8 | `commit_turn` 提交终端结果与待发回复 | `session_commit` + `session_head` + `reply Outbox`（同一事务；`session_event` 生产传 null） | 整体回滚或整体提交；无中间态 |
| 9 | reply relay 发布 Reply Stream | `outbox` reply 行（`published`） | claim 超时后由存活节点重发 |
| 10 | Delivery 领取 Ledger 并发送 | `delivery_ledger`（`sending` + owner/until） | claim 到期后被改写为 `ambiguous/owner_lost`，由存活节点接管 |
| 11 | 置 `sent` | `delivery_ledger.state='sent'` + `provider_message_id` | 只允许 ACK，不允许重发 |
| 12 | **SIGKILL 受害节点** | 上述全部持久化状态 | 未完成工作留在共享存储；存活节点在 TTL/reclaim 后承接 |
| 13 | 受害节点**显式重新加入** | 新的 `process_start_id`；旧 lease 到期消失 | 新进程以新 owner 参与竞争；旧 fence 提交被拒 |
| 14 | 反向强杀另一个节点 | 同 12 | 同一套机制反向成立 |

**判据纪律：** 第 3、4、5、6、12、13、14 步的"被杀"都不会造成数据丢失，因为权威事实在 PostgreSQL/Redis 中。**唯一会永久影响结果的是第 8 步的事务语义**——它必须整体成功或整体回滚，这正是 `commit_turn` 单事务存在的原因。

---

## 十六、隔离与共享：一行结论

> **身份与本地暂态必须隔离（否则接管不可归因）；业务事实与协调状态必须共享（否则存活实例看不到受害实例的工作）。**
> 本包的每一条实现细节——`process_start_id`、独立端口、`restart:"no"`、共享 DSN/Redis 地址、稳定入口——都是这句话的直接推论。
