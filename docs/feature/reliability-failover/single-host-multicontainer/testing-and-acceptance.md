# 单主机多容器实例可用性 — 测试与验收

> 本文给出完整测试方案：参数、逐步命令、脚本行为逐条解释、证据字段清单（含 `/statusz` 三种快照）、通过判定表、反例清单，以及**持续负载下的连接稳定性 / 任务积压 / 重复回复检查方法**。所有端口、变量、断言与实现代码一致（可选核对：`scripts/e2e/single-host-multicontainer.sh`、`scripts/e2e/wecom-ha-failover.sh`、`cmd/trpc-service/wecom_ha_entry_role.go`、`cmd/trpc-service/webui_local_role.go`）。

## 一、参数总表

| 变量 | 默认 | 含义 |
|---|---|---|
| `TRPC_SINGLE_HOST_PROJECT` | `trpc-single-host-${RANDOM}${RANDOM}` | Compose project 名（隔离） |
| `TRPC_LOCAL_WECOM_HA_ENTRY_PORT` | `58087` | 入口宿主端口 |
| `TRPC_LOCAL_WECOM_HA_NODE_A_PORT` | `58088` | N1 宿主端口（仅观测） |
| `TRPC_LOCAL_WECOM_HA_NODE_B_PORT` | `58089` | N2 宿主端口（仅观测） |
| `TRPC_SINGLE_HOST_TIMEOUT_SECONDS` | `120` | 各 `wait_http` / `wait_entry_backend` 超时 |
| `TRPC_SINGLE_HOST_STABILITY_SECONDS` | `5` | 重新加入后稳定窗口观察时长 |
| `TRPC_SINGLE_HOST_KEEP_ENVIRONMENT` | `false` | true 时退出不 `compose down` |
| `TRPC_WECOM_REAL_ACCEPTANCE` | `assumed` | `assumed`（默认）/`recorded` |

前置约束：本机需 `docker` + Compose v2 + `curl`；`deploy/compose/secrets/deepseek-api-key` 与 `deploy/compose/secrets/wecom.env` 必须**非空**，否则脚本直接退出 2。

## 二、逐步命令（基线 → 强杀 → 重加入 → 反向）

```bash
bash scripts/e2e/single-host-multicontainer.sh
```

脚本内部顺序（与 `implementation-walkthrough.md` 模块 J 一致）：

1. `compose up --detach --build`
2. `wait_http node-a/readyz`、`wait_http node-b/readyz`、`wait_http entry/readyz`
3. `wait_entry_backend wecom-ha-node-a true`、`wait_entry_backend wecom-ha-node-b true`
4. `capture baseline` → 取 `node_a_start_before = process_start_id(baseline-node-a.json)`
5. `docker kill --signal=KILL <node-a container>`
6. `wait_http node-b/readyz`、`wait_http entry/readyz`
7. `wait_entry_backend node-a false`、`wait_entry_backend node-b true`
8. `assert_stopped node-a`；`sleep 2`；`assert_stopped node-a`（二次确认）
9. `compose start wecom-ha-node-a` → `wait_http node-a/readyz`、`wait_entry_backend node-a true`
10. `node_a_start_after = process_start_id(node-a-rejoined-node-a.json)`；断言 `非空 且 != node_a_start_before`
11. `sleep ${stability_seconds}`
12. `docker kill --signal=KILL <node-b container>` → `wait_http node-a/readyz`、`wait_http entry/readyz`
13. `wait_entry_backend node-a true`、`wait_entry_backend node-b false`
14. `assert_stopped node-b`
15. 按 `TRPC_WECOM_REAL_ACCEPTANCE` 输出 assumed/recorded

## 三、脚本行为逐条解释

- `capture(label)`：对 `node-a`/`node-b`/entry 三个 `/statusz` 各 `curl` 一次写入 `${diagnostics}/${label}-{node-a,node-b,entry}.json`（`--fail` 失败不写但继续）。这是三种快照的来源。
- `wait_http(url, desc)`：`curl --fail --silent --show-error --max-time 3` 轮询，超时返回 1（脚本因 `set -euo pipefail` 退出）。用于**存活实例** ready 与**入口** ready 的等待。
- `wait_entry_backend(node, healthy)`：轮询入口 `/statusz`，`grep -F` 子串 `"url":"http://<node>:8080","healthy":<bool>` 是否出现。这是判定"入口是否把某节点列入/移出健康集合"的核心断言。注意它只校验入口视图，不校验节点自身 `/readyz`（节点 ready 由 `wait_http` 单独校验）。
- `process_start_id(file)`：`sed -nE 's/.*"process_start_id":"([^"]+)".*/\1/p' | head -n 1`，从 `/statusz` JSON 抽取进程身份。
- `assert_stopped(container)`：`docker inspect --format '{{.State.Running}}'` 必须等于 `false`，否则"victim restarted unexpectedly"。**二次 `sleep 2` 再断言**是为了排除"瞬时又被某机制拉起"。
- `cleanup()`（`trap EXIT`）：先 `capture final`；`compose ps`/`compose logs` 落盘；若 `KEEP=false` 则 `compose down --volumes --remove-orphans`；保留诊断目录路径到 stderr/stdout。

> 重要：`wait_entry_backend node-a false` 与 `assert_stopped node-a` 是**两条独立证据**：前者证明入口不再向 N1 转发，后者证明 N1 容器确实持续停止（未被 `restart: "no"` 之外的机制复活）。两者缺一不可，否则"存活实例接管"不可证。

## 四、证据字段清单（含 `/statusz` 三种快照）

每次 `capture(label)` 产生三份快照：`${label}-node-a.json`、`${label}-node-b.json`、`${label}-entry.json`。

| 快照 | 关键字段 | 用途 |
|---|---|---|
| `baseline-*` | `instance_id`、`process_start_id`、`owners.*` | 记录基线身份，供重加入前后对比 |
| `node-a-down-*` | entry 的 `backends[wecom-ha-node-a].healthy==false` | 证明强杀后入口摘除 N1 |
| `node-a-rejoined-*` | `process_start_id` 与 baseline 不同 | 证明重加入产生新 owner（I9） |
| `node-b-down-*` | entry 的 `backends[wecom-ha-node-b].healthy==false` | 证明反向故障后入口只剩重加入的 N1 |
| `final-*` | 全流程结束态 | 排障与归档 |

入口快照 `backends` 形如：

```json
{"backends":[
  {"url":"http://wecom-ha-node-a:8080","healthy":false},
  {"url":"http://wecom-ha-node-b:8080","healthy":true}
]}
```

节点快照 `owners` 形如（见 REFERENCE-IMPLEMENTATION.md 第 2 节）。

## 五、通过判定表

| # | 判定项 | 通过条件 | 证据 |
|---|---|---|---|
| P1 | 双实例稳定共存 | `node-a`/`node-b` 各自 `/readyz` 200 且 entry 两后端 healthy | baseline 快照 |
| P2 | 强杀 N1 后入口仍可用 | `entry/readyz` 200 且 `wecom-ha-node-a` healthy=false、`wecom-ha-node-b` healthy=true | node-a-down-entry.json |
| P3 | N1 持续停止 | `docker inspect node-a` `State.Running==false`（含 sleep 2 后） | `assert_stopped` |
| P4 | N1 重新加入新身份 | `node_a_start_after` 非空且 `!= node_a_start_before` | node-a-rejoined-node-a.json |
| P5 | 重加入后入口恢复双健康 | `wait_entry_backend node-a true` | 重加入后 entry 快照 |
| P6 | 反向强杀 N2 后入口仍可用 | `entry/readyz` 200，`node-a` healthy=true、`node-b` healthy=false | node-b-down-entry.json |
| P7 | N2 持续停止 | `docker inspect node-b` `State.Running==false` | `assert_stopped` |
| P8 | 公开地址不变 | 全程未改企业微信回调 URL | 人工/配置审查 |
| P9 | 真实最终回复唯一且 tenant 正确 | `TRPC_WECOM_REAL_ACCEPTANCE=recorded` 时的外部证据；否则 assumed | 外部或 assumed |

## 六、反例清单（任一出现即失败/需排查）

- ❌ 强杀后 `docker inspect` 显示 `State.Running==true` 或 sleep 后变 true → 被自动重启，违反 `restart:"no"`，**结论无效**。
- ❌ 入口 `/statusz` 中受害节点仍 `healthy:true` → probe 未摘除，故障未体现。
- ❌ 重加入后 `process_start_id` 与基线相同 → 不是新进程（可能是复用容器/卷误挂载），判定 I9 失败。
- ❌ 重新加入后出现旧 reply 重发、`stale fence` 写回、连接振荡或 backlog 增长 → 稳定窗口未通过。
- ❌ 反向故障后入口只剩 N2 健康 → 说明重加入未真正生效。
- ❌ 把 `wecom-ha-node-a:80888`/`58088` 端口登记为企业微信回调 → 违反"唯一对外端口"约束（应只用 `58087`）。
- ❌ 用 `docker compose down` 模拟单实例故障 → 会连带关闭共享依赖，不属于本能力范围。

## 七、持续负载下的连接稳定性 / 任务积压 / 重复回复检查

> 上述 P1–P9 是生命周期验收；本节能**独立附加**于演练，用于验证"持续负载下"三项指标。可在 `up` 后、强杀前后，用独立客户端向入口 `58087/callbacks/wecom` 注入带唯一标记的消息（或依赖真实 Bot 会话），并周期性采集。

### 7.1 连接稳定性（connection stability）

**观测**：在负载注入期间与故障切换窗口，持续探测入口 `/livez` 与 `/readyz`，以及两节点 `/readyz`。

**判据**：
- 入口 `/livez` 全程 200（进程不崩）。
- 入口 `/readyz` 仅在"两后端均不健康"的极短窗口（≤ 探测周期 + 切换延迟）内可能 503，之后立即恢复 200。
- 切换期间已发出但未获响应的 callback：入口会在本次请求内换健康后端重试，**不丢**；企业应能最终收到响应。
- 不得出现入口进程重启、端口不可达、或两后端同时长期不健康的"连接振荡"。

**可执行命令**：
```bash
# 每 1s 记录入口 ready 状态，持续 5 分钟
for i in $(seq 1 300); do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:58087/readyz || echo 000)
  echo "$(date +%T) entry_ready=$code"
  sleep 1
done > entry-ready.log
grep -c '000\|503' entry-ready.log   # 期望极小或为 0（非切换窗口）
```

### 7.2 任务积压（backlog）

**观测**：采集 PostgreSQL / Redis 中待处理工作的堆积量。关键表/流：
- `inbox`：`state` 非 `terminal` 且长时间不前进（如 `dispatch_pending`/`dispatch_ready` 行数）。
- `execution_record`：`outcome='queued'/'pending'/'blocked'` 行数。
- `outbox`：`state='pending'/'claimed'/'retry_wait'` 行数。
- Redis：Work/Reply/Wakeup Stream 的 pending entries（`XPENDING`）与 consumer 空闲。
- `delivery_ledger`：`state IN ('pending','sending','retry_wait','ambiguous')` 行数。

**判据**：
- 稳定负载下 pending 数应在有界范围内波动，不单调增长。
- 故障切换后，原属受害实例的 pending 应在 `LeaseTTL(30s)` + `ReclaimInterval` 内被存活实例 reclaim 并下降。
- 不得出现"积压无限增长且无人消费"——这是 reclaim 失效的信号。

**可执行查询（示例）**：
```sql
-- 积压计数（PostgreSQL）
SELECT 'inbox_nonterminal' AS metric, count(*) FROM inbox WHERE state <> 'terminal'
UNION ALL SELECT 'execution_active', count(*) FROM execution_record WHERE outcome IN ('queued','pending','blocked')
UNION ALL SELECT 'outbox_active', count(*) FROM outbox WHERE state IN ('pending','claimed','retry_wait')
UNION ALL SELECT 'delivery_active', count(*) FROM delivery_ledger WHERE state IN ('pending','sending','retry_wait','ambiguous');
```
```bash
# Redis pending（以 webui worker group 为例）
docker exec <redis> redis-cli XPENDING trpc-webui-work webui-worker > work-pending.log
```

### 7.3 重复回复（duplicate reply）

**观测**：最终用户收到的回复是否唯一；底层以 `delivery_ledger` 的 `sent` 段为单位判定。

**判据**：
- 对每条 `(tenant, delivery_key, segment_no)`，至多一行 `state='sent'`（幂等约束 + `client_request_id` 去重）。
- `outbox` 中 `kind='reply'` 的 `idempotency_key` 唯一（`outbox_tenant_id_kind_idempotency_key_key` 唯一约束）。
- 故障切换期间，下游按 `client_request_id`（由 `StableDeliveryRequestID(tenant_id, delivery_key, segment_no)` 稳定推导）去重；若下游无法去重且查询超时，ledger 诚实置 `ambiguous` 并进入对账，而非双发。
- 用户侧：同一输入不应收到两条内容相同的最终回复。

**可执行检查**：
```sql
-- 查找同 delivery_key + segment 出现多次 sent 的反常（正常应为 1）
SELECT delivery_key, segment_no, count(*) AS sent_cnt
FROM delivery_ledger WHERE state='sent'
GROUP BY delivery_key, segment_no HAVING count(*) > 1;
-- 期望：0 行

-- 对账中/未知状态占比（应趋近于 0）
SELECT state, count(*) FROM delivery_ledger GROUP BY state;
```

### 7.4 负载演练编排建议

1. 启动后用客户端以固定速率（如 1–5 msg/s，两 Bot 混合）持续向 `58087/callbacks/wecom` 发带唯一 `external_message_id` 的消息。
2. 同时运行 7.1/7.2/7.3 的采集循环（后台）。
3. 在负载进行中执行 N1 强杀 → 重加入 → 反向强杀，全程不停止注入。
4. 结束后比对：每一条输入是否恰有一条 `sent` 回复（按 `external_message_id` → `inbox` → `session_commit` → `delivery_ledger` 关联）；是否有 `sent_cnt>1`；积压曲线是否在故障后回落。
5. 采样间隔 `DELTA_POLL` 必须随结论记录，不得宣称比采样更精确的 RTO（见本包 `release-and-operations.md` 的观测口径）。

> 注意：模型调用次数（`P2` 可 >1）与任务尝试次数（`park_attempt`/`attempt` 可增长）**不**等于最终回复次数。验收最终回复唯一性只看 `delivery_ledger` 的 `sent` 段，不限制上游重试。

## 八、负载注入示例（让持续负载可复现）

为验证第七节的连接稳定性/积压/重复回复，可用一个最小客户端持续向入口 `58087/callbacks/wecom` 发带唯一 `external_message_id` 的消息（真实 Bot 凭据由 `wecom.env` 提供，路由 key 走 `local-wecom`）。示例骨架（bash + curl，仅示意结构，凭据/签名按真实 Bot 协议补充）：

```bash
# 每 200ms 发一条唯一标记消息，持续 10 分钟，记录每条的 external_message_id
END=$((SECONDS+600))
seq=0
while (( SECONDS < END )); do
  seq=$((seq+1))
  mid="loadtest-$(date +%s)-${seq}"
  # 真实场景需按企业微信回调签名规范构造 body；此处仅表达"唯一标记"语义
  curl -s -o /dev/null -w "%{http_code} $mid\n" \
    --max-time 5 -X POST "http://127.0.0.1:58087/callbacks/wecom" \
    -d "external_message_id=$mid&text=ping-$seq" >> inject.log
  sleep 0.2
done
```

采集端（后台并行）：

```bash
# 入口 ready 轨迹
while true; do
  echo "$(date +%T) $(curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://127.0.0.1:58087/readyz)" >> entry-ready.log
  sleep 1
done &

# 积压计数（见 7.2 的 SQL），每 5s 一次
while true; do
  psql "$DSN" -t -A -c "SELECT count(*) FROM inbox WHERE state<>'terminal';" >> backlog.log
  sleep 5
done &
```

演练期间执行 `docker kill --signal=KILL` 并 `compose start`，全程注入不中断。结束后比对：`inject.log` 每 `mid` 应恰有一条最终 `sent` 回复（经 `external_message_id → inbox → session_commit → delivery_ledger` 关联），且 `delivery_ledger` 无 `sent_cnt>1`。

## 九、录制真实验收（TRPC_WECOM_REAL_ACCEPTANCE=recorded）

默认 `assumed` 表示"真实 Bot 原会话与最终投递验收作为前置已通过"，演练只证明实例生命周期与入口接管。若需**显式录制**，设：

```bash
TRPC_WECOM_REAL_ACCEPTANCE=recorded \
TRPC_SINGLE_HOST_KEEP_ENVIRONMENT=true \
bash scripts/e2e/single-host-multicontainer.sh
```

录制内容（运维侧，不在脚本内自动完成）：

1. 在强杀 N1 前，通过主/次 Bot 在原会话各发一条带唯一标记消息，记录消息 ID 与期望回复特征。
2. N1 强杀后，确认两 Bot 仍收到唯一且正确的回复（tenant 正确、历史代号连贯、无重复）。
3. N1 重加入并反向强杀 N2 后，重复步骤 2。
4. 将对话截图/导出与 `delivery_ledger` 的 `sent` 行关联归档，作为 `recorded` 证据。

注意：录制的是"业务最终结果"，脚本的 HTTP 断言（P1–P7）仍独立成立；两者互补，不互相替代。

## 十、指标采集与判读示例

任意现有观测系统可采集以下语义指标：

- 入口健康后端数：`count(entry_backends_healthy == 1)`。
- 节点 `/readyz` 成功率：`probe_success{job="node-a"}`。
- 积压：`inbox_nonterminal`、`outbox_active`、`delivery_active`（由 7.2 SQL 暴露为 gauge）。
- 重复回复：由 7.3 SQL 暴露 `delivery_segment_duplicate` gauge（正常恒为 0）。

判读示例（积压曲线）：

```text
t=0s     backlog=3    (正常波动)
t=60s    backlog=3    (N1 强杀)
t=75s    backlog=1    (N2 reclaim 后下降 ← 健康信号)
t=300s   backlog=2    (稳定负载常态)
t=360s   backlog=2    (N2 强杀，N1 重加入后接管)
t=375s   backlog=0    (回落)
```

若 `backlog` 在故障后持续单调上升不回落，说明 reclaim 失效或容量不足，属 `release-and-operations.md` 6.5 的处置范围。

## 十一、完整演练痕迹示例（端到端）

```text
$ bash scripts/e2e/single-host-multicontainer.sh
[wait] node A readiness                ✓ (200)
[wait] node B readiness                ✓ (200)
[wait] stable entry readiness          ✓ (200)
[entry] node-a healthy=true, node-b healthy=true
[baseline] node-a process_start_id=9f3c1a2b4d5e6f07
[SIGKILL] wecom-ha-node-a
[entry] node-a healthy=false, node-b healthy=true   ✓ P2
[assert] node-a State.Running=false (x2)            ✓ P3
[start] wecom-ha-node-a
[entry] node-a healthy=true (rejoined)              ✓ P5
[rejoin] process_start_id=1b2c3d4e5f6a7b8c != baseline  ✓ P4
[stability] sleep 5s
[SIGKILL] wecom-ha-node-b
[entry] node-a healthy=true, node-b healthy=false   ✓ P6
[assert] node-b State.Running=false                 ✓ P7
real WeCom two-Bot conversation acceptance: assumed
single-host multi-container drill completed; diagnostics retained at /tmp/trpc-single-host.XXXX
```

退出码 0 表示 P1–P7 全部通过；非 0 时诊断目录保留，按 `release-and-operations.md` 排查。

## 十二、与更早在途接管 smoke 的关系

`scripts/e2e/wecom-ha-failover.sh`（可选核对：`scripts/e2e/wecom-ha-failover.sh`）是更早的、聚焦"入口在 SIGKILL 后仍可用、容器不被自动重启"的 smoke。它**不验证**重加入与反向故障。本包主演练 `single-host-multicontainer.sh` 是其超集：在 smoke 基础上增加了 `process_start_id` 前后对比（重加入新身份）与反向强杀。两者都依赖 `TRPC_WECOM_REAL_ACCEPTANCE=assumed` 作为真实会话的外部前置。
