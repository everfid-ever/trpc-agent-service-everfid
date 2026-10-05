# 故障恢复后继续对话 — 测试与验收

> 本文给出完整测试方案：参数、逐步命令、演练脚本全文与逐条解释、证据字段清单、通过判定表、稳定性/负载检查方法、反例清单、真实 WeCom 外部验收步骤。
> 脚本事实与默认值均与参考实现逐字一致；如与原实现交叉核对，可见 `scripts/e2e/wecom-ha-failover.sh`。

---

## 一、参数与证据固定

执行前**必须固定**（写进本轮运行记录，不允许事后补）：

| 项 | 示例 | 为什么必须固定 |
|---|---|---|
| 镜像 digest | `sha256:...` | 否则无法证明"同一份代码"通过 |
| commit SHA | `a1b2c3d` | 二进制与配置的对应关系 |
| `config_version` | `7` | binding/route/policy 的版本，直接影响路由 |
| `W_BASELINE` | `60s` | 双节点无故障基线观察窗 |
| `T_CONNECT` / `T_READY` / `T_REPLY` | `10s` / `30s` / `30s` | 各阶段时限判据 |
| `W_REPEAT` | `120s` | 重复观察窗（迟到重复回复） |
| `DELTA_POLL` | `1s` | 采样间隔，必须随结论记录 |
| `RUN_ID` / `TOKEN_A` / `TOKEN_B` | 每轮随机 | 隔离本轮证据与代号 |

证据目录独立于代码仓库，**不写回设计文档**。

### 1.1 脚本参数

| 变量 | 默认 | 作用 |
|---|---|---|
| `TRPC_WECOM_HA_PROJECT` | `trpc-wecom-ha-$RANDOM$RANDOM` | 隔离的 Compose project 名 |
| `TRPC_LOCAL_WECOM_HA_ENTRY_PORT` | `58087` | 稳定入口端口 |
| `TRPC_LOCAL_WECOM_HA_NODE_A_PORT` | `58088` | node-a 观测端口 |
| `TRPC_LOCAL_WECOM_HA_NODE_B_PORT` | `58089` | node-b 观测端口 |
| `TRPC_WECOM_HA_TIMEOUT_SECONDS` | `120` | 各项就绪等待上限 |
| `TRPC_WECOM_REAL_ACCEPTANCE` | `assumed` | 真实双 Bot 对话验收状态（`assumed` / `recorded`，其他值报错退出 2） |
| `TRPC_WECOM_HA_KEEP_ENVIRONMENT` | `false` | `true` 时结束后保留容器/卷，便于手工核对 DB 行 |

### 1.2 前置条件

| 前置 | 检查 | 不满足的后果 |
|---|---|---|
| `docker` | `command -v docker` | 退出码 2 |
| Docker Compose v2 | `docker compose version` | 退出码 2 |
| `curl` | `command -v curl` | 退出码 2 |
| `deploy/compose/secrets/deepseek-api-key` 非空 | `[[ -s ... ]]` | 退出码 2 |
| `deploy/compose/secrets/wecom.env` 非空，含 `WECOM_*` 与 `WECOM_SECONDARY_*` 两组凭据 | `[[ -s ... ]]` + 人工确认 `(Corp ID, Agent ID)` 不同 | 退出码 2；或启动时报 `secondary WeCom local configuration is incomplete/incompatible` |

---

## 二、逐步命令

```bash
# 1. 进入仓库根
cd /Users/everfid/code/trpc-agent-service

# 2. 确认本地密钥文件存在且非空（脚本会校验）
#    deploy/compose/secrets/deepseek-api-key
#    deploy/compose/secrets/wecom.env   （含 WECOM_* 与 WECOM_SECONDARY_* 两组凭据）

# 3. 静态校验编排（可选，快速发现配置错误）
docker compose -f deploy/compose/docker-compose.local.yml --profile wecom-ha-local config -q

# 4. 运行强杀演练（自动创建独立 Compose project，结束后清理容器与卷）
bash scripts/e2e/wecom-ha-failover.sh

# 5. 可选：保留环境以便手工核对数据库行
TRPC_WECOM_HA_KEEP_ENVIRONMENT=true bash scripts/e2e/wecom-ha-failover.sh
```

手工逐阶段操作（当不使用脚本时，用于取证）：

```bash
# 启动
docker compose -f deploy/compose/docker-compose.local.yml --profile wecom-ha-local up -d --build

# 基线
curl -s http://127.0.0.1:58088/readyz && echo node-a ready
curl -s http://127.0.0.1:58089/readyz && echo node-b ready
curl -s http://127.0.0.1:58087/livez  && echo entry live
curl -s http://127.0.0.1:58088/statusz | tee evidence/baseline-node-a.json
curl -s http://127.0.0.1:58089/statusz | tee evidence/baseline-node-b.json
curl -s http://127.0.0.1:58087/statusz | tee evidence/baseline-entry.json

# 故障：非优雅终止（t0）
docker kill --signal=KILL "$(docker compose -f deploy/compose/docker-compose.local.yml --profile wecom-ha-local ps -q wecom-ha-node-a)"

# 接管观测
curl -s http://127.0.0.1:58089/readyz && echo node-b still ready
curl -s http://127.0.0.1:58087/livez  && echo entry still live
curl -s http://127.0.0.1:58087/statusz | tee evidence/entry-after-kill.json
curl -s http://127.0.0.1:58089/statusz | tee evidence/node-b-after-kill.json

# 确认受害节点持续停止（间隔数秒复查两次）
docker inspect --format '{{.State.Running}}' "$(docker compose -f deploy/compose/docker-compose.local.yml --profile wecom-ha-local ps -q wecom-ha-node-a)"
sleep 3
docker inspect --format '{{.State.Running}}' "$(docker compose -f deploy/compose/docker-compose.local.yml --profile wecom-ha-local ps -q wecom-ha-node-a)"
```

> 注意：受害容器在强杀后仍会被 `compose ps -q` 找到（容器记录存在但未运行），因此 `docker inspect` 的 `State.Running` 才是判据，而不是"能否查到容器"。

---

## 三、脚本行为逐条解释

### 3.1 `scripts/e2e/wecom-ha-failover.sh`（118 行）

| 行区间 | 行为 | 为什么这样设计 |
|---|---|---|
| 1–7 | shebang + `set -euo pipefail` + 头部注释 | 任一断言失败立即终止；注释声明"真实对话记为 assumed" |
| 8–19 | 解析变量 / 默认端口 58087/58088/58089 / 默认超时 120s / 默认 `assumed` / `mktemp -d` 诊断目录 | 每轮独立 project 与证据目录，互不污染 |
| 21–27 | 校验 docker / compose v2 / curl / 两个 secret 文件非空 | 前置缺失直接退出码 2，不产生半成品结论 |
| 29–31 | `compose()` 封装 `docker compose --project-name "${project}" -f "${compose_file}" --profile wecom-ha-local` | 隔离 project，避免影响其他运行中的环境 |
| 33–36 | `capture_diagnostics()`：`compose ps` + `compose logs --no-color` 落盘 | 失败时保留现场，便于复盘 |
| 38–51 | `cleanup()`：`trap EXIT`；非 keep 时 `compose down --volumes --remove-orphans`；按退出码打印结论 | 默认不残留容器与卷；诊断目录始终保留 |
| 53–64 | `wait_http(url, desc)`：循环 `curl --fail --silent --show-error --max-time 3` 直到成功或超时 | 统一就绪探测；超时返回 1 触发 `set -e` |
| 66–80 | `capture_status(port, expected_instance, label)`：`curl /statusz` 后 `grep -Fq '"instance_id":"<node>"'` 与 `grep -Fq '"worker":"webui-local-worker-<node>-'` | 同时校验**实例身份**与**worker owner 前缀**，防止同名容器换进程 |
| 82 | `compose up --detach --build` | 拉起 bootstrap + node-a + node-b + entry |
| 83–87 | 等 node-a/node-b `/readyz`、入口 `/livez`；`capture_status` 两节点 | 基线证据（V1/V2） |
| 89–91 | `victim_container="$(compose ps -q wecom-ha-node-a)"`；`docker kill --signal=KILL` | **非优雅终止**；记录 `t0` |
| 93–95 | 等入口 `/livez`、node-b `/readyz`；`capture_status` node-b | 证明入口与存活节点仍可用（V4） |
| 97–105 | `docker inspect --format '{{.State.Running}}'` 断言 `false`；`sleep 3` 后再断言 `false` | **两次确认**受害节点未自动重启（V8） |
| 107–118 | 按 `TRPC_WECOM_REAL_ACCEPTANCE` 输出 `assumed` / `recorded` / 报错退出 2 | 真实对话是外部前置，明确区分"自动化通过"与"真实通过" |

**脚本不做的事（同样重要）：**

- 不伪造企业微信用户消息；
- 不修改租约、不重新入队、不改 delivery 记录、不重启受害节点；
- 不宣称给出了精确 RTO（`DELTA_POLL` 未定义时不给时限结论）。

---

## 四、证据字段清单

每条证据记录需包含（脱敏后）：

```text
run_id, timestamp, stage(baseline|fault|takeover|verify|repeat),
node(instance_id), process_start_id, owner(worker/preprocess/dispatch-relay/reply-relay/wakeup-relay/wakeup/delivery),
tenant_id, binding_id, session_id, request_id, input_seq,
stream_entry_id, lease_key, fence_before, fence_after,
delivery_key, segment_no, segment_count, client_request_id,
delivery_state, provider_message_id, attempt, reconcile_attempt, last_error_class,
client_observed_reply(tenant 是否正确、代号是否正确、可见最终回复条数),
delivery_evidence(是否只出现一条 'sent' 记录)
```

导出命令（保留环境后执行）：

```sql
-- 本轮两个租户的投递台账
SELECT tenant_id, delivery_key, segment_no, segment_count, state, attempt, reconcile_attempt,
       client_request_id, provider_message_id, claim_owner, claim_until, updated_at
FROM delivery_ledger
WHERE tenant_id IN ('<TENANT_A>', '<TENANT_B>')
ORDER BY tenant_id, delivery_key, segment_no;

-- 本轮会话提交（证明每个 input_seq 只有一条终态）
SELECT tenant_id, agent_app_id, session_id, input_seq, commit_id, outcome, fence, session_version, reply_cursor, result_ref
FROM session_commit
WHERE tenant_id IN ('<TENANT_A>', '<TENANT_B>')
ORDER BY tenant_id, session_id, input_seq;

-- 会话头（fence 单调性与推进状态）
SELECT tenant_id, agent_app_id, session_id, version, last_fence, last_session_seq, next_input_seq, updated_at
FROM session_head
WHERE tenant_id IN ('<TENANT_A>', '<TENANT_B>');

-- 入站去重事实
SELECT tenant_id, channel, external_account_id, external_message_id, request_id, agent_app_id, session_id, input_seq, state
FROM inbox
WHERE tenant_id IN ('<TENANT_A>', '<TENANT_B>')
ORDER BY created_at DESC LIMIT 20;
```

采样间隔 `DELTA_POLL` 必须随结论记录。**不能声称比采样更精确的 RTO。**

---

## 五、完整验收步骤（含时间口径）

| 步骤 | 动作 | 记录 | 判据 |
|---|---|---|---|
| 1. N1 基线 | 仅运行 N1；Bot A/B 分别在各自会话写入并回忆不同代号 | 代号 `ALPHA`/`BETA`、`session_commit` 行、`delivery_ledger` 行 | 两租户各自正确；每输入一条终态 |
| 2. 双节点基线 | 启动 N2，观察完整 `W_BASELINE` | `/statusz`（两节点 + 入口）、owner 名、pending 计数 | 无持续 owner 抖动、无 backlog 增长、入口两个后端均 healthy |
| 3. 故障 | 选取**实际承担职责**的节点，`docker kill --signal=KILL`，记录 `t0` | 受害 `process_start_id`、`docker inspect` | 受害节点未自动重启（两次确认） |
| 4. 接管 | 查询 lease、pending stream、delivery claim、队列积压、健康状态 | `t_connect` / `t_worker` / `t_ready` | 存活节点在预设时限内取得有效职责 |
| 5. 原会话新消息 | 接管后两 Bot 分别发**不含代号**的新消息 | 客户端回复、`delivery_ledger`、`webui_message` | 正确 tenant、各自历史代号、唯一最终回复 |
| 6. 重复验证 | `W_REPEAT` 内以重叠请求再测一轮 | 连接振荡、积压、迟到重复回复 | 无迟到 duplicate final，无永久 pending |
| 7. 收尾 | 保留证据目录；如需保留环境则 `KEEP_ENVIRONMENT=true` | `RUN_ID`、`DELTA_POLL`、镜像 digest | 证据可复核 |

---

## 六、通过判定表

| 条件 | 判定 | 证据 |
|---|---|---|
| 租户与会话 | 两条消息命中**原** tenant/session，且仅回忆本租户代号 | `session_commit` 的 tenant 归属 + 官方会话后端 `session_events.app_name` 的 `tenantID/agentAppID` 前缀；客户端回复内容 |
| 自动接管 | 受害节点持续停止；存活节点在预设时限内取得有效职责 | `docker inspect` 两次 `Running=false`；`/statusz` 新 owner（含新 `process_start_id`）；lease fence 递增 |
| 最终回复 | 每个输入一条客户端可见 final，与 terminal/delivery 记录一致 | `delivery_ledger.state='sent'` 每 segment 一行；`webui_message` 一致 |
| 不重发 | `sent` 后不出现第二条同内容最终回复 | `delivery_ledger` 无重复终态；客户端无重复 |
| 不串扰 | A 的回复不含 B 的代号，反之亦然 | 回复内容 + tenant 关联校验 |
| 上下文不丢 | 进程内存全失后仍能回忆代号 | 答对 `ALPHA`/`BETA`；官方会话后端 `session_events`（按 `app_name`/`user_id`/`session_id`）可重放出同一上下文 |
| 稳定性 | 无持续 ownership 抖动、无长期 pending、无迟到 duplicate final | reclaim 日志、pending 计数、`W_REPEAT` 观察 |
| 时序 | 各阶段在 `T_CONNECT` / `T_READY` / `T_REPLY` 内完成 | 带 `DELTA_POLL` 的时间记录 |

---

## 七、稳定性与负载检查方法

本包承诺"故障后继续对话"，因此除单次强杀外，还须在**持续负载**下证明不退化。

### 7.1 连接稳定性（无连接振荡）

**观测：** 在 `W_BASELINE` 与 `W_REPEAT` 两个窗口内，每 `DELTA_POLL` 采样一次入口 `/statusz` 与两节点 `/statusz`。

**判据：**

- 入口 backend 的 `healthy` 字段不应在两个窗口内反复翻转（振荡）；允许一次因果明确的翻转（对应节点终止），不允许持续抖动。
- 两节点的 `/statusz.process_start_id` 在窗口内必须**不变**（除非本轮显式重新加入）。
- stream consumer 的 pending 数不应持续在高位徘徊。

**反例：** 若采样发现 owner 名每几秒变化一次 → 说明有进程在反复重启或身份被复用 → 判不通过。

### 7.2 任务积压（backlog 不增长）

**观测：** 在两个窗口内周期记录 Redis stream 的 pending 总数与 oldest pending 年龄，以及 `execution_record` 中 `outcome='queued'/'running'` 的记录数。

**判据：**

- 稳态下 pending 数量围绕一个上界波动，不单调增长。
- oldest pending 年龄不应超过 `ReclaimInterval` + `LeaseTTL` + 容差的量级；若持续超过，说明 reclaim 没跟上或 worker 不健康。
- 压测方式：在 `W_BASELINE` 内以固定速率持续投递（每租户多条会话，避免人为串行化），然后在窗口末尾强杀其中一个节点，观察 pending 是否在 `T_READY` 内回落。

**反例：** 积压持续增长且 `readyz` 一直 200 → 说明是吞吐不足而非依赖故障，属容量问题，应记入容量结论而非"接管失败"。

### 7.3 重复回复检查

**观测：**

1. `delivery_ledger` 层面：同一 `(tenant_id, delivery_key, segment_no)` 只能有一条终态；`sent` 之后不得再出现 `sending`。
2. `session_commit` 层面：同一 `(tenant_id, agent_app_id, session_id, input_seq)` 只能有一条终态（终态唯一索引保证）。
3. 客户端层面：同一聊天在 `W_REPEAT` 内不得出现第二条同语义最终回复。

**SQL 快速检查：**

```sql
-- 1) 每输入是否只有一条终态提交（期望 0 行）
SELECT tenant_id, agent_app_id, session_id, input_seq, count(*)
FROM session_commit
WHERE outcome IN ('succeeded','denied','failed','cancelled','confirmation_denied','confirmation_timeout')
GROUP BY 1,2,3,4 HAVING count(*) > 1;

-- 2) 是否存在同一 segment 的多次终态（期望 0 行；主键应保证唯一）
SELECT tenant_id, delivery_key, segment_no, count(*)
FROM delivery_ledger
GROUP BY 1,2,3 HAVING count(*) > 1;

-- 3) 是否存在已 sent 却仍被重新领取的痕迹（期望无 sending 且 attempt 未继续增长）
SELECT tenant_id, delivery_key, segment_no, state, attempt, claim_owner, claim_until, updated_at
FROM delivery_ledger
WHERE state IN ('sending','ambiguous')
ORDER BY updated_at DESC;
```

### 7.4 分片与"重复"的区分

若原始回复超过渠道单条上限，`Deliver` 会按 `MaxTextBytes()` 分片，`segment_count > 1`，**每个 segment 一行 Ledger 记录**。客户端看到多条消息属正常分片，不是重复。

**判据：** 核对 `segment_count` 与客户端条数是否一致；若 `segment_count = 1` 而客户端看到两条 → 才是真重复，按 `release-and-operations.md` §7.6 处置。

---

## 八、反例清单（以下都不算通过）

| 反例 | 为什么不算 |
|---|---|
| 人工等到系统恢复后才发一条成功消息 | 只证明一次业务响应，不是精确 RTO，且未证明"另一节点接管" |
| 关闭聊天窗口后重新发 | 不是创建或恢复服务端 session 的证据 |
| 依据模型自报"我是 Tenant A" | 不是可信路由证据；只有 binding/tenant/session/delivery 一致关联才算 |
| 只看 `/livez` 通过 | `/livez` 恒 200，不代表能处理业务；必须 `/readyz` + `/statusz` owner |
| 受害节点被 Docker 自动重启后通过 | 验证的是"重启恢复"而非"接管"；本包 `restart:"no"` |
| 手工删除租约 / 重新入队后通过 | 证明的是人工修复 |
| 只发新消息，不检查原会话历史 | 无法证明上下文回忆正确 |
| 只核对客户端，不核对服务端记录 | 无法证明租户归属不串扰 |
| 用 `W_REPEAT` 缺失的短窗口下结论 | 迟到重复回复可能落在窗口之外 |
| 把一次强杀演练外推为容量承诺 | 演练证明可用性，不证明容量 |

---

## 九、真实 WeCom 外部验收步骤

自动化部分只覆盖"入口存活 + 存活节点就绪 + 证书/身份未复用"。真实原会话对话必须由外部账户完成。

1. 以 `TRPC_WECOM_REAL_ACCEPTANCE=recorded` 运行演练，或手动保留环境（`TRPC_WECOM_HA_KEEP_ENVIRONMENT=true`）。
2. 在 node-a 被强杀、node-b 接管后，打开企业微信，通过 **Bot A** 在**原会话**发送一条带唯一标记（如 `HA-A-<RUN_ID>`）的新消息。
3. 核对：Bot A 仅回复 Tenant A 的代号 `ALPHA`，且同一聊天中只出现**一条**最终回复。
4. 通过 **Bot B** 在原会话发送带唯一标记的新消息，核对仅回复 Tenant B 的代号 `BETA`，且**不含** `ALPHA`。
5. 服务端核对：`delivery_ledger`（tenant_id 分别为 Tenant A/B，`state='sent'`，每 segment 一行）与 `webui_message`（`channel_binding_id` 分别对应两个 binding）。
6. 将客户端截图 + 服务端行导出至独立运行目录；记录 `RUN_ID`、`DELTA_POLL`、镜像 digest，以及接管前后 node-b 的 `/statusz`（含 `process_start_id`）。
7. 验证无跨租户痕迹：`session_events`（按两个 Bot 各自的 `app_name` 前缀）/ `session_commit` 中不存在另一 tenant 的历史、工具结果或凭据痕迹。

> 真实验收是外部前置。默认 `assumed` 只表示"自动化可观察部分通过"，**不替代**上述人工/外部核对。对外报告时必须显式标注本轮采用的是 `assumed` 还是 `recorded`。

---

## 十、与相邻文档包的边界

| 本包验证 | 不验证（见相邻包） |
|---|---|
| 新消息在单节点故障后仍在**原会话**正确对话 | 原始**在途**消息在 P1–P4 各窗口的精确接管 → `inflight-task-takeover` |
| 单节点故障（受害节点持续停止） | 同主机实例**重新加入**与**反向故障** → `single-host-multicontainer` |
| 共享 PostgreSQL/Redis 正常前提下的接管 | 共享依赖故障、跨主机、整机断电（不承诺） |

> ⚠️ 不要因为"后一条新消息成功"就判定原在途任务已被接管。两者是不同证据面。
