# 可靠性能力参考运行时规范（跨包共享）

> 本规范是三个可靠性包**共同依赖的最小运行时契约**：部署单元、配置契约、原子写入规则、Redis 契约、角色接口与保证边界。
> 把整个 `reliability-failover` 文档目录复制出去后，实施者可用任意语言/框架重建等价行为；下文用 PostgreSQL、Redis Stream 与 Go 风格接口作为确定性参考。
>
> **权威 DDL 在哪里：** 三个能力各自所需的完整表、约束、索引与关键 SQL 函数（逐字），分别位于各包的 `REFERENCE-IMPLEMENTATION.md`：
>
> | 包 | DDL 章节 |
> |---|---|
> | `conversation-continuity` | `REFERENCE-IMPLEMENTATION.md` §2（inbox / session_head / session_commit / session_event / execution_record / outbox / delivery_ledger / result_payload / channel_* 等 + `claim_inbox` / `prepare_dispatch` / `commit_turn` / `park_execution`） |
> | `inflight-task-takeover` | `REFERENCE-IMPLEMENTATION.md` §2（同上，另侧重 P1–P4 相关边界与 preprocess_job / outbox / delivery_ledger 的完整约束） |
> | `single-host-multicontainer` | `REFERENCE-IMPLEMENTATION.md` §2–§4（入口实现、Compose 服务定义、`/statusz` 契约、健康判定矩阵） |
>
> 本规范不再重复列出简化版 DDL，以免与真实 schema 产生偏差。下文只保留**跨包共享的契约**。

---

## 1. 最小部署单元

```text
entry × 1          固定公开 callback URL，按 readiness 转发
application × 2    N1/N2；均运行 ingress、preprocess、worker、relay、delivery
postgres × 1       业务权威：binding/route、Inbox、session、result、Outbox、Ledger
redis × 1          Stream、consumer group、lease/fence 计数
```

每个 application 必须有**稳定 `InstanceID`**（部署槽位）和**每次启动随机生成的 `ProcessStartID`**。所有 owner / consumer 名称使用：

```text
<prefix>-<role>-<InstanceID>-<ProcessStartID>
参考实现中 prefix = "webui-local"（例如 webui-local-worker-wecom-ha-node-a-9f3c1a2b4d5e6f07）
```

**为什么必须两段：** `InstanceID` 只标识"哪个部署槽位"，无法区分同一槽位里换过的进程。容器重建后 `InstanceID` 不变，若名称不含 `ProcessStartID`，新进程会被误认为仍持有旧 lease / 旧 consumer 位置 / 旧 delivery claim。

---

## 2. 必要配置契约

| 配置 | 示例 / 装配值 | 语义 |
|---|---|---|
| `INSTANCE_ID` | `node-a` / `node-b` | 固定部署槽位，两节点不同 |
| `PROCESS_START_ID` | 随机 8 字节 hex（16 字符） | 每次进程启动不同，**不可配置** |
| `POSTGRES_DSN` | 连接串 | 两节点必须指向同一业务数据库 |
| `REDIS_ADDRESS` | `redis:6379` | 两节点必须指向同一协调与队列服务 |
| `LISTEN_ADDRESS` | `:8080` | 应用监听地址 |
| `LEASE_TTL` | `30s` | 会话执行许可有效期 |
| `LEASE_RENEW_INTERVAL` | `10s` | 必须小于 TTL |
| `RECLAIM_INTERVAL` | `5s` | pending 工作回收扫描周期 |
| `CLAIM_TTL` | `30s` | Delivery Ledger 领取有效期 |
| `CLAIM_RENEW_INTERVAL` | `10s` | 必须小于 CLAIM_TTL |
| `ENTRY_BACKENDS` | `http://node-a:8080,http://node-b:8080` | 至少两个内部 application 地址；`http`、无 user/query/fragment、去重 |
| `ENTRY_PROBE_INTERVAL` | `1s` | readiness 探测周期，范围 `[100ms, 1m]` |

**不能把租约 TTL、reclaim 周期或入口探测周期写死在业务逻辑中**：它们决定故障检测时限，必须可记录，并在演练前固定。

> ⚠️ 实现里常有"代码级兜底默认值"（例如 `LeaseTTL<=0` 时退化为 5s）。**文档、告警阈值与时限结论必须引用装配值**，两者差一个数量级会导致完全错误的 RTO 结论。

---

## 3. 原子写入规则（跨包共享）

### 3.1 接收原消息

在一个事务内：按 Inbox 唯一键插入；若重复则读回已有 `request_id`；若首次插入则写 dispatch outbox。**事务提交后才向企业微信确认接收。**

### 3.2 分配输入序号并产生执行任务

在一个事务内：校验 tenant/agent/config/policy 版本；`INSERT ... ON CONFLICT DO NOTHING` 建 session head；`SELECT last_allocated_input_seq+1 ... FOR UPDATE` 分配序号；插入 `execution_record`；更新 inbox 为 `dispatch_ready`；写 dispatch outbox。**重复调用必须收敛到同一 `input_seq`。**

### 3.3 提交 terminal turn

在一个事务内：锁 session head；验证 `fence >= last_fence`、`input_seq == next_input_seq`、`expected_version == version`；插入 terminal turn 与 session events；推进 head（version / last_fence / last_session_seq / next_input_seq / state）；写 reply outbox。任一条件不满足即回滚并返回冲突，**不 ACK 队列**。

### 3.4 领取 Delivery

用条件更新领取 `pending`、到期 `sending`（先改写为 `ambiguous/owner_lost`）或到期 `retry_wait` 记录，更新 owner、claim_until、version 与 state。更新条件必须含旧 version/state/owner/client_request_id，防止两个发送者同时认为自己领取成功。

---

## 4. Redis 参考契约

| 键 / 资源 | 用途 | 规则 |
|---|---|---|
| `work:<shard>` Stream | 执行任务 | 先 terminal commit，后 ACK；consumer group 支持 reclaim |
| `reply:<destination>` Stream | 待发回复 | 先 Ledger terminal，后 ACK |
| `lease:<tenant>:<app>:<session>` | 当前 lease | value = `owner\|leaseID\|fence`；带 TTL 的原子获得/续约/释放 |
| `fence:<tenant>:<app>:<session>` | 递增 fence 计数 | 获得 lease 前必须保证不小于数据库 `last_fence` |

参考实现的键构造（hash tag 保证同一会话的 lease/fence 落在同一 slot）：

```text
tag   = hex(sha256(tenantID + "\x00" + agentAppID + "\x00" + sessionID))
prefix = "trpc:<environment>:{" + tag + "}"
lease  = prefix + ":lease"
fence  = prefix + ":fence"
```

获得 lease 的原子语义：若 lease key 不存在 → `INCR` fence → 写入 `owner|leaseID|fence` 并设置 TTL；否则返回冲突。续约与释放必须比较 `owner`、`leaseID`、`fence` **三者**，旧进程不得续约或释放新进程的 lease。

`EnsureFenceAtLeast(minimum)`：按十进制字符串比较 fence 计数，`current < minimum` 时 `SET minimum`。**这是必需的**：Redis 计数可能因重启/主从切换回退，若不校准，新 lease 可能拿到小于数据库 `last_fence` 的 fence，合法接管者反而被数据库拒绝。

完整 Lua 脚本逐字版本见 `conversation-continuity/CODE-APPENDIX.md` §7 与各包 `REFERENCE-IMPLEMENTATION.md`。

---

## 5. 应用角色接口

```go
type BindingResolver interface {
    ResolveAndVerify(ctx context.Context, callback Callback) (TenantContext, error)
}
type InboxStore interface {
    ClaimAndEnqueue(ctx context.Context, in AcceptedInput) (requestID string, duplicate bool, err error)
}
type LeaseManager interface {
    Acquire(context.Context, SessionKey, string, time.Duration) (Lease, error)
    Renew(context.Context, Lease, time.Duration) (Lease, error)
    Release(context.Context, Lease) error
    EnsureFenceAtLeast(context.Context, SessionKey, uint64) error
}
type SessionStore interface {
    ReadLastFence(context.Context, SessionKey) (uint64, error)
    OpenForRun(context.Context, OpenForRunRequest) (SessionHead, error)
    CommitTurn(context.Context, CommitTurnRequest) (CommitTurnResult, error)
    GetTerminalByInputSeq(context.Context, TerminalKey) (CommitTurnResult, error)
    LoadSession(context.Context, SessionKey) (SessionSnapshot, error)
}
type DeliveryStore interface {
    Claim(ctx context.Context, key DeliveryKey, owner string, ttl time.Duration) (Record, bool, error)
    Finish(ctx context.Context, record Record, expectedVersion uint64) error
    Reconcile(ctx context.Context, record Record) (Record, error)
}
```

任何语言都可实现这些接口；核心是**契约语义**（尤其是 fence 校准、条件更新、ACK 时序），而不是函数名。

---

## 6. 错误语义契约（跨包共享）

数据库必须把拒绝原因映射为**两类不同处置**：

| 类别 | 典型来源 | 上层处置 |
|---|---|---|
| **可重试竞争** | `40001` 版本冲突、`23505` 幂等键冲突 | 按 `RetryWait` 重试，收敛即成功 |
| **不可重试不一致** | `40001` 且消息含 `stale fence`（迟到写）、`XX001`（终态不变量缺失）、`42501`（作用域/租户不匹配） | 上抛 + 告警；**禁止**当竞争重试 |

把第二类当第一类处理（无限重试）是本类系统最常见的隐性缺陷：它会持续消耗资源并掩盖真实故障。

---

## 7. 一致性与能力边界

- **至少一次执行：** pending 工作故障后可重跑；**模型调用次数可能大于一**。
- **一次有效 terminal commit：** session version、input sequence 与 fence 共同保证。
- **一次业务效果：** 写外部系统必须使用业务幂等键（如 `tenant + 原请求 + 业务操作`），并保存外部资源 ID 以便对账。
- **投递恰好一次取决于下游：** 下游必须能按 `client_request_id` 去重或查询；否则 P4 只能诚实提供至少一次策略，并保留 `ambiguous` 与告警。
- **单应用实例故障：** 稳定入口 + 共享权威态 + 独立身份可承接；**跨主机 / 整机 / 共享依赖故障不在承诺范围**。

**三类计数必须分开表述：**

| 计数 | 位置 | 是否会因接管而增加 |
|---|---|---|
| 模型调用次数 | 执行期 | ✅ 会（P2 接管可重跑模型） |
| 任务尝试次数 | `execution_record.park_attempt`、`delivery_ledger.attempt`、`reconcile_attempt` | ✅ 会（reclaim / retry 会增长） |
| 最终回复次数 | `delivery_ledger` 每 segment 一行，`state='sent'` 后不可重发 | ❌ 不会（由幂等约束保证） |

---

## 8. 与三个交付包的关系

| 文档 | 作用 |
|---|---|
| `STANDALONE-SOLUTION-GUIDE.md` | 面向非代码读者/管理层的完整方案说明（不依赖本规范） |
| `TEACHING-GUIDE.md` | 面向工程实现者的教学串联（状态权威、租约/fence、P1–P4） |
| `conversation-continuity/` | 故障恢复后原会话继续对话（含完整 DDL / Lua / 接口 / 算法） |
| `inflight-task-takeover/` | P1–P4 在途任务接管与幂等投递 |
| `single-host-multicontainer/` | 稳定入口、单主机双实例生命周期、重新加入与反向故障 |

每个包均提供：`README.md`（索引 + 复现直达）、`OVERVIEW.md`、`FULL-GUIDE.md`、`design.md`、`implementation-walkthrough.md`、`CODE-APPENDIX.md`、`REFERENCE-IMPLEMENTATION.md`（**从零复现材料**）、`testing-and-acceptance.md`、`release-and-operations.md`。
