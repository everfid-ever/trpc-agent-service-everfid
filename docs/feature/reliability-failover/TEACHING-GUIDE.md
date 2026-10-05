# 多租户故障接管实现教程

> **适用范围：** 本文讲解仓库中已经落地的前三个可靠性能力：故障后原会话继续对话、在途任务接管与幂等投递、单主机双实例可用性。它是实现原理、代码走读和演练方式的统一入口；各子目录的文档保留为可执行的验收与运维手册。
>
> **先明确边界：** 本地代码、单元测试、Compose 配置与故障演练脚本已经实现；真实企业微信最终可见回复需要凭据和手机端会话，按 `TRPC_WECOM_REAL_ACCEPTANCE=assumed` 作为外部验收前置。本文不宣称跨主机高可用，也不把无法由下游协议去重的投递写成“绝对恰好一次”。

## 1. 先理解：系统究竟在保护什么

这不是“让两个容器都活着”的问题。系统同时保护四件事：

1. **归属正确：** Bot A 的任何输入只能进入 Tenant A；Bot B 同理。
2. **上下文不断：** 新节点必须从持久化会话中读取已提交历史，而不是依赖旧节点内存。
3. **处理可恢复：** 已持久接收但未完成的原消息，在节点消失后可被其他节点继续处理。
4. **结果不乱：** 旧节点不能覆盖新节点结果；同一输入不能无限重复生成最终回复或外部写入。

把它们写成系统不变量会更容易审阅代码：

| 不变量 | 代码中的实现策略 | 失效时的风险 |
|---|---|---|
| Tenant 由可信身份推出 | WeCom binding 验签后才建立 Tenant Context | 用户参数伪造 tenant，造成串租户 |
| 每个 provider 输入只有一个 durable 事实 | Inbox 唯一认领 + dispatch outbox | 平台重试导致重复任务 |
| 同一会话同一时刻只有一个有效提交者 | Redis lease + 递增 fence + PostgreSQL fence 校验 | 脑裂旧节点覆盖新结果 |
| terminal 结果和待发回复一起提交 | session `CommitTurn` 与 reply outbox 原子化 | 结果有了、回复丢了，或反复重跑模型 |
| 下游发送可接管、可追责 | Delivery Ledger claim / TTL / terminal `sent` | 发送节点崩溃后重复或丢回复 |

## 2. 全局架构：不要把 Redis 当成最终事实

```text
               ┌────────────── 可信路由 ───────────────┐
WeCom Bot A/B ─┤ stable callback entry                 │
               │     │ /callbacks/wecom                │
               │     ▼                                 │
               │  app N1 / app N2                      │
               └─────┬─────────────────────────────────┘
                     │ Inbox + dispatch outbox（PostgreSQL）
                     ▼
              Redis Work Stream ── Worker N1/N2
                     │                 │ lease + fence
                     ▼                 ▼
       PostgreSQL Session / terminal result / reply outbox
                     │
                     ▼
              Redis Reply Stream ── Delivery N1/N2
                     │
                     ▼
           PostgreSQL Delivery Ledger ── WeCom Reply API
```

这里的分工是这套实现的关键：

- **PostgreSQL 是权威事实。** tenant binding、Inbox、会话 head、terminal turn、reply outbox、delivery ledger 都必须能在进程消失后重新读取。
- **Redis 是协调与可重放传输。** Stream pending 可以被 reclaim；lease 的 TTL 让故障节点最终失去所有权；但 Redis 自身不裁决旧节点是否能写回，所以还要有 PostgreSQL fence。
- **应用节点是可替换计算者。** N1/N2 都可运行 gateway、preprocess、worker、relay、delivery；任何一个被强杀后，状态不会随它消失。

建议按这条链路读代码，而不是从某个 HTTP handler 随机跳入：**身份 → durable inbox → 可重放任务 → 有效执行权 → 原子提交 → 幂等投递**。

## 3. 第一项：多租户原会话恢复与连接/职责接管

### 3.1 为什么“模型能答出代号”还不够

模型自报“我是 Tenant A”没有证明路由正确。可靠的判据是：

1. callback 被验签并解析到唯一 binding；
2. binding 决定 tenant、Agent 和凭据作用域；
3. session key 含 tenant/binding/外部用户与聊天身份；
4. 服务端持久化记录和最终投递记录与客户端回复相互对应。

因此，Tenant B 不能只是 Prompt 中的一句名字，也不能复用 Tenant A 的工具、memory 或 session state。

### 3.2 双 Bot 是怎样被配置出来的

入口代码在 [`cmd/trpc-service/webui_local_role.go`](../../../cmd/trpc-service/webui_local_role.go)。启动时：

1. 读取主 Bot 的 `WECOM_*` 与第二 Bot 的 `WECOM_SECONDARY_*` 配置。
2. 只有显式设置 `TRPC_WECOM_SECONDARY_LOCAL_ENABLED=true`，且两个 `(Corp ID, Agent ID)` 组合不同，才建立第二套 tenant/agent/policy/binding。
3. Tenant B 使用独立的 `local-wecom-secondary` route 与 tenant-scoped Secret 投放；不会注册 Tenant A 的 local-note 工具与 memory service。
4. 两个节点载入相同配置版本，因此任一节点都有按设计接管两个 tenant 的能力。

这条设计回答了一个常见问题：**为什么不是在入口根据请求体的 `tenant_id` 分流？** 因为那个字段属于不可信外部输入。正确路径是“route key 找候选 binding → 在 binding 范围内验签 → 验签成功后创建 Tenant Context”。

### 3.3 会话为什么能在进程消失后继续

会话的核心是一个带 tenant 的 key，而不是节点内存对象。可以把逻辑简化成：

```go
key := SessionKey{TenantID, AgentAppID, SessionID}
head := sessions.OpenForRun(ctx, key, requestID, inputSeq, fence)
context := LoadCommittedTurns(head)
answer := RunModel(context, userInput)
sessions.CommitTurn(ctx, requestID, inputSeq, fence, answer, replyOutbox)
```

这里有三个刻意的限制：

- 只有 **已提交** turn 进入后续模型上下文，半生成内容不会污染下一轮。
- `CommitTurn` 使用 request ID、input sequence、session version 和 fence 做校验；它不是无条件 append。
- terminal turn 与 reply outbox 在同一持久化提交中生成，所以“模型结果已经确认但回复还没发”是可恢复状态，而不是数据丢失。

实际接口定义见 [`trpcservice/storage/session/atomic.go`](../../../trpcservice/storage/session/atomic.go)，PostgreSQL 校验与提交实现见 [`trpcservice/storage/session/postgres/store.go`](../../../trpcservice/storage/session/postgres/store.go)。

### 3.4 lease 和 fence：为什么需要两层，而不是一个 Redis 锁

Lease 解决的是“**现在谁可以执行**”；fence 解决的是“**晚到的人还能不能提交**”。

```text
N1 acquire lease，fence=41
N1 执行中断线，无法 renew
lease TTL 到期
N2 acquire lease，fence=42
N2 成功提交 terminal turn（last_fence=42）
N1 网络恢复，带 fence=41 尝试 CommitTurn
PostgreSQL 拒绝 stale fence，N1 无法覆盖 N2
```

Worker 在 [`trpcservice/worker/consumer.go`](../../../trpcservice/worker/consumer.go) 的处理顺序为：

```go
persisted := sessions.ReadLastFence(sessionKey)
leases.EnsureFenceAtLeast(sessionKey, persisted)
lease := leases.Acquire(sessionKey, workerID, ttl)
ExecuteWithLease(lease.Fence, beforeCommitRenewal)
CommitTurn(..., Fence: lease.Fence)
AckOnlyAfterTerminalCommit()
```

`EnsureFenceAtLeast` 的意义很容易被忽略：Redis 计数若因恢复而倒退，不能让新 lease 获得一个小于数据库 `last_fence` 的值。Redis 实现使用 Lua 保持 fence 递增，见 [`trpcservice/coordination/redis/lease.go`](../../../trpcservice/coordination/redis/lease.go)；数据库才是最后一道拒绝旧 fence 的边界。

### 3.5 一个新节点如何真正接管

强杀 N1 后，系统不做“人工修复”操作。自然发生的是：

```text
N1 停止 heartbeat / lease renew / stream ACK
    ↓
Redis pending entry 在 reclaim 条件满足后可被 N2 看见
    ↓
N2 读 durable last_fence，申请更高 lease fence
    ↓
N2 重试或继续该 request，原子写 terminal turn + reply outbox
    ↓
N2 的 relay / delivery 继续投递
```

这一点与“重启 N1 后问题消失”不同：验收期间 N1 必须保持停止，才能证明存活节点真的接管了职责。

### 3.6 如何观察，而不是猜测

`webui-local` 的实例标识由部署传入的 `TRPC_WEBUI_LOCAL_INSTANCE_ID` 和进程启动时随机生成的 `process_start_id` 组成。代码会据此构造 worker、preprocess、relay、delivery 等 owner/consumer 名称，并通过 `/statusz` 输出匿名运行态。

```text
webui-local-worker-wecom-ha-node-a-<process-start-id>
webui-local-delivery-wecom-ha-node-b-<process-start-id>
```

这解决了“容器名字没变但进程已经换代”的问题。不要根据 N1/N2 名称猜所有者；应把 lease、Stream consumer、日志和 `/statusz.owners` 对齐。

## 4. 第二项：在途任务接管与幂等投递

### 4.1 先分清四个边界

同样是“用户没收到回复”，故障发生的位置不同，恢复策略完全不同：

| 阶段 | Durable 事实 | 不应做的事 | 正确恢复路径 |
|---|---|---|---|
| P1：已接收未执行 | Inbox/preprocess job | 让用户重发、手工重新入队 | reclaim 或继续 dispatch |
| P2：模型/工具中 | task pending + lease | 让旧 owner 继续提交 | 过期后新 owner 更高 fence 重试 |
| P3：结果已提交未发送 | terminal result + reply outbox | 重跑模型 | relay/delivery 发送已保存结果 |
| P4：下游已接受未确认 | provider receipt + sending ledger | 盲目再发并宣称 exactly-once | reconcile、去重或诚实保留 ambiguous |

P1–P4 不是状态名游戏，而是把“故障发生在哪里”变成可重复命中的边界。只有准确命中，测试才能解释为什么恢复策略正确。

### 4.2 代码如何制造可控故障，而不污染生产路径

测试控制面位于 [`trpcservice/reliability/inflight/barrier.go`](../../../trpcservice/reliability/inflight/barrier.go)。它只定义一个很小的接口：

```go
type Barrier interface {
    Wait(context.Context, Observation) error
}
```

业务组件只依赖这个接口，不依赖 HTTP、Docker 或测试脚本。正常运行时 barrier 为 `nil`，不会暂停、不会重试、更不会删除租约。只有同时设置下列变量才会创建 controller：

```text
TRPC_INFLIGHT_TEST_BARRIER=p1|p2|p3|p4
TRPC_INFLIGHT_TEST_BARRIER_INSTANCE_ID=auto|wecom-ha-node-a|wecom-ha-node-b
TRPC_INFLIGHT_TEST_BARRIER_TENANT_ID=<可选>
```

`auto` 的用途是：无需猜测哪台节点真正领取了本次任务。两个节点各自拥有 process-local controller；脚本读取 `/statusz` 找到真正 hit 的节点，再终止它。存活节点的 controller 被释放后，只允许正常的 reclaim 路径继续。

### 4.3 四个 hook 的精确位置

| Point | 实际文件 | 挂点含义 | 教学上的关键判断 |
|---|---|---|---|
| P1 | [`preprocess/worker.go`](../../../trpcservice/preprocess/worker.go) | durable job 已有，调用 `dispatch` 前 | 证明“收到”不是 HTTP 200，而是可恢复事实已存在 |
| P2 | [`worker/runner.go`](../../../trpcservice/worker/runner.go) | 模型/工具和渲染结束，terminal commit 前 | 允许模型重跑，但不允许旧 fence 写回 |
| P3 | [`channels/delivery/service.go`](../../../trpcservice/channels/delivery/service.go) | Ledger 已 claim，调用 Adapter 前 | 结果已保存，只应恢复发送 |
| P4 | 同上 | Adapter 返回 receipt，`FinishDelivery(sent)` 前 | 验证不确定投递窗口与下游去重边界 |

例如 P2 的故障故事是：N1 已调用模型但还没有 `CommitTurn` → N1 被杀 → pending task 未 ACK → N2 reclaim → N2 获得更高 fence → 可以重新执行模型 → 只有 N2 能提交 terminal turn。模型调用次数可能大于 1；这不等于业务结果重复。

### 4.4 为什么“幂等”不能只靠一个布尔字段

这套实现使用不同层的幂等键和条件更新：

| 对象 | 识别方式 | 保护的重复 |
|---|---|---|
| 输入 | tenant/binding/provider message identity + payload digest | Provider callback 重试 |
| 执行提交 | request ID + input sequence + expected session version + fence | 双 worker / 旧 owner 提交 |
| 回复片段 | tenant + delivery key + segment number | relay 反复发布 |
| 外部写工具 | tenant + request ID + operation 的业务幂等键 | 模型/工具重试产生重复写入 |

它们不能合并成一个“已经处理”标记：输入可以已收到但未执行，执行可以完成但未投递，投递可以已被下游接受但本地还没确认。

### 4.5 Delivery Ledger 的读法

Delivery service 的主线可以理解为：

```go
record, claimed := ledger.ClaimDelivery(key, owner, ttl)
if !claimed { return HandleExisting(record) }

receipt := adapter.Deliver(event, record.ClientRequestID)
ledger.FinishDelivery(record, receipt)
AckReplyEvent()
```

`ClaimDelivery` 是“谁有资格发送”的租约；`sent` 是不可逆 terminal 状态。若网络在 adapter 调用后断开，应用无法仅靠 timeout 推断下游是否收到了消息，应进入 `ambiguous` 或使用下游 receipt/reconcile 能力。

因此，对外表述应严谨：**系统对执行可实现至少一次，对持久化 terminal commit 通过 fence 实现一次有效提交；最终用户可见的恰好一次依赖 WeCom 或下游的 client request ID 去重/查询能力。**

## 5. 第三项：单主机双实例与稳定 callback 入口

### 5.1 为什么入口要独立于 N1/N2

如果 WeCom callback 直接指向 N1，N1 被杀时即使 N2 的 worker 能接管，新的用户输入也进不来。第三期把公开地址固定在独立入口：

```text
public HTTPS tunnel / WeCom callback URL
                │
         wecom-ha-entry
          ├─ probe N1 /readyz
          └─ probe N2 /readyz
                │
           healthy node only
```

入口实现位于 [`cmd/trpc-service/wecom_ha_entry_role.go`](../../../cmd/trpc-service/wecom_ha_entry_role.go)：

1. 读取至少两个 `TRPC_WECOM_HA_ENTRY_BACKENDS`；拒绝异常 URL、重复后端和不合理探测周期。
2. 周期性探测每个后端的 `/readyz`，而不是仅看容器进程是否存在。
3. `/readyz` 只有至少一个后端健康时返回成功；`/statusz` 输出每个后端的健康布尔值。
4. `/callbacks/wecom` 仅接受 GET/POST，限制请求体大小，轮询健康后端。
5. 后端连接失败、502、503、504 时将其标为不健康，并在本次请求还未返回时尝试另一个健康后端。

入口重试并不替代业务幂等：它只在 callback 返回前换后端；真正防止 Provider 重传与入口切换造成重复处理的仍是 durable Inbox identity。

### 5.2 Compose 如何确保它是“两个实例”

[`deploy/compose/docker-compose.local.yml`](../../../deploy/compose/docker-compose.local.yml) 的 `wecom-ha-local` profile 做了这些事情：

- `wecom-ha-bootstrap` 只运行一次，负责共享的 tenant/config fixture 初始化；
- `wecom-ha-node-a`、`wecom-ha-node-b` 使用相同 image、相同共享 PostgreSQL/Redis、不同 `TRPC_WEBUI_LOCAL_INSTANCE_ID` 和不同本机调试端口；
- 两节点 `restart: "no"`，强杀验收中 Docker 不会偷偷重建受害节点；
- `wecom-ha-entry` 是唯一对外 callback 端口，两个节点只是内部后端；
- 重新加入必须显式 `docker compose start`，进程会生成新的 `process_start_id`。

这种编排证明的是**单主机应用实例故障**。PostgreSQL、Redis、Docker daemon、磁盘和宿主机仍是同一故障域，所以它不等价于跨主机高可用。

### 5.3 强杀、重新加入、反向故障的真实时序

```text
baseline: N1 ready + N2 ready + entry sees both healthy
    ↓
SIGKILL N1（N1 必须持续 stopped）
    ↓
entry probe marks N1 unhealthy；N2 remains ready
    ↓
N2 接收新 callback，并 reclaim N1 的可接管职责
    ↓
explicitly start N1（new process_start_id）
    ↓
完整稳定窗口：不应重发旧 reply、抢占抖动或 backlog 增长
    ↓
SIGKILL N2，验证 N1 反向承接
```

仓库脚本 [`scripts/e2e/single-host-multicontainer.sh`](../../../scripts/e2e/single-host-multicontainer.sh) 自动验证这些**代码侧生命周期事实**：两个节点和入口 readiness、入口健康集变化、受害容器确实没有自动重启、重新加入后 process identity 改变、反向强杀后入口仍就绪。它会保留阶段性 `/statusz`、Compose 状态与日志的独立诊断目录。

它不伪造真实用户消息；原会话代号、tenant/session/delivery 关联和最终客户端可见回复仍需要按测试文档用真实 Bot 采集。

## 6. 从一次请求读完整代码路径

下面用“Tenant A 用户发送一条消息，N1 在执行中被杀”为例串起来：

```text
1. WeCom → stable entry → N1
2. N1 验签 binding，得到 Tenant A；durable Inbox 认领输入
3. dispatch outbox 发布到 Work Stream
4. N1 consumer 获取 Tenant A/session 的 lease，fence=101
5. N1 调模型；强杀发生在 terminal commit 前
6. 没有 ACK，因此 Stream 保留 pending；lease 会过期
7. N2 reclaim；读 PostgreSQL last_fence，申请 fence=102
8. N2 重跑可重试计算，CommitTurn 原子写 terminal result + reply outbox
9. reply relay 发布 Reply Stream；Delivery Ledger claim 后调用 WeCom
10. 持久化 sent / receipt 后 ACK reply event
```

这条路径的每一步都要能回答两个问题：

- **它的 durable truth 在哪里？** 例如第 2 步是 Inbox、第 8 步是 PostgreSQL session/outbox、第 10 步是 Delivery Ledger。
- **节点在此刻被杀会怎样？** 没有 durable truth 的步骤可重试；已 durable 的步骤只能继续后半段，不能从头乱跑。

## 7. 如何按教学顺序阅读与运行

### 7.1 推荐阅读顺序

1. 先看本教程第 1、2 节，建立“权威状态与可重放传输分离”的模型。
2. 阅读 [故障后继续对话概览](./conversation-continuity/OVERVIEW.md)，再顺着 `webui_local_role.go → worker/consumer.go → session/postgres/store.go` 看 tenant、会话与 fence。
3. 阅读 [在途任务接管概览](./inflight-task-takeover/OVERVIEW.md)，对照 `barrier.go` 和 P1–P4 的四个插入点。
4. 阅读 [单主机双实例概览](./single-host-multicontainer/OVERVIEW.md)，最后看 `wecom_ha_entry_role.go` 与 Compose 编排。

### 7.2 自测层次

```bash
# 代码级：入口、barrier、preprocess、worker、delivery 的单元/包测试
go test -count=1 ./cmd/trpc-service ./trpcservice/reliability/inflight \
  ./trpcservice/preprocess ./trpcservice/worker ./trpcservice/channels/delivery

# 静态部署检查：不启动真实 Bot
docker compose -f deploy/compose/docker-compose.local.yml \
  --profile wecom-ha-local config -q

# 单主机生命周期演练：需要本地 Docker 与 secret 文件；真实 WeCom 结果按外部前置记录
bash scripts/e2e/single-host-multicontainer.sh

# P1–P4：按点暂停、杀真正 owner、释放存活节点
TRPC_INFLIGHT_TEST_BARRIER=p2 \
  bash scripts/e2e/inflight-takeover.sh
```

P1–P4 的完整操作、计时和证据字段见 [在途任务验收手册](./inflight-task-takeover/testing-and-acceptance.md)。不要因为“后一条新消息成功”就判定原在途任务已经接管。

## 8. 常见误解与代码审阅清单

| 误解 | 正确理解 |
|---|---|
| 有 Redis 锁就不会脑裂 | 锁只决定短期 owner；必须由 durable fence 拒绝迟到写入。 |
| ACK 后再慢慢写数据库没问题 | 正好相反：terminal durable commit 在前，ACK 在后。 |
| 发送 API 返回超时就是失败 | 这可能是下游已接受但响应丢失，应进入 ambiguous/reconcile。 |
| 两个容器共享一个缓存目录就能共享会话 | 会话应进支持并发与事务的存储；本地缓存只能是可丢失优化。 |
| Docker 自动重启证明了接管 | 自动重启验证的是重建，不是存活节点接管；两者必须分开测试。 |
| 模型只调用了一次才算正确 | P2 接管允许重跑模型；关注 terminal commit、外部副作用和最终回复的正确幂等边界。 |

审阅新增功能时，至少检查：

1. 新表/查询是否包含 tenant predicate，且外部 tenant 不能直传为可信值；
2. 新任务是否先 durable 再 ACK，是否可在 crash 后重新发现；
3. 新 session 写入是否受 request、sequence、version、fence 共同约束；
4. 新外部调用是否有业务幂等键、receipt 或 reconciliation 策略；
5. 新后台 goroutine 是否使用唯一 owner，是否能在 context 取消时停止；
6. 新容器是否有独立 instance/process identity，且 readiness 真正涵盖运行所需依赖。

## 9. 面试或交接时的精炼讲法

可以用下面的因果链描述这三个能力，而不要只罗列 Redis、PostgreSQL、Docker 等名词：

> 我把多租户消息处理拆成 durable Inbox、可重放 Stream、带 lease/fence 的会话执行、原子 terminal outbox 和 Delivery Ledger 五层。tenant 只由验签 binding 产生；节点故障时 Stream pending 和过期 claim 可被另一实例回收，而 PostgreSQL fence 阻止旧实例迟到覆盖。P1–P4 通过默认关闭的 barrier 精确命中接收、执行、提交、投递边界；单主机场景再用独立健康探测入口保证强杀节点后 callback 不换 URL，配合新 process identity 验证重新加入与反向故障。

这段表述的重点是“每个风险由哪个 durable 边界和哪个机制解决”，因此既能解释实现逻辑，也不会夸大未覆盖的跨主机或下游 exactly-once 能力。
