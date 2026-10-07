# 数据库设计与实现（跨包共享）

> 本文回答"数据库相关设计实现是否说清楚"这一层：**库的划分与 schema 演进、事务边界与隔离级别、锁与并发、加密存储、约束归属、索引、数据生命周期、连接池、备份边界**。
> 三个包各自的 `REFERENCE-IMPLEMENTATION.md` 给出**本子系统表的逐字 DDL 与 SQL 函数语义**；本文不重复逐字 DDL，而是补齐**跨包共享、且此前未在任何地方交代**的数据库设计决策。
> 所有事实均与实现逐字核对；引用位置写成「（可选核对：`path`）」，不作为唯一说明。

---

## 1. 先分清：这套系统里有**两个**数据库

| 库 | schema 来源 | 内容 | 谁连接它 |
|---|---|---|---|
| **业务库**（authoritative） | `migrations/000001_service_schema.up.sql` | 多租户控制面、Session/Event/Commit、Inbox/Outbox、execution、delivery ledger、channel/binding、artifact 引用、迁移状态机、**受控 purge 角色与租户 redaction rules** | 应用节点（gateway / worker / preprocess / relay / delivery）、bootstrap、admin 角色 |
| **合规/审计库**（compliance） | `compliancemigrations/000001_compliance_schema.up.sql` | 不可变审计事实、保留策略、legal hold、quarantine resolution、**窄授权 purge 路径** | 审计角色与 purge 角色（`audit-relay` / `business-audit-purge`） |

**为什么必须分开：**

1. **不可变性要求不同。** 业务库的行会被更新（`outbox.state`、`session_head.version`、`delivery_ledger.state`）；合规库的审计事实一旦写入就不允许修改（由 `reject_audit_event_change` 之类的拒绝触发器保障）。把两者放同一个库会让"业务表的正常 UPDATE"与"审计表的不可变约束"互相牵制。
2. **销毁/保留的生命周期不同。** 合规库的保留策略与 legal hold 决定何时可以删除审计事实；业务库的清理是另一条线。
3. **权限边界不同。** 合规库只向审计与 purge 角色开放，且 purge 是**窄授权**（有独立角色、需批准信息与证书），业务角色不能直接删审计事实。

**实现约束（重要）：** **两套数据库必须分别运行各自的 Runner**，不能用一个 Runner 通吃。回滚语义也不同——合规库在已完成销毁（purge）后会**拒绝回滚**。

> 可选核对：`migrations/README.md`、`compliancemigrations/`、`cmd/trpc-service/schema_migrate_role.go`、`cmd/trpc-service/audit_relay_role.go`（同时持有 `db` 与 `complianceDB` 两个连接池）。

### 1.1 同一业务库里的两个表族（务必别混淆 `session_event` 与 `session_events`）

业务库里并存两族会话相关表，**键、写法、职责都不同**，这是最容易写错查询与写错文档的地方：

| 表族 | 表 | 键 | 谁写 | 职责 |
|---|---|---|---|---|
| **官方会话后端（SDK 表族）** | `session_states`、`session_events`（**复数**）、`session_track_events`、`session_summaries`、`app_states`、`user_states` | `(app_name, user_id, session_id)`，`app_name = tenantID + "/" + agentAppID` | `trpc-agent-go/session/postgres` 服务。表由平台迁移创建（服务侧 `WithSkipDBInit(true)`），写入为**同步**（`WithEnableAsyncPersist(false)`），每事件一行 | **对话历史（模型上下文）的唯一权威**。跨节点恢复上下文就是读它 |
| **平台协作表族** | `session_head`、`session_commit`、`session_event`（**单数**）、`execution_record`、`inbox`、`outbox`、`delivery_ledger`、`tool_attempt`、`tool_result_payload`、各 `*_payload` | `(tenant_id, agent_app_id, session_id)` / `(tenant_id, request_id)` | 平台 plpgsql 函数（`commit_turn` / `claim_inbox` / `prepare_dispatch` / `park_execution` …）与应用代码 | fence、输入序号、终态提交、幂等收件/发件、投递账本、工具级账本 |

**三条必须记住的结论：**

1. **上下文恢复 = 读 `session_events`。** 不要从平台 `session_event` 去找历史——那不是转写。
2. **平台 `session_event`（单数）在生产 durable 路径不会被写入。** 生产 worker 用的是 `NewDurableBufferedTurnScoped`，其 `Commit` 显式把 `Events`/`StateDelta`/`SummaryCandidate` 置 nil（注释原文：避免重建"第二个 Session 真相源"），因此 `commit_turn` 的 `p_events` 恒为 null，`session_event` 不产生新行、`last_session_seq` 不推进；`session_event_unpack_payload` 触发器只作用于这张平台表，`LoadSession`（读平台表）也没有生产调用方。该分支只服务测试与迁移路径。
3. **终态与顺序权威仍在平台表族**：`session_head`（fence / `next_input_seq` / version）、`session_commit`（每 `input_seq` 一行终态）、`inbox`（收件幂等）、`outbox`（待发事实）、`delivery_ledger`（投递一次）。恢复时的"做到哪一步"看这一族，"说了什么"看 SDK 族。

> 可选核对：`trpcservice/storage/session/buffered_turn.go`（`Commit` 置 nil 的注释）、`trpcservice/storage/session/postgres/official.go`（`WithSkipDBInit(true)` / `WithEnableAsyncPersist(false)`、`app_name` 编码）、`migrations/000001_service_schema.up.sql`（`session_states` / `session_events` 等 SDK 表由平台迁移创建）、`trpcservice/integration/runtime_slice_test.go`（对 `session_events` 的 durable 断言）。

---

## 2. Schema 基线与演进机制

### 2.1 基线是"压缩后的单一版本"，不是渐进历史

| 事实 | 含义 |
|---|---|
| `000001_service_schema` 是**唯一的首次交付压缩基线** | 它把核心表、索引、函数、触发器、权限、受控 purge 角色、迁移 pause/resume/abort 控制、租户 redaction rules、phase-intent journal 全部压进一个版本 |
| 基线**不含任何真实数据** | 没有真实租户、会话、消息、密钥或模型配置；数据由 bootstrap 与运行时写入 |
| Runner 只应用该基线，并在 `schema_migrations` 中记录唯一的 `000001` | 版本表是唯一权威的"当前 schema 版本" |
| 目标数据库为 **PostgreSQL 16** | 从空库启动 Compose 时的约定版本 |

### 2.2 演进规则（不得违反）

1. **不得改写或删除已发布基线。** Runner 用 `schema_migrations` 中的 **checksum 拒绝漂移**——有人手工改过基线文件，迁移就会在启动时不匹配而拒绝。
2. **新版本必须从 `000002_*.up.sql` / `000002_*.down.sql` 开始追加。**
3. **`down` 仅用于可丢弃的本地测试数据库**；合规库在已完成销毁后拒绝回滚。
4. 该基线与压缩前的历史数据库**不兼容**，不做原地升级。

### 2.3 本地验证与重置入口

```bash
# 验证持久化会话、租约 fence、Outbox 与投递台账
go test ./migrations ./trpcservice/gateway/postgres ./trpcservice/worker

# 重置数据库卷（必须使用该显式命令，不要手工删卷）
docker compose -f deploy/compose/docker-compose.local.yml down -v
```

### 2.4 与应用发布的关系（两阶段）

schema 演进与应用发布必须解耦，顺序固定：

```text
① 先发布「能识别新旧 schema」的 consumer（preprocess / worker / delivery / relay）
② 再执行扩展迁移（新增列/表，向后兼容）
③ 观察稳定后，才启用依赖新字段/新状态的逻辑
④ 清理旧结构放到更后的版本
```

**为什么：** 旧 consumer 遇到未知 envelope 字段必须能安全忽略、遇到未知 delivery 状态必须能安全遍历。若先迁移再发布，老节点会因不认识新状态而把 `ambiguous` 误判为 `failed`，从而放弃本可收敛的投递。

---

## 3. 事务边界与隔离级别（此前完全没写）

### 3.1 隔离级别：使用 PostgreSQL 默认的 READ COMMITTED，靠**显式行锁**保证正确性

实现中所有显式事务都是 `BeginTx(ctx, nil)`（可选核对：`trpcservice/storage/messaging/postgres/store.go`、`trpcservice/storage/artifact/postgres/lifecycle.go`、`trpcservice/storage/summary/postgres/store.go`）。在 PostgreSQL 中这意味着 **READ COMMITTED**——不使用 SERIALIZABLE，也不使用 REPEATABLE READ。

**这是刻意的设计选择，不是疏漏。** 因为本系统的正确性需求是"**特定行的互斥与特定条件下的一次性提交**"，而不是"整个事务的快照一致性"，所以用**行级显式锁 + 条件更新**表达即可，且失败模式更可控：

| 需求 | 用 SERIALIZABLE 会怎样 | 本系统实际做法 |
|---|---|---|
| 同一会话同一时刻只有一个提交者 | 需要为读-改-写加 `SERIALIZABLE`，并在序列化失败时重试——重试语义要自己实现，且高竞争下重试风暴明显 | **显式行锁**：`commit_turn` 内 `SELECT ... FROM session_head ... FOR UPDATE` |
| 同一 delivery segment 只能被一个 owner 发送 | 同上 | **条件更新**：`WHERE version=? AND state='sending' AND claim_owner=?` |
| 多个 relay 并发领同一批 outbox 不互相阻塞 | SERIALIZABLE 下冲突会直接报序列化错误 | **`FOR UPDATE SKIP LOCKED`**：跳过已被他人持有的行 |
| 旧的迟到写不得覆盖新结果 | 需要额外的版本列比较 | **`expected_version` + `fence` 双列比较**，显式拒绝 |

**结论：** 正确性由"**行锁 + 条件更新 + 版本/栅栏列比较**"承担；隔离级别保持默认，从而避免 SERIALIZABLE 带来的重试风暴与不可预测的延迟。**若有人把某个查询改成无锁读-改-写，正确性不会被隔离级别兜住** —— 这一点必须在代码评审中守住。

### 3.2 每个"必须一次"的动作都有数据库层兜底

| 动作 | 数据库层保证 |
|---|---|
| 一条 provider 消息只产生一个 durable 事实 | `inbox` 主键 `(tenant_id, channel, external_account_id, external_message_id)` + `ON CONFLICT DO NOTHING` |
| 一个 `input_seq` 只分配一次 | `session_head.last_allocated_input_seq` 在 `FOR UPDATE` 下递增 |
| 一个 `input_seq` 只有一条终态 | 部分唯一索引 `session_commit_terminal_input_idx (tenant_id, agent_app_id, session_id, input_seq) WHERE outcome IN (终态集合)` |
| 一次请求只提交一次 | `commit_turn` 的 `commit_id` + `request_digest` 幂等判重 |
| 一个 outbox 事件只发布一次语义 | 唯一约束 `(tenant_id, kind, idempotency_key)` |
| 一个 delivery segment 只被一个 owner 发送 | 主键 `(tenant_id, delivery_key, segment_no)` + claim 条件更新 |

> 逐字 DDL 与函数体见：`conversation-continuity/REFERENCE-IMPLEMENTATION.md` §2–§3、`inflight-task-takeover/REFERENCE-IMPLEMENTATION.md` §1–§2、§6。

### 3.3 行锁使用清单

| 位置 | 锁 | 为什么必须 |
|---|---|---|
| `commit_turn` | `session_head ... FOR UPDATE` | 序列化同一会话的并发提交（version/next_input_seq 的读-改-写） |
| `commit_turn` | `execution_record ... FOR UPDATE` | 防止同一请求被并发提交 |
| `prepare_dispatch` | `tenant` / `inbox` / `session_head` / `agent_app` 均 `FOR UPDATE` | 版本校验与序号分配必须在同一锁序内完成 |
| `park_execution` | `session_head` 先锁、`execution_record` 再锁 | 停放判定依赖 `next_input_seq`，必须先固定它 |
| `claim_inbox` | `inbox ... FOR UPDATE` | 重传并发时读回同一行并做摘要比对 |
| outbox claim | `FOR UPDATE SKIP LOCKED` | 多个 relay 并发领同一 kind 时不互相阻塞，且每条只被一人领走 |
| Ledger claim | 条件 `UPDATE`（无显式 `FOR UPDATE`） | 单行 UPDATE 本身即原子；用 `state/version/owner/client_request_id` 四重条件表达"只有我能改" |

**锁序纪律（避免死锁）：** 涉及多表的事务遵循固定顺序——**控制面（`tenant` / `agent_app`）→ `inbox` → `session_head` → `execution_record` → `outbox`**。新增事务必须沿用同一顺序，否则两个并发事务交叉持锁会死锁。

---

## 4. 消息正文在库中是**加密存储**的（此前只出现列名，没有解释）

`inbound_payload` / `prepared_payload` / `result_payload` 三张表存的不是明文，而是 `*_ciphertext bytea` + `*_nonce bytea` + `key_version bigint`。其实现语义如下：

### 4.1 算法与绑定

| 项 | 实现 |
|---|---|
| 算法 | **AES-256-GCM**（`aes.NewCipher(32 字节密钥)` + `cipher.NewGCM`） |
| nonce | 每次写入用 `crypto/rand` 随机生成，长度取 GCM nonce size |
| **AAD（附加认证数据）** | `tenantID + "\x00" + requestID + "\x00" + payloadRef + "\x00" + contentDigest` |
| 解密失败 | `aead.Open` 失败 → `ErrVersionMismatch`（**不是**返回密文或空值） |

### 4.2 为什么 AAD 里必须带 `tenantID` 与 `requestID`

这是**第二道租户隔离**，与 SQL 的 `WHERE tenant_id=...` 相互独立：

```text
假设某个查询漏写了 tenant 条件，把 Tenant B 的密文行读出来了
  → 但解密时的 AAD 用的是"本次请求的 tenantID + requestID"
  → GCM 校验失败 → aead.Open 返回错误 → ErrVersionMismatch
  → 攻击者/缺陷代码拿不到任何明文
```

也就是说：**即使应用层租户过滤写漏了，密文本身也解不开**。同理，把一行密文搬到另一个 `request_id` 下、或改掉 `content_digest` 后再放回去，都会因 AAD 不匹配而解密失败——密文与"租户 + 请求 + 载荷引用 + 内容摘要"四者绑定。

### 4.3 密钥来源与轮换

| 项 | 实现 |
|---|---|
| 密钥解析 | `messaging.PayloadKeyResolver` 接口；生产实现为 `trpcservice/secrets/payloadkey/` |
| 作用域 | `Scope{TenantID: tenantID, Subject: tenantID, Purpose: PurposePayloadEncrypt, ResourceID: "messaging-payload", ResourceVersion: version}` —— **按租户 + 版本取密钥** |
| 密钥长度 | 必须恰好 **32 字节**，否则 `ErrCapabilityUnsupported` |
| 版本一致性 | 解析出的密钥版本必须等于请求版本，否则 `ErrVersionMismatch` |
| 失败清理 | 解析失败或校验失败时**清零密钥字节**（`clear(value.Bytes)`） |

**轮换流程（由 `key_version` 列支撑）：**

```text
写入：用当前版本的密钥加密，并把 key_version 一并写入行
读取：按该行的 key_version 解析对应密钥再解密
轮换：发布新版本密钥 → 新写入使用新版本；旧行仍按其 key_version 可解
      → 因此轮换不需要停机、不需要批量重写历史行
```

**运维含义：** 删除旧版本密钥前，必须确认**不再有引用该版本的未迁移数据**，否则对应历史消息将永久无法解密。这是数据层面的不可逆操作。

> 可选核对：`trpcservice/storage/messaging/postgres/store.go`（`payloadAAD` / `encryptPayload` / `decryptPayload`）、`trpcservice/secrets/payloadkey/resolver.go`。

---

## 5. 关键 SQL 函数的两个安全约定

### 5.1 `SECURITY DEFINER`

所有关键的 plpgsql 函数（`claim_inbox` / `claim_channel_inbox` / `prepare_dispatch` / `commit_turn` / `park_execution` / 各类 `guard_*` / `reject_*` / purge 路径等）都声明为 `SECURITY DEFINER`。

含义：函数**以其属主的权限执行**，而不是调用者。这样做的目的是：

- 应用角色只需要 `EXECUTE` 权限，不需要对底层表持有宽泛的 `INSERT/UPDATE/DELETE` 权限；
- 所有写路径被收敛到少数几个函数里，**校验逻辑无法被绕过**（例如想直接 `UPDATE session_head` 提版本号是做不到的）。

### 5.2 `SET search_path TO 'pg_catalog'`

同上这批函数还固定了 `search_path`。**这是防 search_path 劫持的必要措施**：若 `search_path` 可控，攻击者可在其中提前创建一个同名对象（或函数），使 `SECURITY DEFINER` 函数在解析名字时调用到攻击者的版本，从而以更高权限执行其代码。显式固定到 `pg_catalog` 后，函数体内的名字解析被钉死，消除了这条提权路径。

> 复现自己的实现时若省略这两项，功能可能一样能跑，但**权限模型会退化**：应用角色不得不拿到直接写表权限，且 `SECURITY DEFINER` 反而会成为提权面。

---

## 6. 触发器：把一部分不变量下沉到数据库

触发器不是"顺带写的"，它们承担明确职责。与可靠性链路直接相关的是下面几个：

| 触发器 | 表 / 时机 | 承担的不变量 |
|---|---|---|
| `execution_requires_preprocess` | `execution_record` BEFORE INSERT | **禁止出现"未经预处理的执行记录"**——保证 F1→F2 的顺序不可跳过 |
| `execution_record_hydrate_execution_budget` | `execution_record` BEFORE INSERT | 插入时注入预算相关字段，避免调用方各自拼装 |
| `execution_cancel_intent_guard` | `execution_record` BEFORE UPDATE OF outcome | 取消意图与 `outcome` 变更的一致性 |
| `outbox_idempotency_guard` | `outbox` BEFORE INSERT | 幂等键保护（与唯一约束互为表里） |
| **`session_event_unpack_payload`** | `session_event` BEFORE INSERT | **把写入的 JSON 包装拆成 `payload_ref` 与 `event_payload`** |
| **`session_head_wakeup_next_parked`** | `session_head` AFTER UPDATE OF `next_input_seq` | **序号推进后自动为被停放的输入入队 wakeup** |
| `session_commit_capture_migration` | `session_commit` AFTER INSERT | 迁移期变更捕获 |
| `tenant_maintain_update_trg` | `tenant` BEFORE UPDATE | 租户版本维护 |

### 6.1 为什么 `session_event` 的写入是一个"包装对象"

应用侧写入 `session_event` 时，`payload_ref` 位置放的其实是一个 JSON 包装：

```json
{ "ref": "<真实载荷引用>", "payload": { ... 规范化事件表示 ... } }
```

BEFORE INSERT 触发器负责**校验并拆包**：`ref` 必须非空、`payload` 必须是 JSON 对象（`jsonb_typeof(...) = 'object'`），否则 `RAISE ... ERRCODE='23514'`；校验通过后写回两列——`payload_ref` 存真实引用字符串，`event_payload` 存事件 JSON。

**为什么这样设计：** 让"事件引用"与"事件内容"在同一行内**原子地被校验**，同时保持 `payload_ref` 列语义纯净（只存引用）。另一个 Worker 接管时只需按 `session_seq` 顺序读 `event_payload` 即可重建上下文——这正是"上下文可跨节点恢复"的物理基础。

> 应用侧对应代码见 `conversation-continuity/CODE-APPENDIX.md` §12 的 `CommitTurn`（构造 `{"ref":..., "payload":...}` 包装）。

### 6.2 `wakeup` 事件从哪来（此前未解释）

文档里存在 `wakeup` 这种 outbox kind，也存在 `wakeup-relay` / `wakeup` 两个 owner，但**此前没有任何地方说明 wakeup 事件由谁产生**。答案是数据库触发器：

```text
Worker 提交一轮后 session_head.next_input_seq 被推进
  → AFTER UPDATE OF next_input_seq 触发器 enqueue_next_parked_wakeup()
  → 自动为该会话下一个「被 park 停放」的输入入队 wakeup 事件
  → wakeup-relay 发布 → wakeup 消费者唤醒它重新进入执行
```

这就是"前序输入未终态时被停放（`park_execution`）、前序完成后被自动唤醒"这一闭环的最后一环：**停放由函数做，唤醒由触发器做**，两者都不需要人工介入。

---

## 7. 索引：不只列出来，还说明服务哪个查询

> 完整索引清单（逐字）见各包 `REFERENCE-IMPLEMENTATION.md` 的 DDL 章节。这里只列**可靠性链路的关键索引**及其服务的查询。

| 索引 | 服务的查询 | 为什么关键 |
|---|---|---|
| `session_commit_terminal_input_idx`（**部分唯一**）`(tenant,app,session,input_seq) WHERE outcome IN (终态)` | `commit_turn` 判定"该输入是否已有终态" | 既是**查询索引**，又是**唯一性约束**——它同时保证"一个输入只有一条终态" |
| `outbox_claim_idx` `(kind, state, next_attempt_at, created_at)` | relay 领取：`WHERE kind=? AND ((state IN ('pending','retry_wait') AND next_attempt_at<=now()) OR (state='claimed' AND claim_until<now())) ORDER BY next_attempt_at, created_at` | 与领取条件**逐列对应**，使领取走索引而不是全表扫 |
| `delivery_ledger_claim_expiry_idx` `(claim_until) WHERE state='sending'` | 对账与超期 claim 回收 | 部分索引：只索引在途的 `sending`，体积小 |
| `delivery_ledger_retry_idx` `(state, not_before, updated_at) WHERE state IN ('pending','retry_wait','ambiguous')` | 待重试/待对账扫描 | 同上，只索引需要被扫描的状态 |
| `execution_park_ready_idx` `(tenant,app,session,input_seq,not_before) WHERE outcome='pending'` | `park_execution` 找可继续的停放输入 | 与停放判定条件对齐 |
| `preprocess_job_claim_idx` / `preprocess_job_ready_idx` | preprocess 领取待办、找 `ready` 且未 dispatch 的任务 | `ready_idx` 是 P1 接管的发现入口 |
| `inbox` 主键 / `(tenant_id, request_id)` 唯一 | 去重与按请求反查 | 入站幂等的物理基础 |

**设计纪律：** 这些索引是**部分索引（partial index）**——只为"当前需要被扫的状态"建索引，而不是为整张表建。复现时若改成普通索引，功能不变但索引膨胀更快，且"只增表"的写放大更严重。

---

## 8. 数据生命周期：哪些表只增、谁负责清理（**重要运维盲区**）

### 8.1 事实：可靠性主链路的表**没有自动清理路径**

经核对，迁移与合规迁移中唯一的受控删除位于**审计**路径（`execute_business_audit_purge`），且它只删 `kind='audit'` 且 `state='published'` 且早于安全水位的 outbox 行，并写入 `business_audit_purge_certificate`（含 `event_count` / `outbox_count` / `event_digest` / `approved_by` / `reason`）。

因此下面这些表在长期运行中是**只增**的：

| 表 | 增长驱动 |
|---|---|
| `inbox` | 每条入站消息一行 |
| `execution_record` | 每条入站消息一行 |
| `session_commit` / `session_event` | 每轮对话一行 / 每轮多个事件行（注：生产路径 `session_event` 无新行，见 §1.1） |
| `session_events`（SDK 转写） | 每个被持久化的事件一行；**上下文恢复的唯一来源，长会话下是主要增长项** |
| `tool_attempt` / `tool_result_payload` | 每个 `ask` 工具调用各一行（含结果密文） |
| `outbox`（`dispatch` / `reply` / `wakeup` 类） | 每个待发事件一行（发布后仍保留 `published` 行） |
| `delivery_ledger` | 每条回复片段一行 |
| `*_payload` 三张表 | 每条消息/结果一行（**密文，体积随内容增长**） |
| `webui_message` | 邮箱视图行 |

> 🔴 **这是必须写进运维手册的事实：** 文档化范围内**没有**针对上述表的清理/归档机制。长期运行必须由运维侧另行决定策略（按时间分区 + 分区 detach、或按保留窗口归档后删除），并且**必须在确认无待处理事实**（无未终态 execution、无 pending/claimed/retry_wait outbox、无 sending/ambiguous ledger）之后再删，否则会破坏在途接管证据。

### 8.2 有明确保留语义的部分

| 对象 | 保留语义 |
|---|---|
| 审计事实（合规库） | 由保留策略 + legal hold 决定；purge 需批准并留证书；合规库已完成销毁后拒绝回滚 |
| 业务审计 purge 批次 | `business_audit_purge_batch` / `business_audit_purge_certificate` 记录批次与证书，`quarantine_*` 支持隔离处置 |
| Artifact（产物） | 由 `artifact_reference.retain_until` 与 `artifact_object_upload.protect_until` 驱动，有独立生命周期与保留扫描 |
| Session 摘要内容 | `session_summary_content` 有 `superseded` / `delete_claimed` 状态与清理索引 |

### 8.3 建议（供运维决策，非实现承诺）

| 表 | 建议策略 |
|---|---|
| `inbox` / `execution_record` | 按 `created_at` 月度分区；确认该月所有 `inbox.state='terminal'` 且 execution 终态后 detach |
| `outbox` | published 且早于保留窗口的可归档；但**保留窗口不得短于最长重试/对账周期** |
| `delivery_ledger` | 仅 `sent` / `failed` 且早于窗口的可归档；**`ambiguous` 严禁清理**（需人工对账） |
| `session_event` / `session_commit` | 与业务会话保留期一致；删除会直接导致上下文不可恢复，需业务确认 |
| `*_payload` | 密文删除前确认无引用；同时注意删除后对应 `key_version` 才可能被安全淘汰 |

---

## 9. 连接池：本平台节点**未显式设置**参数（实现事实，与直觉相反）

这一点与"上面那些角色都设了 16"的印象不同，必须分开说：

| 角色 | 连接池设置 | 说明 |
|---|---|---|
| **`webui-local` / `wecom-local`（本平台应用节点）** | ❌ **未设置** | 仅 `sql.Open("pgx", DSN)`，随后直接使用。即使用 `database/sql` 的**零值默认**：`MaxIdleConns` = 2、**`MaxOpenConns` = 0（无上限）**、`ConnMaxLifetime` = 0（**连接不过期**） |
| `worker` / `gateway` / `channel` / `channel-delivery` / `preprocess` / `admin` / `audit-relay` / `business-audit-purge` / `schema-migrate` | ✅ 显式设置 | 各自 `SetConnMaxLifetime(30 * time.Minute)`、`SetMaxIdleConns(4)`、`SetMaxOpenConns(16)`；审计类角色对业务库与合规库**各设一套** |
| `main.go` 的 `demo` 路径 | ⚠️ 另一组值 | `SetMaxIdleConns(8)`、`SetMaxOpenConns(32)`——属 demo 路径，不是角色分发路径 |

### 9.1 这对可靠性意味着什么（务必写进运维认知）

1. **`MaxOpenConns = 0` 意味着无上限。** 存活节点在接管期间会承接两个实例的全部工作（含高频的 lease 续租小事务），而它打开的连接数不会自我限制——**可能打到数据库的 `max_connections` 上限**，届时失败形态是"连不上库"，而不是优雅降级。
2. **`ConnMaxLifetime = 0` 意味着连接永不主动回收。** 受害节点被 SIGKILL 后，它持有的连接要等 TCP/内核超时或数据库侧 idle 超时才会释放；在此之前它们仍占用数据库连接额度。**故障时刻实际可用连接数低于静态规划值**，这是接管期的隐性容量缺口。
3. **池耗尽在本系统里不会先报连接错误。** 因为续租是高频小事务，先出现的信号是**续租延迟 → `lease_lost` 升高 → 任务被他人接管**。排障时若只看"有没有连接错误"会漏掉根因。
4. **调整需要改代码。** 目前这些参数不是环境变量；若要在部署侧按环境调整，必须先将其改为可配置，否则容量问题只能靠"减少角色/减少节点"来缓解。

### 9.2 容量测算的正确姿势

```text
业务库连接额度需求 ≈ Σ(各角色 MaxOpenConns × 节点数)  +  DBA/监控/迁移等预留
```

由于本平台节点是"无上限"，测算时**不能**代入一个具体数字，而应：

- 先给本平台节点**补上显式上限**（例如按角色设定并使其可配置），再进行测算；
- 或在上层用连接池代理（PgBouncer 等）对该库的连接总量硬限流。**但注意：`FOR UPDATE` / `FOR UPDATE SKIP LOCKED` 依赖长事务语义在事务级连接上，不能使用 transaction-pooling 之外的模式**（statement pooling 会破坏会话状态与锁语义）。

> 可选核对：`cmd/trpc-service/webui_local_role.go`（两处 `sql.Open`，均在 175 / 544 附近，其后无池参数设置）、`cmd/trpc-service/worker_role.go`、`gateway_role.go`、`channel_role.go`、`channel_delivery_role.go`、`preprocess_role.go`、`admin_role.go`、`audit_relay_role.go`、`business_audit_purge_role.go`、`schema_migrate_role.go`。

---

## 10. 备份、恢复与故障域的诚实边界

| 项 | 现状与边界 |
|---|---|
| 业务库 | 是**业务权威事实**的唯一来源。它的丢失等于会话、结果、投递记录全部丢失；备份策略由部署方负责 |
| 合规库 | 与业务库**分别**备份、分别 Runner；保留与 legal hold 语义在合规库侧 |
| 本平台承诺 | 仅覆盖**单个应用实例故障**（见各包边界章节）；**不承诺** PostgreSQL 自身故障、主从切换、磁盘损坏或机房级灾难下的可用性 |
| 恢复语义 | 若从备份恢复业务库，必须注意：`session_head.last_fence` 会**回退**到备份时刻的值。虽然 Worker 每次取租约前都会 `EnsureFenceAtLeast` 校准（因此不会出现"新 lease 拿到更小 fence"），但**备份点之后已提交的结果会丢失**，且下游可能已收到回复——恢复后必须以 `delivery_ledger` 与下游对账，不能假定"回滚即一致" |
| 明确不做 | 本包不提供 PITR 配置、备份脚本、跨机房复制方案；这些属部署与 DBA 职责 |

---

## 11. 复现等价系统时的数据库核对清单

- [ ] 两个库（业务 + 合规）分别建库、分别 Runner，权限角色分离。
- [ ] `schema_migrations` 记录版本并在后续演进中拒绝 checksum 漂移。
- [ ] 关键写路径收敛到 `SECURITY DEFINER` + `SET search_path TO 'pg_catalog'` 的函数里，应用角色不持表写权限。
- [ ] 事务用默认隔离级别 + 显式行锁；多表事务遵循 控制面 → inbox → session_head → execution_record → outbox 的锁序。
- [ ] 每个"必须一次"的动作都有数据库层兜底（唯一约束 / 部分唯一索引 / 条件更新）。
- [ ] payload 表存密文 + nonce + `key_version`；AAD 绑定 `tenant + request + payload_ref + content_digest`；密钥按租户 + 版本解析且长度校验为 32 字节。
- [ ] 可靠性链路的触发器齐备（尤其 `session_event_unpack_payload` 与 `session_head_wakeup_next_parked`）。
- [ ] 关键索引为**部分索引**且与领取/扫描条件逐列对应。
- [ ] 明确记录"只增表"清单与运维清理策略，并写清"清理前必须确认无在途事实"。
- [ ] 连接池参数与容量测算对齐（并记录"当前需改代码才能调"这一事实）。
- [ ] 备份/恢复不属于本平台承诺范围，但恢复后必须与下游对账。

---

## 12. 相关文档

| 想了解 | 读 |
|---|---|
| 本子系统表的逐字 DDL 与 SQL 函数语义 | 各包的 `REFERENCE-IMPLEMENTATION.md` |
| 事务在业务上"保证了什么" | `conversation-continuity/FULL-GUIDE.md` §14（原子提交）、`inflight-task-takeover/FULL-GUIDE.md` §2.1（可重试性判定） |
| 状态机与状态取值 | 各包 `design.md` 的状态机章节 |
| 发布顺序与回滚约束 | 各包 `release-and-operations.md` |
| 跨包运行时契约（部署单元、配置、Redis 契约） | `REFERENCE-RUNTIME-SPEC.md` |
