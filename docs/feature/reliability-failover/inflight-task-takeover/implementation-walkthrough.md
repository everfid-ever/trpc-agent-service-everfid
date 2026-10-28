# 在途任务接管与幂等投递 — 实现走读

## 从 durable truth 选择入口，而不是固定重跑整条链

以下为对实际分支的阅读提纲，不是可以替代仓库实现的伪代码接口：

```text
以原 envelope 进入 Worker，并取得有效会话资格
  → 加载权威输入/版本并核对作用域
  → 已终结：读取既有结果，不重演本轮
  → 后序输入：交给持久停放协议
  → 有冻结答案：校验、重查意图、继续提交
  → 无冻结答案：SDK 运行，工具逐个按账本与策略执行
  → 冻结最终结果，再提交 terminal + Outbox
  → Relay 发布原 reply，Delivery 按原分段计划发送/核对
```

起点仍是原始输入；“从更后面继续”指复用持久事实，并不允许任意跳过原权限、取消和版本校验。

## 每类错误在哪一层结束本次尝试

| 错误或处置 | 本次局部处理 | 后续责任 |
|---|---|---|
| 获取会话 lease 失败 | 不进入执行器。 | 队列未确认工作按调度/回收继续。 |
| BeginExecutionAttempt 失败 | 不调用执行器。 | 保留原业务身份，检查数据库或 stale fence。 |
| 执行器返回 ErrInputNotReady | ParkInput，按最新 disposition 再执行、停放或标识阻塞。 | durable 协调状态；不能仅用短暂内存 timer 代替。 |
| 执行器正常返回 | 尝试日志 completed，随后检查取消/失权并 ACK。 | 正常返回可能含 durable 等待，业务完成另查 terminal。 |
| 工具 Complete 失败 | 不把成功文本当作可复用成功结果。 | 按账本、业务键和策略重新判定原效果。 |
| 发布成功、MarkPublished 失败 | 保留可重复发布原事件的窗口。 | 下游稳定键、终态与 Ledger 去重。 |
| 分段 sending 资格到期 | 原发送结果不能推断未发生。 | ambiguous 核对，而非直接重新领取发送。 |
| 核对耗尽 | 保存 failed/reconcile_exhausted，可结束自动消费。 | 运维事件与证据化 resolve；不是用户已收到。 |

## 检查取消与崩溃的差别

取消是有业务意图的状态转移，崩溃是当前执行者失去运行能力。Worker 的取消观察会取消执行 context，并尝试以有效 lease 提交取消；崩溃后则由其他 owner 读取持久状态决定动作。冻结成功答案存在时也要重查当前意图。

两者都不承诺撤销已在远端完成的副作用。工具若有独立补偿接口，补偿是新的受控业务动作，需要自己的权限、幂等键与记录，不能把重新运行原工具函数误称为回滚。当前通用接管协议没有替所有工具实现补偿事务。

## 日志关联应保留原身份

一次接管前后 request、session/input_seq、工具 operation 和 reply/delivery key 保持稳定，owner、fence、attempt 允许变化。traceparent 可帮助串联，但业务恢复不从 trace 文本推断状态。新增日志字段时优先记录稳定引用与错误类别，避免输出密钥、完整工具参数或用户正文。

## 按运行时顺序走读

| 顺序 | 模块 / 函数 | 本步权威状态 | 此刻被杀会怎样 |
|---|---|---|---|
| 1 | loadWebUILocalConfig、bootstrapWebUILocal | 版本化配置、Bot route、租户密钥、已发布技能 | 未 ready；peer 使用既有共享配置，不接管内存 bootstrap 栈。 |
| 2 | identity.Mapper.Map、渠道验签入口 | verified Binding；tenant scoped user/session | durable 接收前不能宣称已纳入恢复保证。 |
| 3 | Inbox/payload 与预处理入库 | Inbox 唯一身份、加密原输入、preprocess_job、ReplyRoute | 共享扫描领取未完成 job，用户不重发。 |
| 4 | prepare_dispatch 与 Dispatch Relay | 原 input_seq、execution_record、dispatch Outbox | 重复 dispatch 复用序号；发布后未标记可重复 event。 |
| 5 | worker.Consumer.handle | Redis lease/fence；BeginExecutionAttempt | 失权后 pending reclaim；新尝试 supersede 遗留 running。 |
| 6 | RunnerExecutor.ExecuteWithLease | session_head / 官方 SDK 历史 / 输入与版本 | 已 terminal 识别完成；输入未就绪则停放/唤醒，不强行越序。 |
| 7 | frozen result 检查 | tenant/request/key/digest 验证通过的 result_payload | 已冻结时续提交，不再次运行模型。 |
| 8 | run.Run 与 GuardedCallable | SDK session_events、工具账本/密文 | SDK 事件可能已写；平台 rollback 不撤销；工具按恢复契约。 |
| 9 | PutResult → commitFrozenSuccess | 冻结答案；beforeCommit；terminal/Outbox 事务 | result 已写未提交时复用；事务提交后不重跑模型。 |
| 10 | Reply Relay.PublishReply | reply Outbox 与账号 reply stream | 先发布后标记，可重复发布同一 DeliveryKey。 |
| 11 | delivery.Service.Deliver | 分段计划与 Ledger claim | 只有成功 claim 才发送；expired sending 进入不确定路径。 |
| 12 | adapter.Deliver → FinishDelivery | provider 接受消息 ID、Ledger sent | 接受后确认前为 P4；不得因超时判断未发送。 |
| 13 | Broker.Ack / AckReply | terminal/停放等 durable disposition；发送终态 | 未完成执行失败留 pending；发送明确失败可被 ACK，靠异常记录处置。 |

注意 ACK 不全等价于“用户答案完成”：Worker 对已持久停放/阻塞等 dispositions 可正常返回后 ACK，Wakeup/协调记录负责后续推进；Delivery 的 TerminalError 也允许在 failed 持久化后 ACK。审查时要检查 durable 后续责任，不能只查 ACK。


## 工具恢复的执行顺序

1. `GuardedCallable.callDurable` 校验租户执行上下文、payload key version、规范化参数及工具版本。普通 allow 的 operation ID 是 tenant/request/tool/version/参数摘要的 SHA-256；同一输入的同工具同参数视为同一逻辑操作。需要不同效果时必须带不同业务参数/键。ask 使用确认中的持久化调用 ID。
2. 读取 tool_execution。已 succeeded 且不可变字段一致时，LoadResult 解密并校验摘要，直接复用；failed 拒绝继续。未知新工具默认 manual。
3. Claim 首次创建 running，或领取 pending/effect_unknown/过期 running；attempt 和工具 fence 增加。未过期 running 不可抢占。
4. 启动工具续租。ask 消费 grant；遇到已消费冲突时仅允许恢复同 request/tool-call 且 effect_unknown 的 attempt，不再次消费授权。
5. 恢复时 queryable 先查询；completed=false 必须证明外部操作未被接受，pending/unknown 必须返回错误。idempotent_key 同键调用；replay_safe 重放；manual 再次尝试收敛为 effect_unknown，任务可终结为 tool_effect_unknown。
6. 外部调用错误可将 tool_execution 标为 effect_unknown；lease 丢失立即取消 call context。外部系统若不响应取消，仍由其幂等/查询协议避免重复业务效果。
7. 成功后停止续租，Complete 在同一 PostgreSQL 事务中验证 owner/fence/未过期租期、把执行置 succeeded 并写入 AES-GCM 加密的 tool_execution_result。任何一步失败均回滚。
8. ask 再把结果镜像至 tool_result_payload 并完成 tool_attempt；镜像前故障可从 tool_execution_result 重建。业务效果与平台结果是两个事务；local_note 用 (tenant_id,business_key) 唯一键覆盖这段窗口。

恢复的是已保存结果或外部操作的协议状态，不会恢复工具函数体的内存/指令位置。未冻结最终结果时模型可能重新推理；相同逻辑操作重新进入 GuardedCallable 时复用或安全恢复。任意 Graph 检查点也不能据此推断为自动恢复整张图的执行栈。

## 冻结答案与 terminal 提交

Runner 先加载并校验原输入。若 Results.GetResult 找到同 tenant/request 的冻结结果，要求 result_ref 非空、key version 与 payload 一致、内容摘要匹配，随后进入 commitFrozenSuccess。没有结果才执行模型。生成结束先 PutResult，再在 result-before-commit 暂停点后提交。commitFrozenSuccess 重查取消意图、beforeCommit 与 terminal 条件，原子写 terminal 和 reply Outbox。

SDK 事件已同步持久，续提交不会再运行 SDK。result_payload 独立于 terminal 事务，所以这是专门的恢复分支，不能把它误写成“提交前一律重跑”。

## Delivery 的精确分支

| 条件 | 动作 |
|---|---|
| Ledger sent | 跳过 adapter，继续剩余段；队列最终可 ACK。 |
| 正在被其他有效 owner 发送 / not_before 未到 | DeferredError，保持队列 pending。 |
| 当前 claim 有效且允许发送 | adapter.Deliver，并续期 claim。 |
| delivered 且 ProviderMessageID 非空 | FinishDelivery(sent)，保持原 client_request_id。 |
| ambiguous 错误 / 缺少接受 ID | 记录 ambiguous；不按一般 retry 盲目发送。 |
| 明确可重试错误 | retry_wait；最多次数后 failed。 |
| ambiguous 且 adapter 实现 DeliveryReconciler | delivered→sent；确定未送达→retry_wait；unknown→延迟核对。 |
| 无核对能力/持续失败 | 延迟核对至上限；failed/reconcile_exhausted，保留异常待外部证据处置。 |

| 测试路径名 | 实际边界 | 证据 / 使用限制 |
|---|---|---|
| p1 | 预处理持久领取后、执行前 | 原输入/job 已持久化；不能代替所有 P1 子窗口。 |
| p2 | 模型成功响应头返回后、SDK 消费结果前 | 请求尝试与成功头状态；不证明 provider 内部仍在生成。 |
| p2-commit | 模型/工具完成、terminal 结果提交前 | 旧边界，不能代替执行中的 P2。 |
| result-before-commit | 冻结结果保存后、terminal/Outbox 提交前 | 原 result_ref/digest 不变，接管不再调用模型。 |
| p3 | terminal 已提交、reply PublishReply 前 | session_commit、result_payload、已领取 reply Outbox。 |
| p4-before-send | Ledger claim 后、provider 调用前 | 旧发送前边界，不能证明 P4 已接受。 |
| p4 | provider 成功接受且返回消息 ID 后、Ledger sent 前 | barrier 快照 Evidence；进程内快照需在 kill 前采集，非 durable receipt。 |
| tool-effect | 工具业务效果完成后、工具结果提交前 | 按业务键查外部效果，恢复不能增加效果数。 |

仅 `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true` 注册 `/test/failover/<point>/arm|status|release`，用本地 WebUI token 鉴权。arm 可带 tenant_id/request_id 过滤；status 回显命中身份/evidence。暂停控制面是进程内测试设施，不续 lease、不修改业务状态、不替系统重投。标准接管在命中后杀 owner，不对 survivor release。

## 关键模块的真实入口

| 模块 | 当前代码 |
|---|---|
| 运行时装配、Bot 配置、故障端点 | cmd/trpc-service/webui_local_role.go |
| 就绪与排空 | cmd/trpc-service/runtime_health.go、runtime_drain.go |
| 渠道身份映射 | trpcservice/channels/identity/identity.go |
| Worker 执行权和 ACK | trpcservice/worker/consumer.go |
| 原结果恢复与提交 | trpcservice/worker/runner.go |
| SDK 作用域与同步历史 | trpcservice/storage/session/postgres/official.go、storage/session/buffered_turn.go |
| 工具恢复 | trpcservice/tool/durable.go、tool/execution/postgres/ |
| 发送、claim 续期与核对 | trpcservice/channels/delivery/service.go |
| 发送消费与监督器 | trpcservice/channels/delivery/consumer.go、supervisor.go |
| 冻结结果/Outbox/Ledger/处置 | trpcservice/storage/messaging/postgres/ |
| 稳定 callback 入口 | cmd/trpc-service/wecom_ha_entry_role.go |

## Worker入口的逐行职责

Consumer.handle从delivery.Envelope取tenant/app/session建立SessionKey，读PG last_fence并EnsureFenceAtLeast。acquire成功后才启动执行，defer使用独立短时context释放当前lease，释放也必须匹配lease身份。

续租goroutine和取消观察goroutine与实际执行并行。leaseLost通过一次性标记取消executionCtx；beforeCommit先检查失权再Renew。模型响应晚到不自动取得提交资格。

每次进入ExecuteWithLease前BeginExecutionAttempt；执行后用独立短时context结束attempt日志。若结束日志失败，错误不能被当成业务成功吞掉，原delivery仍需要恢复判定。

## 停放、阻塞与Wakeup的具体顺序

当输入序号暂不可执行时，Worker调用ParkInput。其返回的ready、parked、terminal、blocked是不同处置结果：有的应立即再试，有的已有持久后续责任，有的是明确阻塞需观测。

WakeupActivator先InspectWakeup，要求执行仍pending且ready，按同tenant/app/session计算shard，再Publish原envelope，最后MarkWoken。Publish成功而MarkWoken前崩溃可能重复激活事件；原请求身份和执行协议必须吸收重投。

如果MarkWoken发生版本冲突，代码重新检查当前状态；已经不再pending可以说明另一个处理者推进过，不能简单把所有冲突当作丢失。

## 工具claim前必须冻结哪些输入

| 输入 | 来自哪里 | 校验目的 |
|---|---|---|
| tenant/request | 可信执行上下文 | 防跨任务恢复。 |
| tool ID/version | 固定工具引用 | 防恢复时替换工具实现。 |
| canonical args digest | 规范化参数 | 防同键不同参数。 |
| recovery_policy | 工具声明 | 固定能否查询/重试/重放。 |
| idempotency_key | 恢复键或稳定默认键 | 连接平台尝试与外部业务效果。 |
| payload key version | 原输入密钥版本 | 结果加密与恢复一致。 |

只在上述不可变输入一致时复用或领取原记录。claim条件不是“有一个相同tool_call_id就继续”，而是逐项匹配；不匹配应暴露版本/幂等错误。

## 查询型恢复的三种返回

QueryRecovery的completed=true表示结果已完成，平台保存结果而不再次调用。completed=false且无错误只允许表示明确未被接受，可以启动调用。

若外部操作仍在处理或无法确定是否接受，返回错误，不允许把pending当false。否则平台会把“不知道”解释成“可安全重新开始”，破坏queryable策略的核心约束。

## Complete事务为什么停止续租后再提交

如果后台续租在自己的成功事务提交后继续执行，它会看到state不再running而返回冲突，可能误取消正在进行的确认结果镜像。当前调用先停止续租并等待goroutine结束，再校验context并Complete。

Complete使用数据库当前时间确认租期，更新succeeded并插入密文在同一事务中。停止续租不表示允许过期提交；如果真正超过租期，事务会拒绝，保留重试和核对责任。

## 确认工具的主结果与镜像

tool_execution_result是统一恢复主结果；tool_result_payload与tool_attempt保留确认流程需要的结果和授权关联。Complete之后镜像之前退出，新owner可读主结果并补齐镜像。

不能反过来只凭confirmation_grant已经consumed就再次消费或直接推断成功。consumed证明授权已使用，并不证明外部效果和结果保存全部完成。

## ReplyRelay的六个校验点

1. Outbox的kind属于reply。
2. 冻结结果能按tenant/request/ref解析。
3. 结果tenant/request/ref与Outbox一致。
4. ReplyRoute属于相同tenant/request，并有有效Binding/account/config version。
5. 构造ReplyDestination和稳定DeliveryKey，不生成新的答案身份。
6. 发布成功后由Base推进Outbox状态；发布后未标记的故障允许相同event再次出现。

某一步失败留下对应claim/错误供回收；不能为了“先让用户收到”绕过路由或内容校验直接发文本。

## Delivery关键更新条件的差异

| 更新 | 关键WHERE条件 | 不能混淆的点 |
|---|---|---|
| RenewDeliveryClaim | version、sending、owner、client_request_id、claim未到期 | 续租后version更新，后续确认使用最新记录。 |
| FinishDelivery | version、sending、owner、client_request_id | 当前SQL没有单独claim_until谓词，不应凭文档虚构。 |
| ReconcileDelivery | version、ambiguous、稳定请求键 | 对未知外部状态的核对，不是普通发送claim。 |
| 工具Complete | running、owner、fence、lease未到期 | 工具结果状态与密文同事务。 |
| 人工resolve | expected_version及可处置异常状态 | 证据审计和必要Outbox重调度同事务。 |

把所有CAS统称为“检查租约”会掩盖实际条件差异。审查时以每条SQL和调用方续期逻辑联合判断，不能只看一个方法名称。

## 接管后结果不一致时的排查顺序

先查tenant/request与原envelope，再查冻结result_ref/digest，随后查工具operation身份/业务键，最后看renderer/format与segment计划。新Worker拿到更高fence不允许覆盖不同内容的同键结果。

若只有model_request_count增加，可能只是HTTP尝试；若execution_attempt增加而无terminal，可能在重试或停放；若Ledger attempt增加却无新可见消息，可能在明确失败重试。计数必须配合状态读取。
