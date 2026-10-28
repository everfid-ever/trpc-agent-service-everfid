# 在途任务接管与幂等投递 — 测试与验收

## 固定版本、前提与时间口径

本文是可复用流程，不包含实际执行结果。每轮固定完整提交 SHA、镜像摘要、配置/迁移版本、资源限制、拓扑、RUN_ID 和证据目录。两个 Bot 对应不同租户；同一真实用户在各自原会话写入不同随机 TOKEN_A/B，后续提问不再提供代号。使用真实模型时记录调用身份但不以共用上游 key 判断隔离。

关闭受害实例自动重启/替换，共享依赖保持正常，故障节点在本轮业务验收前一直停止。不得手删 lease、重绑 Bot、重投原消息或临时修补业务代码来获得通过。

| 参数 | 执行前必须约定 |
|---|---|
| W_BASELINE | 双节点稳定窗口，覆盖多个心跳、租约与调度周期。 |
| T_READY / T_TASK / T_DELIVER | 整体就绪、原任务完成、最终可见回复时限；不得事后放宽。 |
| W_DUPLICATE / W_REPEAT | 覆盖 lease/reclaim/retry 的重复检查和稳定窗口。 |
| DELTA_POLL | 状态采样间隔，恢复时延结论包含采样误差。 |

t0 为外部确认故障时刻；t_ready 为观察到执行链路就绪；新消息收到至最终可见是请求延迟。人工等待后才发消息不能推算精确端到端 RTO。barrier 主动暂停时长单独记录，不任意扣除。

## 当前故障注入点

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

## 每个窗口独立执行

1. 每轮重建健康双节点基线、不同输入标记；记录真实执行者或发送者。两租户各发原任务，目标租户暂停，对照租户并发。
2. 原输入必须有 durable 接收证据。arm 使用 tenant_id/request_id 过滤，采集 barrier hit 快照和相关数据库状态；不能凭“模型慢”判断命中。
3. SIGKILL 实际 owner，记录 t0，并保持停止。暂停的外部依赖可按预定方案恢复，但不得替应用重入队、删租约或补发。
4. 用户不重发，观察同 request_id 的新尝试、新 fence、工具恢复、原结果与最终交付。后来新消息成功不能替代原任务完成。
5. 在原任务完成后两 Bot 各发新消息，验证执行链路未永久堵塞；W_DUPLICATE 内检查迟到重复、悬挂sending/running和积压。
6. 更换受害租户并覆盖并发；每个窗口独立判定，不由一个故障推断其他窗口成立。

## 各窗口的最小证据

| 窗口 | 命中证据 | 接管后预期 | 不能替代 |
|---|---|---|---|
| P1 | Inbox/payload/job持久，未执行 | 同request分配/复用input_seq并完成 | 客户端“已发送”。 |
| P2模型 | 实际请求成功头、无完成结果 | 新尝试取更高fence，允许模型再请求 | p2-commit只在完成后暂停。 |
| P2工具 | tool_execution running及实际效果窗口 | 结果复用或按恢复策略；业务效果数量正确 | 仅SDK tool_call文本。 |
| result-before-commit | result_ref/digest已冻结，terminal尚无 | 原结果续提交，模型调用不增加 | 一律重跑模型。 |
| P3 | terminal+reply Outbox+原冻结结果 | Relay/Delivery发原答案，无模型重算 | 仅新请求可用。 |
| P4 | ProviderMessageID接受证据、Ledger仍sending | 可查询时对账；否则ambiguous/异常处置 | p4-before-send或“客户端还没显示”。 |

## 工具恢复判定

| 契约/状态 | 预期 |
|---|---|
| succeeded且结果密文存在 | 无外部重放，校验后复用。 |
| idempotent_key未落结果 | 同稳定业务键恢复，实际效果仅一份。 |
| queryable | 已接受/处理未知不得返回false重放；完成结果复用。 |
| replay_safe | 允许重放，不把调用次数大于一当成重复业务效果。 |
| manual不确定 | tool_effect_unknown安全终结，不能判为业务成功。 |
| ask grant已消费 | 不二次消费；同逻辑调用取得工具lease后策略恢复。 |

内置local_note在工具业务事务完成、结果事务之前强杀，按tenant/business_key核对实际加密记录只有一条。同输入同工具版本同参数的普通调用即同operation；参数不同会形成不同操作，不能把模型更改业务意图也当成相同幂等请求。

## 结果判定与能力边界

内部terminal唯一、租户/会话正确、用户不重发、业务效果符合契约是自动恢复正确性的条件。严格验收要求每输入一次可见最终回复时，真实WeCom P4若无法证明完成，记录该项不满足/受限；ambiguous正确落盘仅说明安全处置分支成立，不能替代该严格验收成功。

## 最小证据闭环与计数

每次证据至少包含：版本/拓扑/参数、实例及 process_start_id、owner 与 fence/lease 变化、tenant/binding/session、external_message_id/request_id/input_seq、execution_attempt、冻结 result_ref/digest、terminal commit、Outbox、Ledger 的 segment/client_request_id/provider_message_id/state/version，以及客户端最终回复。敏感凭据/个人标识脱敏，完整正文按环境要求保护。

| 计数 | 权威记录 | 解释 |
|---|---|---|
| Worker 尝试 | `execution_attempt` 的行数 / `attempt_no` | 每次进入执行器一行；新尝试把旧 `running` 标为 `superseded`。completed 仅表示执行器正常返回。 |
| 逻辑模型调用 | `execution_record.model_call_count` | 进入模型适配器的逻辑调用，可因恢复增加。 |
| 模型 HTTP 尝试 | `execution_record.model_request_count` | transport 尝试，含连接失败及内部重试；不等于接受数或计费数。 |
| 工具尝试 / 业务效果 | `tool_execution.attempt` / 外部业务幂等记录 | 同键工具重试可大于一次，业务效果应按工具契约去重。 |
| 回复发送 / 用户可见答案 | Ledger `attempt`、`segment_no/count` / 客户端证据 | 一条答案可有多个分段；唯一 Ledger 行不能单独证明用户只见一次。 |

服务端 Ledger 一行 sent 只能证明本地终态；客户端可见重复必须直接核对。模型调用多次不直接失败，正确性看同一原请求提交、业务效果和交付边界。

内部保证与外部交付分别判定：同一输入只有一个有效 terminal commit；每个回复 segment 在 Ledger 中只有一行权威状态，`sent` 后自动路径只跳过发送。该行唯一不等于渠道只接收一次。下游已接受、本地确认未提交的 P4 窗口需要下游可靠去重或查询才能自动收敛。当前 WeCom 没有请求键查询适配器，内容级有限时间去重不足以提供永久请求级幂等；取消、HTTP 5xx 和响应丢失均可能留下不确定结果。系统不盲目补发，先保留 `ambiguous`；核对次数耗尽可转为 `failed/reconcile_exhausted`，仍需运维查证。

## 脚本与回归

```sh
bash scripts/e2e/inflight-takeover.sh p1
bash scripts/e2e/inflight-takeover.sh p2
bash scripts/e2e/inflight-takeover.sh p3
bash scripts/e2e/inflight-takeover.sh p4
```

当前脚本只接受p1–p4，使用WebUI multinode和真实模型key；其他端点需独立控制流程。它不自动覆盖真实WeCom、双租户全矩阵或所有工具效果窗口。脚本成功时的 durable WebUI reply 不应称作真实WeCom客户端证据。

## 代码回归入口

```sh
go test ./...
go test -race ./cmd/trpc-service ./trpcservice/worker ./trpcservice/channels/delivery ./trpcservice/relay ./trpcservice/reliability/drain
```

这些命令运行包/竞态测试，不等价于真实 WeCom 故障验收。若某集成测试依赖外部服务，应检查是否实际运行或跳过，不能把普通 go test 退出0视为全部部署验证完成。测试结果、日志与时延保存在独立运行目录，不回填本文。

## 建议的窗口覆盖矩阵

每一行都是独立子用例。先对A注入、B作并发对照，再交换租户；若两任务不能稳定落在同一受害实例，应如实记录实际owner，不假定两个任务同时受影响。

| 子用例 | 暂停前须确认 | 重点结果 |
|---|---|---|
| P1接收后未领取 | 原输入与后续工作durable | 接收进程退出不丢输入。 |
| 当前p1领取后未执行 | preprocess已claim，未执行 | 过期领取可回收。 |
| p2模型读取前 | 成功响应头证据，SDK无完整结果 | 新尝试可重跑模型。 |
| 工具函数体执行中 | tool_execution running及外部调用证据 | 恢复契约限制外部重放。 |
| tool-effect | 业务效果存在，Complete尚未提交 | 同键效果不重复。 |
| result-before-commit | result存在，无terminal | 模型调用不增加，续同result。 |
| p3 | terminal+Outbox，无发送接受 | 仅恢复旧结果投递。 |
| p4 | 下游接受证据，无sent | 对账或不确定处置，不谎报唯一交付。 |

当前e2e脚本只自动接受p1–p4参数；其余控制点需要独立安排。不能在命中旧p2-commit后将这一轮写成“工具执行中崩溃”。

## 查询原任务前后状态的示例

以下为psql变量查询模板。tenant_id、request_id来自原输入关联证据；只读元数据，不导出密文正文或密钥。

```sql
SELECT request_id, outcome, input_seq, model_call_count,
       model_request_count, park_attempt, cancel_version, result_ref
FROM execution_record
WHERE tenant_id = :'tenant_id' AND request_id = :'request_id';

SELECT attempt_no, worker_id, fence, state, started_at, finished_at
FROM execution_attempt
WHERE tenant_id = :'tenant_id' AND request_id = :'request_id'
ORDER BY attempt_no;

SELECT tool_call_id, tool_id, tool_version, args_digest, recovery_policy,
       idempotency_key, state, attempt, fence, lease_owner, lease_until, result_ref
FROM tool_execution
WHERE tenant_id = :'tenant_id' AND request_id = :'request_id';

SELECT tool_call_id, result_ref, content_digest, key_version, created_at
FROM tool_execution_result
WHERE tenant_id = :'tenant_id' AND request_id = :'request_id';

SELECT note_id, business_key, args_digest, created_at
FROM local_note
WHERE tenant_id = :'tenant_id' AND request_id = :'request_id';
```

execution_attempt中的superseded表示旧尝试不再是当前尝试，不能据此认定旧进程从未执行过任何外部调用。local_note只有一行是该内置工具的业务幂等证据；其他工具要查自己的外部业务系统。

## 四种计数如何形成最小报告

| 数字 | 应从哪里取得 | 示例解释 |
|---|---|---|
| 任务尝试 | execution_attempt | 可为2，说明发生一次恢复尝试。 |
| 模型调用/HTTP尝试 | execution_record两个计数 | 逻辑调用2、HTTP3并不冲突。 |
| 业务效果 | local_note或真实外部资源 | 同业务键1份。 |
| 用户最终可见答案 | 客户端与Ledger关联 | 一个答案可能分三段，但不应重复出现整段最终答案。 |

这些是口径示例，不是期望每轮固定数字。若故障发生在模型HTTP前，模型尝试可能不增加；若已有冻结结果，恢复应复用而不是强制增加一次。

## P4不能自动唯一交付时怎样判定

首先检查协议能力。如果adapter有可信查询，已交付须有ProviderMessageID；明确未交付才允许进入retry_wait。若queryable只返回“没查到”却不能证明未被接受，仍应记为unknown。

真实WeCom当前没有请求键查询适配器。该轮可以证明系统正确保留不确定结果、没有盲重发及有证据处置可执行；不能同时把严格的“全自动唯一可见回复”标通过。业务验收表应分别列“异常安全处理”和“自动交付保证”。

## 工具恢复反例

| 反例 | 违反的规则 |
|---|---|
| 模型生成新call ID就创建第二份笔记 | 普通operation身份未稳定，或业务键错误。 |
| queryable返回false表示“还在处理中” | 未接受与未知被混淆。 |
| grant已consumed后再发一个grant自动继续 | 绕过一次性授权与原调用身份。 |
| 旧工具owner租期过后仍成功Complete | owner/fence/lease校验失效。 |
| 成功状态存在却无加密结果 | 原子结果事务不成立。 |
| 手改effect_unknown为succeeded | 没有业务效果与结果证据。 |

## 本轮记录与收尾

每个点保存arm/status快照、命中时数据库状态、kill目标、故障后新owner/fence、计数变化和最终客户端证据。barrier快照为进程内数据，必须在kill前保存；它不能在节点退出后自动变成durable接受凭据。

收尾解除暂停、清理独立测试资源，恢复健康基线再进入下一个窗口。若仅执行普通聊天，不把结论扩展到有外部副作用工具。
