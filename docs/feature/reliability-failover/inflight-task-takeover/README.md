# 在途任务接管与幂等投递

本目录按 L1–L4 分层；跨层的审查问题见 [文档方法](../DOCUMENTATION-METHOD.md)。最新代码增量为迁移 `000004_inflight_recovery`：统一普通/确认工具恢复、加密工具结果、独立任务尝试记录、真实笔记业务写入及模型 HTTP 请求尝试计数。迁移 `000002` 的工具执行记录和 `000003` 的投递处置继续保留。

## 文档地图

| 文件 | 级别 | 回答什么问题 | 谁读 / 何时读 | 可整篇跳过 |
|---|---|---|---|---|
| `README.md` | 导航 | 覆盖范围、当前实现和阅读入口 | 所有人，先读 | — |
| `OVERVIEW.md` | L1 | 场景、术语、业务故事、承诺/边界 | 管理/产品/交接 | — |
| `FULL-GUIDE.md` / `design.md` | L2 | P1–P4 因果链、状态机、竞态及“不能只做 X” | 架构评审/实现前 | 非代码读者可跳过设计细节 |
| `implementation-walkthrough.md` / `CODE-APPENDIX.md` / `REFERENCE-IMPLEMENTATION.md` | L3 | 运行时顺序、代码证据、DDL/Redis/接口/算法/配置/启动顺序 | 实现/外部复刻 | 非代码/纯验收 |
| `testing-and-acceptance.md` / `release-and-operations.md` | L4 | 判定表、最小证据、反例、演练、上线与处置 | QA/SRE/上线 | 非验收角色 |

## 目标

同一条已持久化输入在节点故障后无需用户重发。系统区分模型调用、任务尝试和最终回复：模型可以重跑，任务 lease 可以易主，但一个输入只允许一个有效 terminal commit，最终可见回复由投递台账收敛。

## 四个故障窗口

| 窗口 | 已持久化事实 | 接管动作 |
|---|---|---|
| P1：消息已持久化、未执行 | Inbox、Payload、预处理任务 | 存活节点扫描/领取任务并继续 dispatch |
| P2：模型或工具执行中 | broker delivery、会话 lease/fence、确认与工具状态 | 未 ACK 的消息被 reclaim；新 owner 取得更高 fence 后继续或安全恢复工具 |
| P3：结果已提交、未发布回复 | terminal session commit、Outbox、结果 payload | 不重跑模型；Relay 重新发布已保存的 reply event |
| P4：下游已接受、本地发送确认未落盘 | Ledger 仍为 `sending`，暂停快照保存渠道接受凭据 | 渠道支持按请求键查询时确认已交付；不能查询时进入不确定投递处置，不盲目补发 |

当前测试端点 `p1` 在持久领取后执行前，`p2` 在真实模型返回成功响应头后、SDK 读取结果前，`p3` 在 reply publish 前，`p4` 在下游接受成功后、Ledger 置为 sent 前。原来两个边界保留为 `p2-commit` 和 `p4-before-send`，不能拿来代替新的 P2/P4。另有 `tool-effect` 暂停点，位于真实工具效果完成后、工具结果事务提交前。

`result-before-commit` 还覆盖冻结结果已保存、terminal/Outbox 未提交的间隙。接管者验证原结果的租户、请求、密钥版本和内容摘要，重新检查取消意图及执行权后，直接续提交相同结果与回复 Outbox；不再次调用模型。此时官方 SDK 会话事件已持久化，不复制第二份聊天历史。

`/test/failover/<point>/arm` 可带 `tenant_id`、`request_id` 查询参数，只暂停目标原任务；快照回显命中租户、请求和可用凭据。P2 的凭据为成功响应头状态，不能证明模型内部仍在生成；它证明请求已发出且应用还未取得完成结果。P4 的凭据为渠道返回的 ProviderMessageID。`scripts/e2e/inflight-takeover.sh p1|p2|p3|p4` 使用当前四个端点，真实 WeCom P4 仍受渠道能力边界约束。

## 接管与提交规则

- 会话 lease 保证同一会话只有一个活动执行者；续租失败立即失去执行权。
- Fencing Token 单调增加，并在会话提交处校验，旧 owner 不能覆盖新 owner。
- 输入只有 durable 后才 ACK；执行失败的 delivery 保持 pending，供消费者组 reclaim。
- terminal commit 与 Outbox 在同一持久化事务中产生；Relay 与 Delivery 可独立重试。
- Delivery Ledger 对每个回复片段做条件领取和状态推进，`sent` 后不再次发送。

## 工具调用接管

普通 `allow` 和确认 `ask` 工具共用 `tool_execution`、加密 `tool_execution_result` 与恢复策略；生产运行时同时注入执行 Store 和密钥解析器，缺少加密结果能力时拒绝降级调用。`nil` Store 的旧独立调用接口不提供分布式恢复保证。键为 `(tenant_id, request_id, tool_call_id)`，保存工具/版本、参数摘要、恢复策略、幂等键、状态、尝试次数、fence、lease owner/expiry、结果引用和错误。接管只允许领取 pending、明确不确定或租约已过期的 running，并提高工具 fence；未过期的 running 不会被抢走。

普通调用的 operation ID 由 tenant、request、工具版本和规范化参数摘要派生，避免模型重试更换工具调用 ID 导致副作用重做。同一输入内同一工具版本和相同参数代表同一逻辑操作；有意产生不同效果时必须携带不同业务参数/幂等键。确认工具继续使用持久化确认中的调用 ID。工具实现必须遵守声明的幂等契约，平台不能把任意外部写入自动变成幂等。

成功结果先与工具成功状态在同一数据库事务中提交，事务验证 owner、fence 和未过期租期；后续 owner 直接读取加密结果。续租失败取消实际工具调用的上下文；不响应取消的外部操作仍需业务幂等键或查询契约保护。`queryable` 的未完成返回必须证明操作未被接受；仍在处理或无法判断时必须返回错误，禁止当成可重新执行。

| 恢复策略 | 接管后的动作 |
|---|---|
| `queryable` | 先用稳定恢复键查询外部操作；若已完成，只持久化结果，不再调用外部系统 |
| `idempotent_key` | 用同一稳定幂等键重试；外部系统应将重复请求折叠为同一业务效果 |
| `replay_safe` | 可直接重放 |
| `manual` | 禁止自动重放，记录 `effect_unknown` 并将任务收敛为 `tool_effect_unknown` |

确认令牌只消费一次。若旧节点已消费令牌但在结果落库前死亡，接管节点会先查找已保存结果；没有结果时，它仅在取得同一工具执行租约且策略允许时恢复，绝不会再次消费令牌。`tool_attempt` 与加密的 `tool_result_payload` 记录最终业务效果和结果引用。

当前内置 `webui_create_note` 工具声明 `idempotent_key`，在两个 Bot 租户中均注册真实 PostgreSQL Writer。`local_note` 按 `(tenant_id,business_key)` 唯一保存加密笔记，业务事务先于工具结果事务完成；故障后同键写入复用同一笔记。未提供 Writer 时工具拒绝执行，不再返回虚构的 created。未来新增带副作用的工具必须实现恢复契约，否则默认 `manual`。

## 独立计数与新增数据

| 数据 | 语义 |
|---|---|
| `execution_attempt` | 持有会话租约的 Worker 每次进入执行器独立一行，保存 worker/fence/开始结束；新尝试将遗留 running 标为 superseded。completed 仅表示此次执行器调用正常返回，不等于整个业务任务已经成功 |
| `execution_record.model_call_count` | 逻辑模型调用计数，包括进入模型适配器的调用；不是上游接受证明 |
| `execution_record.model_request_count` | HTTP transport 发起请求的尝试次数，含适配器内部重试和连接失败；不是服务商计费或接受次数 |
| `tool_execution.attempt` | 单个逻辑工具操作的领取/恢复次数 |
| `tool_execution_result` | 按 tenant/request/tool-call 存储 AES-GCM 密文、nonce、密钥版本、摘要和结果引用，与工具成功状态原子提交 |
| `local_note` | 真实业务效果，一条业务幂等键只对应一条加密笔记 |
| `delivery_ledger` | 每个回复 segment 的发送尝试、核对尝试和发送状态；分段数不能直接当成最终答案数 |

管理接口 `/ops/reliability?tenant_id=...` 增加任务尝试明细、模型 HTTP 尝试和笔记业务效果计数。接口鉴权及异常投递处置见 [部署说明](../../../../deploy/compose/README.md)。

## 验证与边界

- `go test ./trpcservice/tool ./trpcservice/worker ./trpcservice/agent` 覆盖查询恢复不重放、消费冲突下的接管、人工恢复拒绝重放和续办路径。
- `go test ./...` 覆盖全量回归。
- 如果下游渠道不支持请求级去重且不能查询接受状态，P4 无法在理论上证明恰好一次；系统只能保留不确定状态并告警。
