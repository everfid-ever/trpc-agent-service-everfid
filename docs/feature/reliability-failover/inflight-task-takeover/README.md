# 在途任务接管与幂等投递

本目录按 L1–L4 分层；跨层的审查问题见 [文档方法](../DOCUMENTATION-METHOD.md)。`tool_execution`、可查询/幂等工具恢复及已消费 Grant 接管属于当前实现增量，以 L3 文档中的增量章节和迁移 `000002_tool_execution_recovery` 为准。

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
| P4：外部发送已开始、未确认 | Delivery Ledger、渠道请求身份 | 以 `client_request_id` 去重或查询后完成；无法确认时标记不确定而不谎报成功 |

测试 barrier 分别挂在 P1（预处理前）、P2（terminal commit 前）、P3（reply publish 前）和 P4（provider delivery 前）。`scripts/e2e/inflight-takeover.sh p1|p2|p3|p4` 命中 barrier 后强杀 owner，再观察 peer 收敛。

## 接管与提交规则

- 会话 lease 保证同一会话只有一个活动执行者；续租失败立即失去执行权。
- Fencing Token 单调增加，并在会话提交处校验，旧 owner 不能覆盖新 owner。
- 输入只有 durable 后才 ACK；执行失败的 delivery 保持 pending，供消费者组 reclaim。
- terminal commit 与 Outbox 在同一持久化事务中产生；Relay 与 Delivery 可独立重试。
- Delivery Ledger 对每个回复片段做条件领取和状态推进，`sent` 后不再次发送。

## 工具调用接管

需要确认的工具另有 `tool_execution` 记录，键为 `(tenant_id, request_id, tool_call_id)`，保存工具/版本、参数摘要、恢复策略、幂等键、状态、尝试次数、fence、lease owner/expiry、结果引用和错误。接管者只能在原 lease 到期后领取该记录，并持有更高 fence。

| 恢复策略 | 接管后的动作 |
|---|---|
| `queryable` | 先用稳定恢复键查询外部操作；若已完成，只持久化结果，不再调用外部系统 |
| `idempotent_key` | 用同一稳定幂等键重试；外部系统应将重复请求折叠为同一业务效果 |
| `replay_safe` | 可直接重放 |
| `manual` | 禁止自动重放，记录 `effect_unknown` 并将任务收敛为 `tool_effect_unknown` |

确认令牌只消费一次。若旧节点已消费令牌但在结果落库前死亡，接管节点会先查找已保存结果；没有结果时，它仅在取得同一工具执行租约且策略允许时恢复，绝不会再次消费令牌。`tool_attempt` 与加密的 `tool_result_payload` 记录最终业务效果和结果引用。

当前内置 `webui_create_note` 工具声明 `idempotent_key`，其稳定键由 tenant、request、tool call 和参数摘要推导。未来新增带外部副作用的工具必须实现相应恢复契约，否则默认 `manual`。

## 验证与边界

- `go test ./trpcservice/tool ./trpcservice/worker ./trpcservice/agent` 覆盖查询恢复不重放、消费冲突下的接管、人工恢复拒绝重放和续办路径。
- `go test ./...` 覆盖全量回归。
- 如果下游渠道不支持请求级去重且不能查询接受状态，P4 无法在理论上证明恰好一次；系统只能保留不确定状态并告警。
