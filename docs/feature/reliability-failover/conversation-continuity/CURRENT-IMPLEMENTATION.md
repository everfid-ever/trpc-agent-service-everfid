# 故障恢复后继续对话 — 当前实现摘要

本文是阅读摘要，详细机制、字段、代码及验收分别在同目录L1–L4文档中展开，所有正文使用同一当前实现口径。

保护两个独立 Bot 的租户归属、原会话上下文、请求身份与回复目的地。故障发生在一轮已完成之后，再发送新消息验证会话连续性；处理中的旧消息另按在途接管判定。

## 身份、历史与恢复

当前 Worker 还为 SDK 事件标记可信 root request/Fence；读取副本省略同一原请求的旧尝试、其他请求较低 Fence 的迟到事件及可辨识孤立工具消息，按 Event.ID 去重。确认恢复仅保留原输入和准确批准的 call；持久原始事件仍保留，不能宣称数据库历史原子回滚。治理结算中断则从加密 execution_completion 复用原审核决定与答案，避免关闭预约后重新 Begin 失败。

可信Binding固定tenant/app/account，HMAC派生user/session；Inbox按原渠道消息去重，ReplyRoute冻结。SDK session_events是上下文权威，平台head/commit负责顺序、fence和Outbox。SDK写入与平台terminal事务独立，不能回滚已写SDK事件。Worker校准fence、reclaim原工作，已冻结答案直接续提交，原会话新消息从共享历史继续。

## 共同计数与投递边界

| 计数 | 权威记录 | 解释 |
|---|---|---|
| Worker 尝试 | `execution_attempt` 的行数 / `attempt_no` | 每次进入执行器一行；新尝试把旧 `running` 标为 `superseded`。completed 仅表示执行器正常返回。 |
| 逻辑模型调用 | `execution_record.model_call_count` | 进入模型适配器的逻辑调用，可因恢复增加。 |
| 模型 HTTP 尝试 | `execution_record.model_request_count` | transport 尝试，含连接失败及内部重试；不等于接受数或计费数。 |
| 工具尝试 / 业务效果 | `tool_execution.attempt` / 外部业务幂等记录 | 同键工具重试可大于一次，业务效果应按工具契约去重。 |
| 回复发送 / 用户可见答案 | Ledger `attempt`、`segment_no/count` / 客户端证据 | 一条答案可有多个分段；唯一 Ledger 行不能单独证明用户只见一次。 |

内部保证与外部交付分别判定：同一输入只有一个有效 terminal commit；每个回复 segment 在 Ledger 中只有一行权威状态，`sent` 后自动路径只跳过发送。该行唯一不等于渠道只接收一次。下游已接受、本地确认未提交的 P4 窗口需要下游可靠去重或查询才能自动收敛。当前 WeCom 没有请求键查询适配器，内容级有限时间去重不足以提供永久请求级幂等；取消、HTTP 5xx 和响应丢失均可能留下不确定结果。系统不盲目补发，先保留 `ambiguous`；核对次数耗尽可转为 `failed/reconcile_exhausted`，仍需运维查证。

## 详细文档

见 [概念](OVERVIEW.md)、[机制](FULL-GUIDE.md)、[字段与状态](design.md)、[运行走读](implementation-walkthrough.md)、[代码](CODE-APPENDIX.md)、[参考实现](REFERENCE-IMPLEMENTATION.md)、[验收](testing-and-acceptance.md)、[运维](release-and-operations.md)。
