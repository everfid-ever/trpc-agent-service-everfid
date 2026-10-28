# 三项可靠性能力

所有能力文档按 [四个深度级别与二十个审查角度](DOCUMENTATION-METHOD.md) 组织。每个能力的 README 提供文件地图以及 A1–A20 的“为什么必须有、提问式判据、文档位置”。L1–L4 正文已按当前迁移、运行时和 Compose 重新组织，CURRENT-IMPLEMENTATION 仅作摘要，各层正文直接说明当前实现。

## 当前实现状态

### 按问题定位详细说明

| 想弄清楚的问题 | 首选文档 | 接着核对 |
|---|---|---|
| 换节点为什么仍是原会话、两个 Bot 为什么不混？ | [会话机制](conversation-continuity/FULL-GUIDE.md) | [身份键、读取条件与字段设计](conversation-continuity/design.md) |
| 历史保存了却回答不对，该查哪一层？ | [会话走读](conversation-continuity/implementation-walkthrough.md) | [历史与路由排障](conversation-continuity/release-and-operations.md) |
| 原输入在任一阶段崩溃后，究竟由谁继续？ | [在途责任转移与故障推演](inflight-task-takeover/FULL-GUIDE.md) | [事务和不变量](inflight-task-takeover/design.md) |
| 工具执行一半、外部已写成功、平台没记下怎么办？ | [工具六个检查点与恢复策略](inflight-task-takeover/FULL-GUIDE.md) | [实际执行顺序](inflight-task-takeover/implementation-walkthrough.md) |
| 唯一终态、唯一 Ledger 行、只见一条答案分别是什么意思？ | [计数与分段交付边界](inflight-task-takeover/design.md) | [证据和反例](inflight-task-takeover/testing-and-acceptance.md) |
| P4 无法判定、自动核对结束后怎么处理？ | [在途运维操作单](inflight-task-takeover/release-and-operations.md) | [真实处置接口与 SQL](inflight-task-takeover/CODE-APPENDIX.md) |
| 两容器共存、重新加入和反向故障为什么安全？ | [多容器故障域与生命周期](single-host-multicontainer/FULL-GUIDE.md) | [状态归属与准入竞态](single-host-multicontainer/design.md) |
| ready、入口健康、执行资格为何不等价？ | [多容器运行时走读](single-host-multicontainer/implementation-walkthrough.md) | [故障当班与容量处置](single-host-multicontainer/release-and-operations.md) |
| 如何证明没有通过重启、重发或换会话绕过问题？ | [会话验收](conversation-continuity/testing-and-acceptance.md)、[在途验收](inflight-task-takeover/testing-and-acceptance.md) | [实例职责与轮次矩阵](single-host-multicontainer/testing-and-acceptance.md) |

各能力继续沿用 README、L1–L4 文件结构。L2 用业务故事、责任转移和反例解释原因；L3 给真实字段、算法条件与运行顺序；L4 区分实现证据、环境证据与外部事实。REFERENCE-IMPLEMENTATION 保留精确 DDL/代码契约，字段理解优先读 design 中的表格。

### 贯穿三项能力的阅读约定

- “自动接管”指在共享依赖和恢复契约满足时，由存活角色依据持久事实推进；不是转移旧 goroutine、任意工具函数栈或模型内部生成状态。
- “持久化”指保存了可恢复事实，不表示所有表、Redis 操作和外部效果在一个事务内；每一处跨边界窗口都要单独解释。
- “安全”与“自动完成”分别判断：保留 tool_effect_unknown 或 ambiguous 可避免盲重放，但不等于该任务已经成功或答案已经送达。
- “历史可读取”与“模型正确回忆”分别取证：存储存在不证明所有历史已进入本次模型上下文。
- “支持验收路径”表示有步骤、观测和判据，不把未实际执行的持续负载、真实渠道和容量指标写成通过结果。

三项能力均已落入同一套共享运行时：PostgreSQL 保存权威业务状态，Redis 提供会话与任务协调，Worker/Relay/Delivery 以独立节点身份竞争可回收的工作。节点失效不会把内存状态当作恢复依据；接管者只依据持久化记录、租约和 Fencing Token 行动。

| 文档包 | 内容 |
|---|---|
| [会话连续性](conversation-continuity/README.md) / [当前基线](conversation-continuity/CURRENT-IMPLEMENTATION.md) | 身份链路、会话 ownership、双 Bot tenant 隔离和故障续办 |
| [在途任务接管](inflight-task-takeover/README.md) / [当前基线](inflight-task-takeover/CURRENT-IMPLEMENTATION.md) | P1–P4、lease/fence、Outbox/Ledger、工具恢复协议 |
| [多容器 HA](single-host-multicontainer/README.md) / [当前基线](single-host-multicontainer/CURRENT-IMPLEMENTATION.md) | Compose 拓扑、入口、身份、强杀接管、重加入与反向故障 |

| 能力 | 关键事实 | 当前实现 |
|---|---|---|
| 故障后继续对话 | 租户、Bot Binding、会话和外部消息身份 | 持久化路由、输入、会话提交和回复路由；双 WeCom Bot 使用不同 tenant、binding、app 和密钥作用域 |
| 在途任务接管与幂等投递 | 输入、执行、提交、投递和工具副作用 | Inbox/Outbox、会话 lease/fence、队列 reclaim、Delivery Ledger、工具执行租约与恢复策略 |
| 单主机多容器 HA | 多节点身份和共享协调资源 | Compose 双节点、稳定回调入口、独立端口与 `process_start_id`，单节点 SIGKILL 后由对端接管 |

## 共享执行链路

```text
已验签 Bot Binding
  -> Inbox / Payload / ReplyRoute
  -> preprocess job / broker delivery
  -> session lease + fence
  -> model / durable tool execution (allow + ask)
  -> terminal session commit + Outbox
  -> reply relay
  -> Delivery Ledger
  -> 外部渠道最终回复
```

持久化接收先于异步执行，terminal与reply Outbox同事务；工具结果、SDK历史、最终结果冻结和外部发送各有独立窗口。SDK历史写入不能随平台terminal回滚。旧owner的业务提交由fence/version/claim条件限制，外部副作用仍须工具与渠道协议保护。

最新在途恢复增量由 `000004_inflight_recovery` 提供：独立 Worker 尝试日志、逻辑模型调用与 HTTP 尝试分计数、普通/确认工具统一恢复、原子加密工具结果及真实幂等笔记写入。`p2`/`p4` 端点已调整为模型请求接受后/下游发送接受后，旧边界另名保留；当前精确定义见 [在途任务接管](inflight-task-takeover/README.md)。

## 故障承诺与边界

- 单个应用节点失效后，已持久化接收的输入由系统回收或按恢复契约处理，无需用户重发；不可安全重放的工具可收敛为明确失败/效果不确定。
- 模型调用和任务尝试次数可以因接管增加；Delivery Ledger记录每个分段的有效发送状态，可见回复数另以客户端证据核对。
- 下游渠道支持可靠请求级去重或可查询时，可将“请求已发出但本地未确认”收敛；否则先保存 ambiguous，核对耗尽可转为 failed/reconcile_exhausted，并通过外部证据处置。Ledger 单行不能证明客户端仅一次可见，不声称无条件 exactly-once。
- 工具调用不是所有都可安全重放：只有 `replay_safe`、`idempotent_key` 或 `queryable` 契约允许自动续办；`manual` 契约会终止为 `tool_effect_unknown`。
- 范围是单主机内的应用实例故障。宿主机、Docker daemon、PostgreSQL、Redis 或外部渠道整体不可用不属于本地双实例 HA 承诺。

## 验证入口

| 目标 | 命令 | 覆盖 |
|---|---|---|
| P1–P4 在途接管 | `bash scripts/e2e/inflight-takeover.sh p2` | 可替换为 `p1`、`p3`、`p4`；命中 barrier 后 SIGKILL owner，由 peer 完成原输入 |
| WeCom 双节点生命周期 | `bash scripts/e2e/single-host-multicontainer.sh` | N1 强杀、N1 新身份重加入、N2 反向强杀和稳定入口健康检查 |
| 全量单元/包测试 | `go test ./...` | 运行时、工具恢复、接管与交付回归 |

两份 E2E 脚本需要相应 Compose 密钥文件。真实 WeCom 用户会话属于外部渠道验收；脚本验证本地拓扑、接管和持久化语义。
