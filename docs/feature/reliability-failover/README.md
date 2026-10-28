# 三项可靠性能力

所有能力文档按 [四个深度级别与二十个审查角度](DOCUMENTATION-METHOD.md) 组织。每个能力的 README 提供文件地图以及 A1–A20 的“为什么必须有、提问式判据、文档位置”。L1–L4 正文已按当前迁移、运行时和 Compose 重新组织，CURRENT-IMPLEMENTATION 仅作摘要，各层正文直接说明当前实现。

## 当前实现状态

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
