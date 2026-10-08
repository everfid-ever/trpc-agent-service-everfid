# 三项可靠性能力

所有能力文档按 [四个深度级别与二十个审查角度](DOCUMENTATION-METHOD.md) 组织。先用 L1 确认承诺，再依次检查 L2 机制、L3 实现和 L4 证据；每个能力的 `README.md` 提供对应文件地图。

## 当前实现状态

三项能力均已落入同一套共享运行时：PostgreSQL 保存权威业务状态，Redis 提供会话与任务协调，Worker/Relay/Delivery 以独立节点身份竞争可回收的工作。节点失效不会把内存状态当作恢复依据；接管者只依据持久化记录、租约和 Fencing Token 行动。

| 文档包 | 内容 |
|---|---|
| [会话连续性](conversation-continuity/README.md) | 身份链路、会话 ownership、双 Bot tenant 隔离和故障续办 |
| [在途任务接管](inflight-task-takeover/README.md) | P1–P4、lease/fence、Outbox/Ledger、工具恢复协议 |
| [多容器 HA](single-host-multicontainer/README.md) | Compose 拓扑、入口、身份、强杀接管、重加入与反向故障 |

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
  -> model / confirmed tool
  -> terminal session commit + Outbox
  -> reply relay
  -> Delivery Ledger
  -> 外部渠道最终回复
```

每个箭头左侧的状态先 durable，再允许右侧的工作开始。旧 owner 即使恢复，也会因为较低的 fence、失效 lease 或 Ledger 条件更新而不能覆盖新 owner。

## 故障承诺与边界

- 单个应用节点失效后，已持久化接收的输入无需用户重发。
- 模型调用次数可以因接管增加；任务尝试次数可以增加；同一最终回复的可见投递通过 Delivery Ledger 收敛。
- 下游渠道支持请求级去重或可查询时，可将“请求已发出但本地未确认”收敛为单次可见回复；下游既不能去重也不能查询时，只能保留不确定状态，不能声称绝对 exactly-once。
- 工具调用不是所有都可安全重放：只有 `replay_safe`、`idempotent_key` 或 `queryable` 契约允许自动续办；`manual` 契约会终止为 `tool_effect_unknown`。
- 范围是单主机内的应用实例故障。宿主机、Docker daemon、PostgreSQL、Redis 或外部渠道整体不可用不属于本地双实例 HA 承诺。

## 验证入口

| 目标 | 命令 | 覆盖 |
|---|---|---|
| P1–P4 在途接管 | `bash scripts/e2e/inflight-takeover.sh p2` | 可替换为 `p1`、`p3`、`p4`；命中 barrier 后 SIGKILL owner，由 peer 完成原输入 |
| WeCom 双节点生命周期 | `bash scripts/e2e/single-host-multicontainer.sh` | N1 强杀、N1 新身份重加入、N2 反向强杀和稳定入口健康检查 |
| 全量单元/包测试 | `go test ./...` | 运行时、工具恢复、接管与交付回归 |

两份 E2E 脚本需要相应 Compose 密钥文件。真实 WeCom 用户会话属于外部渠道验收；脚本验证本地拓扑、接管和持久化语义。
