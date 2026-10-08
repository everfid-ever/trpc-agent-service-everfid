# 故障恢复后继续对话

本目录按 L1–L4 分层；跨层的审查问题见 [文档方法](../DOCUMENTATION-METHOD.md)。发生历史说明与当前代码不一致时，以 L3 的 `REFERENCE-IMPLEMENTATION.md` 中“当前实现增量”及数据库迁移为准。

## 文档地图

| 文件 | 级别 | 回答什么问题 | 谁读 / 何时读 | 可整篇跳过 |
|---|---|---|---|---|
| `README.md` | 导航 | 范围、入口、已实现能力 | 所有人，先读 | — |
| `OVERVIEW.md` | L1 | 场景、术语、业务故事、边界 | 管理/产品/交接 | — |
| `FULL-GUIDE.md` / `design.md` | L2 | 机制、故障窗口、架构、状态机与竞态 | 架构评审/实现前 | 非代码读者可跳过设计细节 |
| `implementation-walkthrough.md` / `CODE-APPENDIX.md` / `REFERENCE-IMPLEMENTATION.md` | L3 | 运行时顺序、真实代码、DDL/接口/配置/启动顺序 | 实现和复刻 | 非代码/纯验收 |
| `testing-and-acceptance.md` / `release-and-operations.md` | L4 | 证据、反例、演练、发布门禁和处置 | QA/SRE/上线 | 非验收角色 |

## 目标

用户在原 Bot 会话中继续发言时，应用节点故障不能造成上下文、租户归属或回复路由串扰。恢复不是复制故障进程内存，而是由新节点从共享的持久化身份和会话事实继续处理。

## 持久化身份链路

每条输入从已验证的渠道 Binding 推导身份，而不信任请求体中的租户字段：

```text
tenant_id
  + channel_binding_id (Bot Binding)
  + agent_app_id
  + session_id / external conversation identity
  + external_message_id
  -> request_id / inbox record / payload / reply route
```

Worker 以该身份打开同一会话、读取已提交的会话状态并以更高 fence 提交下一轮。回复投递也读取冻结的 `ReplyRoute`，因此接管后不会把同一请求发往另一个 Bot 或另一个 tenant。

## 双 Bot 隔离

本地 WeCom 组合根会创建主、次两个独立的 tenant/app/binding：

- 主 Bot：主 tenant、`local-wecom` binding 和主模型/密钥作用域。
- 次 Bot：独立 tenant、`local-wecom-secondary` binding、独立 app、模型 profile、payload/identity/session/model/channel 密钥作用域。

次 Bot 的路由构造会校验其 tenant、binding 和 app 均不同于主 Bot。共享 PostgreSQL/Redis 仅表示共享基础设施，不表示共享业务身份或会话。

## 节点故障后的处理

1. 稳定回调入口或健康节点接收后续渠道回调。
2. 存活 Worker reclaim 未确认的 broker delivery，并重新取得对应会话的 lease。
3. lease 的 fence 高于已提交 fence 后才能执行或提交；旧节点恢复后的写入被拒绝。
4. 已提交的会话上下文、terminal result 和 reply route 被复用；未完成输入进入在途任务接管流程。

回调式 WeCom 入口没有“迁移长连接”的动作；所谓连接接管是稳定公开入口将后续回调只转发至 ready 节点，Worker 接管则由共享队列和会话协调完成。

## 验证

运行 `bash scripts/e2e/single-host-multicontainer.sh` 可验证双节点入口、强杀、重加入和反向故障。`cmd/trpc-service/webui_local_role_test.go` 覆盖次 Bot tenant/binding/app 独立性。真实 WeCom 原会话记忆需要配置真实渠道凭据并在渠道侧观察。

## 边界

本能力保证应用节点故障后的身份与会话连续性；不承诺恢复模型 provider 的内部生成过程。模型执行中断时，由在途任务接管重新运行必要步骤，但只允许一个有效的持久化提交。
