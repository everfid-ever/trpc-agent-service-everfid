# 可靠性实现文档

本仓库只维护以下三项可靠性能力。文档按 L1–L4 分层，当前代码与迁移优先于说明文字；每次实现变更需同步更新相应 L2、L3 与 L4 材料，避免语义漂移。

| 能力 | 权威说明 |
|---|---|
| 故障恢复后继续对话 | [会话连续性](feature/reliability-failover/conversation-continuity/README.md) |
| 在途任务接管与幂等投递 | [在途任务接管](feature/reliability-failover/inflight-task-takeover/README.md) |
| 单主机多容器实例高可用 | [单主机多容器](feature/reliability-failover/single-host-multicontainer/README.md) |

跨能力的身份、持久化事实、故障边界和演练入口见 [可靠性总览](feature/reliability-failover/README.md)。
每个能力目录均按 `OVERVIEW`、`FULL-GUIDE`、`design`、L3 实现材料及 L4 验收/运维材料分层。
阅读和评审统一采用 [L1–L4 与 A1–A20 方法](feature/reliability-failover/DOCUMENTATION-METHOD.md)。

## 代码事实优先级

发生描述冲突时，按以下顺序判断：数据库迁移、运行时代码、测试、本文档。特别是外部工具的故障恢复能力由其恢复契约决定；未声明安全恢复契约的工具不会在副作用未知时自动重放。
