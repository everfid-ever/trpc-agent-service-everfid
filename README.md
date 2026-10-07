# tRPC Agent Service：可靠性与故障接管

本项目仅专注以下能力：

1. 应用节点故障后，用户在原会话继续对话且保持租户、Bot 与上下文隔离。
2. 在途任务自动接管和幂等回复投递；用户不需重发，最终可见回复不重复。
3. 同一主机的多个容器实例共享 PostgreSQL、Redis 与协调状态运行；任一实例非优雅退出后由其他实例接管。

功能设计、参考实现、测试和运维说明位于 [docs/feature/reliability-failover](docs/feature/reliability-failover/README.md)。

## 本地验收

```bash
# 两个节点在 PostgreSQL/Redis 短断后恢复
bash scripts/e2e/dependency-recovery.sh

# node-a/node-b 双向 SIGKILL 后完成 durable reply
bash scripts/e2e/node-takeover.sh
```

后两个演练需要 Docker Desktop 和本机 `deploy/compose/secrets/deepseek-api-key`。脚本在隔离 Compose project 中运行，失败时保留诊断日志位置。
