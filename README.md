# tRPC Agent Service：可靠性与故障接管

本项目仅专注以下能力：

1. 应用节点故障后，用户在原会话继续对话且保持租户、Bot 与上下文隔离。
2. 在途任务自动接管和幂等回复投递；用户不需重发，最终可见回复不重复。
3. 同一主机的多个容器实例共享 PostgreSQL、Redis 与协调状态运行；任一实例非优雅退出后由其他实例接管。

实现总览位于 [可靠性文档](docs/feature/reliability-failover/README.md)。其中每项能力均有一份详细实现说明，涵盖持久化模型、运行时接管、故障窗口、代码地图、演练和边界。

## 本地验收

```bash
# 全量代码回归
go test ./...

# P1–P4 中任选一个 durable 边界强杀 owner，由 peer 接管原输入
bash scripts/e2e/inflight-takeover.sh p2

# WeCom 双节点：强杀、重加入和反向故障
bash scripts/e2e/single-host-multicontainer.sh
```

演练需要 Docker Desktop；在途接管需要本机 `deploy/compose/secrets/deepseek-api-key`，WeCom 双节点演练还需要 `deploy/compose/secrets/wecom.env`。脚本在隔离 Compose project 中运行，失败时保留诊断日志位置。
