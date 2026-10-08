# 本地可靠性 Compose

Compose 文件 `docker-compose.local.yml` 提供两种演练拓扑：

| Profile | 用途 | 演练入口 |
|---|---|---|
| `webui-multinode` | 两个 WebUI 节点共享 PostgreSQL/Redis，验证 P1–P4 在途任务接管 | `bash scripts/e2e/inflight-takeover.sh p1|p2|p3|p4` |
| `wecom-ha-local` | 两个 WeCom 应用节点和稳定入口，验证 SIGKILL、重加入和反向故障 | `bash scripts/e2e/single-host-multicontainer.sh` |

节点拥有独立端口、实例 ID 和启动身份；Worker、Relay、Delivery 使用独立 owner。它们共享 PostgreSQL、Redis 与 durable 业务状态，接管的正确性由 lease、Fencing Token、Outbox 和 Delivery Ledger 保证，而不是由容器自动重启保证。

完整语义见 [可靠性实现文档](../../docs/feature/reliability-failover/README.md)。
