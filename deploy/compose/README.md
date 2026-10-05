# 本地多实例 Compose

`webui-multinode` 启动 node-a 与 node-b。两者使用独立端口与 Worker、relay、delivery、wakeup consumer 身份，但共享 PostgreSQL、Redis 和 Skill staging 卷。

使用 [node-takeover.sh](../../scripts/e2e/node-takeover.sh) 验证任一节点被 `SIGKILL` 后的双向故障接管；使用 [dependency-recovery.sh](../../scripts/e2e/dependency-recovery.sh) 验证共享 PostgreSQL/Redis 短断恢复。
