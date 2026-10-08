# 单主机多容器实例高可用

本目录按 L1–L4 分层；跨层的审查问题见 [文档方法](../DOCUMENTATION-METHOD.md)。Compose profile、稳定入口和双 Bot 独立 tenant 的当前配置以 L3 参考实现和 `docker-compose.local.yml` 为准。

## 文档地图

| 文件 | 级别 | 回答什么问题 | 谁读 / 何时读 | 可整篇跳过 |
|---|---|---|---|---|
| `README.md` | 导航 | 范围、拓扑入口、已实现演练 | 所有人，先读 | — |
| `OVERVIEW.md` | L1 | 场景、术语、业务故事和不包含 | 管理/产品/交接 | — |
| `FULL-GUIDE.md` / `design.md` | L2 | 入口/节点机制、故障时序、竞态和隔离 | 架构评审/实现前 | 非代码读者可跳过设计细节 |
| `implementation-walkthrough.md` / `CODE-APPENDIX.md` / `REFERENCE-IMPLEMENTATION.md` | L3 | 运行时顺序、真实代码、Compose/配置/启动顺序 | 实现/外部复刻 | 非代码/纯验收 |
| `testing-and-acceptance.md` / `release-and-operations.md` | L4 | 强杀、重加入、反向故障、证据与处置 | QA/SRE/上线 | 非验收角色 |

## 拓扑

`wecom-ha-local` Compose profile 启动一次 bootstrap、两个应用节点和一个稳定入口：

```text
WeCom callback
  -> wecom-ha-entry :58087
  -> wecom-ha-node-a :58088   ┐
  -> wecom-ha-node-b :58089   ┘ shared PostgreSQL + Redis
```

节点容器内均监听 `:8080`。入口持续探测各节点 `/readyz`，只向健康节点转发回调；`/statusz` 显示后端健康状态。节点共享数据库、队列和协调服务，但每次启动均有不同的 `process_start_id`，并据此构造 Worker、Relay、Delivery 等 owner 身份。

## 隔离与共享

| 类别 | 内容 |
|---|---|
| 共享权威状态 | PostgreSQL 业务记录、Redis 协调/队列、已发布配置和持久化 payload/result |
| 节点局部状态 | HTTP 监听端口、进程启动身份、运行中的 goroutine、短暂连接和本地临时状态 |
| 防冲突机制 | 独立实例 ID/端口、lease、fence、条件更新、Outbox/Delivery Ledger |

`restart: "no"` 故意禁用自动重启：演练验证的是存活节点的业务接管，而非 Docker 重启掩盖故障。重新加入的节点必须显式启动，并以新的 `process_start_id` 重新参与竞争，不能复用旧 owner。

## 非优雅终止与重新加入

当任一节点被 `SIGKILL`：

1. 稳定入口将其标记为不健康，继续向存活节点转发后续回调。
2. 已领取但未 ACK 的任务在 lease/消费者 reclaim 条件满足后由存活节点重新领取。
3. 会话 fence、工具执行 lease、Relay claim 和 Delivery Ledger 防止旧 owner 或并发 owner 重复提交、发送或产生业务副作用。
4. 重启受害容器后，节点以新身份加入；再强杀原存活节点，重加入节点可反向接管。

## 演练

准备 `deploy/compose/secrets/deepseek-api-key` 与 `deploy/compose/secrets/wecom.env` 后运行：

```bash
bash scripts/e2e/single-host-multicontainer.sh
```

脚本验证：双节点和入口 ready、N1 强杀后 N2 健康、N1 新身份重加入、N2 反向强杀后 N1 健康，并保留 `/statusz`、Compose 状态和日志诊断。`TRPC_WECOM_REAL_ACCEPTANCE=recorded` 只表示真实渠道会话证据已在外部保存；本地脚本本身不伪造真实 WeCom 消息。

## 范围

本能力处理同一宿主机上单个应用实例的故障。它不覆盖宿主机掉电、Docker daemon、共享 PostgreSQL/Redis、外部模型或渠道整体故障；这些属于共享故障域或跨主机 HA 议题。
