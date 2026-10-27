# 本地可靠性 Compose

Compose 文件 `docker-compose.local.yml` 提供两种演练拓扑：

| Profile | 用途 | 演练入口 |
|---|---|---|
| `webui-multinode` | 两个 WebUI 节点共享 PostgreSQL/Redis，验证 P1–P4 在途任务接管 | `bash scripts/e2e/inflight-takeover.sh p1|p2|p3|p4` |
| `wecom-ha-local` | 两个 WeCom 应用节点和稳定入口，验证 SIGKILL、重加入和反向故障 | `bash scripts/e2e/single-host-multicontainer.sh` |

节点拥有独立端口、实例 ID 和启动身份；Worker、Relay、Delivery 使用独立 owner。它们共享 PostgreSQL、Redis 与 durable 业务状态，接管的正确性由 lease、Fencing Token、Outbox 和 Delivery Ledger 保证，而不是由容器自动重启保证。

完整语义见 [可靠性实现文档](../../docs/feature/reliability-failover/README.md)。

## 运行时就绪与处置接口

`/readyz` 在共享依赖可用且预处理、Worker、三个 Relay、唤醒消费者、投递消费者和确认过期调度均成功运行后返回 200；启动未完成、组件异常或退出期间返回 503。空闲轮询计为成功，模型生成耗时不作为组件故障判据。投递监督器需要每个账号消费者真实成功访问队列，启动 goroutine 本身不算就绪。状态基于最近一次实际操作，不能作为任意线程死锁检测器。

启用 `TRPC_WEBUI_LOCAL_STATUS_ENABLED=true` 时，`/statusz` 返回 `instance_id`、`process_start_id`、`owners`、`components` 和 `components_ready`。组件包括最近成功/失败时间、失败次数和脱敏原因；该接口的 `components_ready` 不代替 `/readyz` 对共享依赖的实时检查。每次进程启动都会更换 owner 后缀，与是否启用状态接口无关。企业微信使用 HTTP 回调，`channel_mode=http_callback`，没有长连接 Bot owner。

设置独立的 `TRPC_RUNTIME_OPERATIONS_TOKEN`（至少 32 字符，不得复用 WebUI token）后才注册以下管理接口；Compose 可通过节点加载的 `.env.local` 注入。请求头 `X-TRPC-Local-Token` 携带此管理 token。稳定企业微信入口不代理这些路径，管理调用直连节点。

| 接口 | 输入 | 行为 |
|---|---|---|
| `GET /ops/reliability` | `tenant_id` | 单次数据库快照返回该租户 Inbox、执行、Outbox、投递、工具执行计数，以及预处理/Outbox claim、投递异常和最近处置记录；各明细最多 100 条，不返回聊天正文或凭据 |
| `GET /ops/session/lease` | `tenant_id`、`agent_app_id`、`session_id` | 原子读取 Redis 当前执行者、Fence 和剩余租期；不泄露可用于续租/释放的 LeaseID，不修改租约 |
| `POST /ops/delivery/resolve` | 下述 JSON | 核实外部结果后的投递处置；版本不匹配或原 Outbox 正被 Relay 持有时返回 409 |

处置 JSON 字段：`tenant_id`、`delivery_key`、`segment_no`、`expected_version`、`actor`、`evidence_ref`、`decision`，以及确认已送达时必填的 `provider_message_id`。`expected_version` 来自异常查询，`actor` 为管理调用方声明的操作人，`evidence_ref` 指向外部核实记录。仅允许处置 `ambiguous` 或因 `reconcile_exhausted` 进入 `failed` 的分段。

| decision | 结果 |
|---|---|
| `delivered` | 有凭据确认已送达，记为 `sent`，重新调度原回复以继续剩余分段；已送达分段只 ACK |
| `not_delivered` | 确认未送达，记为 `failed`，关闭此异常，不触发补发 |
| `retry_not_delivered` | 有证据确认未送达并授权重试，将该分段置为 `retry_wait`，重新调度原回复；保留原 DeliveryKey、ClientRequestID、内容及累计尝试次数 |

Ledger 变更、`delivery_resolution` 审计插入和原 Outbox 重新调度在同一事务内提交，任一步失败全部回滚；新表由增量迁移 `000003_delivery_resolution` 创建。重新调度覆盖旧 Redis 事件已 ACK 的情况，不重新运行模型/工具。自动核对无能力或重试耗尽时会保留异常记录并输出投递错误日志；没有下游去重/查询能力时，接口仍需外部证据，不能由超时推断“未送达”。文本超过渠道限制时仍按持久化 segment 分段，承诺按最终结果及分段统计。

迁移 `000004_inflight_recovery` 增加 `execution_attempt`、加密的 `tool_execution_result`、真实业务表 `local_note` 和 `execution_record.model_request_count`。管理快照新增 `execution_attempts`、`model_calls`、`model_request_attempts`、`note_effects`；执行器正常返回与整个请求完成分别判断，HTTP 尝试次数也不代表上游接受次数。普通工具和确认工具均接入持久化恢复；未提供业务幂等/查询契约的工具在不确定恢复时拒绝盲目重放。

启用故障钩子时，`/test/failover/p2/*` 表示模型成功响应头返回后、读取结果前；`p4/*` 表示渠道返回 ProviderMessageID 后、Ledger sent 落盘前。`p2-commit/*`、`p4-before-send/*` 保留原边界，`tool-effect/*` 覆盖真实业务写入完成但工具结果未提交的窗口。arm 可带 `tenant_id`、`request_id` 筛选，status 回显命中身份及接受证据。真实 WeCom 暂无请求键查询适配器，内容级有限时去重不等价于永久请求级去重；其 P4 不承诺自动 exactly-once，取消和 HTTP 5xx 也会保守记为不确定投递。

`result-before-commit/*` 覆盖冻结答案已保存但 terminal/Outbox 尚未提交的窗口；接管读取并校验原答案后，直接续提交相同结果，不重新运行模型，也不重写官方 SDK 已保存的会话事件。
