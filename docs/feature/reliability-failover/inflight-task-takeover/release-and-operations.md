# 在途任务接管与幂等投递 — 发布与运维

> 本文覆盖：运行参数与配置、部署拓扑与发布门禁、观测指标与告警、容量与背压、时间口径、回滚（约束 + 检查清单 + 回滚后验证）、分场景故障处置手册、对账手册、变更评估清单、边界声明与组合演练。
> 复现材料见 [REFERENCE-IMPLEMENTATION.md](./REFERENCE-IMPLEMENTATION.md)；验收见 [testing-and-acceptance.md](./testing-and-acceptance.md)。

---

## 一、运行参数与配置

### 1.1 关键参数（区分"装配值"与"代码级兜底默认值"）

`webui-local` 角色会**显式传入**下表的装配值，这才是部署实际生效的参数；组件内部的兜底默认仅在调用方未设置时生效。**告警阈值与时限结论必须引用装配值**——两者差一个数量级会导致完全错误的 RTO 结论。

| 参数 | 位置 | 装配值（部署生效） | 代码级兜底（≤0 时） |
|---|---|---|---|
| `LeaseTTL` | worker consumer | 30s | 5s |
| `RenewInterval` | worker consumer | 10s | `LeaseTTL/3` |
| `RetryWait` | worker consumer | 250ms | 10ms |
| `ReclaimInterval` | worker consumer | 5s | 1s |
| `ReclaimLimit` | worker consumer | 100 | 100 |
| `DrainTimeout` | worker consumer | 30s | 30s |
| `ClaimTTL` | delivery | 30s | 30s |
| `ClaimRenewInterval` | delivery | 10s | `claimTTL/3` |
| `MaxAttempts` | delivery | 8 | 8 |
| `MaxReconcileAttempts` | delivery | 8 | 8 |
| `DefaultRetryDelay` | delivery | 1s | 1s |
| `MaxRetryDelay` | delivery | 1min | 1min |
| 退避 | delivery `backoff` | `1<<(attempt-1)`，上限 `MaxRetryDelay` | 同左 |
| `ReclaimInterval` / `ReclaimLimit` | delivery consumer | 5s / 100 | 同左 |
| `LeaseTTL` / `RetryDelay` / `MaxAttempts` | preprocess | 30s / 1s / 8 | 30s / 1s / 8 |

**取值纪律：** `lease_ttl > renew_interval`；`claim_ttl > claim_renew_interval`；`reclaim_idle`、`retry_delay`、`max_attempts`、`T_TASK` / `T_DELIVER` / `W_DUPLICATE` 必须在本轮执行前固定。🔴 **不可在超时后修改阈值，把失败重判为通过。**

### 1.2 必设环境变量（生产）

```bash
TRPC_WEBUI_LOCAL_INSTANCE_ID=wecom-ha-node-a   # N2 必须不同
TRPC_POSTGRES_DSN=...                          # 两节点必须相同
TRPC_REDIS_ADDRESS=redis:6379                  # 两节点必须相同
TRPC_LISTEN_ADDRESS=:8080
TRPC_WECOM_LOCAL_ENABLED=true
TRPC_WECOM_SECONDARY_LOCAL_ENABLED=true        # (Corp ID, Agent ID) 必须不同
TRPC_WECOM_HA_ENTRY_BACKENDS=http://wecom-ha-node-a:8080,http://wecom-ha-node-b:8080
TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL=1s
# 生产默认绝对不设置：
# TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true
```

### 1.3 测试控制面（仅隔离 Compose 使用）

`TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED=true` 才会注册 process-local 的 P1–P4 控制面：不写入业务数据库、不续租、不重投消息、不修改 Ledger。实例被终止后，接管只由已有的 pending reclaim、lease TTL 或 delivery claim TTL 完成。

约束：

- 🔴 生产部署**必须不设置**该开关；一旦误设，移除配置并**重启**相关实例以清除内存 controller。
- 🔴 不得把 `/test/failover/{p1|p2|p3|p4}/{arm,status,release}` 经公网入口暴露；每个操作均要求正确 `X-TRPC-Local-Token`，无 token 返回 403。
- 标准接管演练命中后直接杀死 owner；不对 survivor 调 release。release 不是业务重试或租约变更。
- 演练脚本退出即销毁其自创容器与卷；失败日志保留到临时目录。

---

## 二、部署拓扑与发布门禁

### 2.1 编排事实（profile `wecom-ha-local`）

| 服务 | command | restart | 宿主端口 | 依赖 |
|---|---|---|---|---|
| `wecom-ha-bootstrap` | `webui-local-bootstrap` | `no` | — | postgres / redis / qdrant healthy |
| `wecom-ha-node-a` | `wecom-local` | `no` | `${TRPC_LOCAL_WECOM_HA_NODE_A_PORT:-58088}:8080` | bootstrap(completed_successfully), clamav, otel-collector |
| `wecom-ha-node-b` | `wecom-local` | `no` | `${TRPC_LOCAL_WECOM_HA_NODE_B_PORT:-58089}:8080` | 同上 |
| `wecom-ha-entry` | `wecom-ha-entry` | `no` | `${TRPC_LOCAL_WECOM_HA_ENTRY_PORT:-58087}:8080` | 两 node + 依赖 |

- `wecom-ha-entry` 是**唯一允许暴露给 HTTPS tunnel / 企业微信控制台**的端口；两个 Bot 回调共用同一 public origin，只用 durable route key 区分 `local-wecom` 与 `local-wecom-secondary`。
- 两节点共享 `TRPC_POSTGRES_DSN` / `TRPC_REDIS_ADDRESS` / `TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS`；与 PostgreSQL、Redis、Qdrant、ClamAV、宿主机同属**单一故障域**（见 §十）。
- `restart: "no"` 是接管验收的前提：强杀后 Docker 不得暗中重建受害实例，否则"存活实例接管"不可证。

### 2.2 发布门禁顺序

1. **共享依赖先行**：postgres / redis / qdrant / clamav / otel-collector healthy 后再启动应用（`depends_on` 已声明）。
2. **一次性 bootstrap**：`wecom-ha-bootstrap` 必须 `service_completed_successfully` 后节点才可启动；重复运行幂等。
3. **先发布能识别新状态的 consumer**（preprocess / worker / delivery），再发布 producer。理由：旧的 consumer 遇到未知 envelope 字段或未知 delivery 状态必须能安全忽略/遍历，而不是 panic 或把 `ambiguous` 误判为 `failed`。
4. **P1–P3 集成验证通过 + P4 的 Provider 能力结论已记录**（下游是否支持 `client_request_id` 去重/查询），才允许把自动重试策略用于真实写工具。
5. 两节点并行运行并通过稳定基线后，才允许扩大流量或启用第二个 Bot。
6. `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED` 仅用于隔离的 `webui-multinode` 测试 Compose，**不得混入生产发布**。

---

## 三、观测指标与告警

### 3.1 指标与精确口径

| 指标 | 计算方式（口径） | 维度 |
|---|---|---|
| `inbox_durable_failure_rate` | `inbox` 写入失败数 / 总回调数 | tenant |
| `dispatch_outbox_oldest_age` | `max(now - created_at)` where `outbox.state='pending'` | tenant、kind |
| `reply_outbox_oldest_age` | 同上，`kind='reply'` | tenant |
| `redis_pending_oldest_idle` | consumer group pending 中 `max(now - idle_since)` | consumer group |
| `lease_lost_total` | `renew` 失败次数累加 | worker |
| `stale_fence_total` | `commit_turn` 返回 SQLSTATE `40001` 且消息含 `stale fence` 的次数 | session |
| `terminal_commit_conflict_total` | `ErrCommitConflict` / `ErrVersionConflict` 次数 | session |
| `delivery_state_dist` | `count(*)` group by `delivery_ledger.state` | tenant |
| `provider_error_total` | adapter 返回 429 / 5xx 次数 | provider、channel |
| `final_duplicate_total` | 同 `(delivery_key, segment_no)` 出现 > 1 行 `sent` | delivery_key |
| `external_tool_ambiguous_total` | 副作用进入未决对账状态的数量 | tool |
| `idempotency_key_collision_total` | 同一幂等键被重复写入的计数 | kind |

### 3.2 告警规则

| 优先级 | 条件 | 含义与首查动作 |
|---|---|---|
| 🔴 P0 | 同一 `(delivery_key, segment_no)` 出现两条 `sent` | I6 失守：检查 `ClaimDelivery` / `FinishDelivery` 的条件更新是否被削弱（`state='sending' AND version=? AND claim_owner=? AND client_request_id=?` 缺项即可能双发） |
| 🔴 P0 | `stale fence` 计数突增且伴随旧 owner 提交**成功** | fence 机制失效：检查 `commit_turn` 的 `p_fence < last_fence` 拒绝是否仍在 DB 层执行、错误消息是否仍含 `stale fence` 字样 |
| 🟠 P1 | `delivery_ledger.state='ambiguous'` 出现并持续 | P4 诚实边界状态，**不代表 bug**，但不许静默。触发对账或人工处置（见 §七） |
| 🟠 P1 | `reconcile_attempt` 达 `MaxReconcileAttempts` 转 `failed` | 下游不可对账，需人工核对下游实际状态 |
| 🟠 P1 | `dispatch_outbox_oldest_age` / `reply_outbox_oldest_age` 超过 `T_DELIVER` | 重放积压：检查 relay 是否在扫描、claim 是否过期未回收 |
| 🟠 P1 | `redis_pending_oldest_idle` 持续增长 | reclaim 未及时生效：检查消费循环是否存活、`ReclaimInterval` 是否被调大 |
| 🟠 P1 | `lease_lost_total` 突增 | 大量会话失去执行权：检查 Redis 抖动、续租线程是否被阻塞、`LeaseTTL` 是否过小 |
| 🟡 P2 | `terminal_commit_conflict_total` 升高 | 并发提交竞争；若伴随积压需确认是否会收敛 |
| 🟡 P2 | `external_tool_ambiguous_total` 非零 | 副作用未决：按业务幂等键对账 |

---

## 四、容量与背压

| 项 | 建议 / 事实 | 说明 |
|---|---|---|
| worker `ReclaimLimit` | 100 | 单次 reclaim 最多 100 条，避免单批饥饿；积压时**先查 `redis_pending_oldest_idle`**，而不是直接扩并发 |
| delivery 重试窗口 | 最坏约 `1+2+4+…+128s ≈ 255s`（`MaxAttempts=8` + 指数退避 + `MaxRetryDelay=1min` 上限） | `T_DELIVER` 应 ≥ 此值再加对账余量 |
| `park_execution` `p_max_attempts` | ≤ 64 | 输入停放上限；超时转 `blocked`，需人工/自动释放，**不应无限重试** |
| 单实例容量 | 应 ≥ 全量峰值（而非峰值 / 2） | P2 接管会重跑模型，接管窗口内模型调用量高于稳态，需额外预留 |
| PostgreSQL 连接池 | 覆盖"两节点 + 接管期额外续租/提交" | 续租是高频小事务，池耗尽会先表现为 `lease_lost` 升高 |
| Redis 内存 / fd | 覆盖 stream pending 峰值 + 每会话 1 lease key + 1 fence key | fd 耗尽早表现为消费停滞 |

> ⚠️ 共享依赖（PG / Redis）是单一故障域：**扩容节点不提升可用性**，只提升吞吐与接管速度。低流量演练只证明可用性，绝不可外推为容量承诺。

---

## 五、时间口径（不得宣称更精确）

| 时刻 | 含义 | 不代表 |
|---|---|---|
| `t0` | 外部确认受害节点被终止 | 业务已恢复 |
| `t_connect` | 入口 / 连接所有者恢复 | worker 一定能提交 |
| `t_worker` | 执行者可处理或回收任务 | 用户已看到回复 |
| `t_ready` | 业务链路整体 ready | 精确端到端 RTO |
| 收到 → 最终可见 | 单请求真实响应时间 | 故障期间未接收消息的恢复时间 |

采样间隔 `DELTA_POLL` 必须随结论记录，**不得宣称比采样更精确的 RTO**。

---

## 六、回滚

### 6.1 回滚约束（禁止操作）

| 禁止操作 | 为什么 |
|---|---|
| 删除 pending stream | 未 ACK 的在途任务永久丢失（本应由 reclaim 接管） |
| 手工改 `session_head.next_input_seq` | 与终态 `session_commit` 唯一索引冲突，或让已终态输入被再次执行 |
| 手工改 `delivery_ledger` 状态（尤其把 `sent` 改回 `pending`） | 直接造成重复发送，破坏"每输入一次可见最终回复" |
| 把 `ambiguous` 直接改成 `sent` | 谎报投递成功，掩盖可能的重复或丢失 |
| 手工置 `execution_record.outcome` 为终态 | 绕过 `commit_turn`，破坏"一次有效提交" |
| 删除 lease / 跳过 tenant predicate / 人工补发回复 | 破坏在途接管证据，且可能串租户 |
| 为"清积压"而手工重新入队 | 证明的是人工修复，不是自动接管 |

> 回滚关注的是**生产代码路径**；barrier 是测试控制面，不影响线上，无需为它设计回滚。

### 6.2 回滚检查清单

- [ ] 确认现存 `pending` 的 in-flight task 会被新版本正确 `Reclaim`（CAS 语义未被破坏性变更）；
- [ ] 若新版本改了 `commit_turn` / `prepare_dispatch` / `claim_inbox` / `ClaimDelivery` 的 CAS 条件，**先排空** `sending` / `pending` 再切版本；
- [ ] 确认 `delivery_ledger` 无残留 `sending` 超过 `claim_until`（否则会被回收为 `ambiguous` 后由新版本重 claim）；
- [ ] 若生产误设 `TRPC_INFLIGHT_TEST_*`，已从配置移除并重启相关实例；
- [ ] 保留 `run_id`、版本、owner 变化与 delivery 状态轨迹供复盘。

### 6.3 回滚后验证

- [ ] 旧版本 consumer 能正常 `Reclaim` 现存 pending；
- [ ] `/readyz` 覆盖 db + redis + malware 全部返回 200；
- [ ] `final_duplicate_total` 无告警（同一 segment 无两条 `sent`）；
- [ ] `delivery_state_dist` 中无长期 `sending`；
- [ ] `TRPC_WEBUI_LOCAL_FAILOVER_TEST_ENABLED` 未设置；生产角色不会注册 failover test endpoint。

---

## 七、故障处置手册

> 所有处置的前提：**不删除租约、不手工重新入队、不手工改状态、不补发回复**。任何"手工修好"都不是自动接管。

### 7.1 P1 场景：预处理 / dispatch 卡住

**现象：** `preprocess_job` 大量 `pending` / `ready` 但 `dispatched_at IS NULL`；`inbox` 堆积 `dispatch_pending`。

**处置：**

1. 确认 preprocess 与 dispatch-relay 进程存活、`/readyz` 200；
2. 检查 `claim_inbox` 唯一键冲突日志（SQLSTATE `23505`）——属正常去重，不应被当作错误；
3. 检查 `prepare_dispatch` 是否因 tenant 非 active / 版本不匹配（`40001`）把 `inbox` 置为 `terminal`，并确认该判定符合预期；
4. 🔴 **不得手工重新入队**；让存活节点的 `ClaimReadyForDispatch` 自然接力。

### 7.2 P2 场景：worker 执行中丢失 / 双提交风险

**现象：** `execution_record.outcome='running'` 长期不变；Redis lease 过期；可能出现 `stale fence` 日志。

**处置：**

1. 受害节点为 `restart:"no"`，不会自动复活；存活节点会 `Reclaim` 同一 entry；
2. 观察 `session_head.last_fence` 是否**单调推进**；`stale fence` 计数随存活节点提交而增长属**正常拦截**，不是故障；
3. 若 `park_execution` 把输入转 `pending` / `blocked`，按 `park_attempts_exhausted` / `park_deadline_exceeded` 处置（检查 `not_before` / `park_deadline`，必要时人工释放）；
4. 🔴 严禁手工置 `outcome` 为终态——必须让 `commit_turn` 走正常路径。

### 7.3 P3 场景：结果已提交但回复未发

**现象：** `session_commit` 有终态、`reply` outbox 为 `pending`；用户无最终回复。

**处置：**

1. 确认 relay 在扫描 `reply` outbox（索引 `outbox_claim_idx` 命中）、claim 未过期；
2. delivery 消费后 `ClaimDelivery` 应成功；`result_payload` 已存在 → **不重跑模型**；
3. 若 delivery 进程死亡，存活节点重新 claim；超期 `sending` claim 会先被改写为 `ambiguous` / `owner_lost` 再重 claim；
4. 检查是否卡在 `retry_wait` / `ambiguous`（见 §7.4）。

### 7.4 P4 场景：下游已接受、本地未确认

**现象：** `delivery_ledger.state='ambiguous'` 或 `sending` 长时间无 `sent`；用户可能重复收到或收不到。

**处置（务必克制）：**

1. 若 adapter 实现了 `DeliveryReconciler`：触发 `ReconcileDelivery` —— `ReconciliationDelivered` → `sent`；`ReconciliationNotDelivered` → `retry_wait` + `last_error_class='reconciled_not_delivered'`；`ReconciliationUnknown` → 继续 defer；
2. 若下游**不支持**去重/查询：保留 `ambiguous` + 告警，按**至少一次**与业务对账；🔴 **不得盲重发**；
3. `ProviderMessageID == ""` 一律 `ambiguous` / `missing_provider_message_id`，**不得伪造 receipt**；
4. `reconcile_attempt` 达 `MaxReconcileAttempts` → `failed` / `reconcile_exhausted`，需人工核对下游实际状态。

---

## 八、对账手册

定期运行 `FindReconciliationIssues(before, limit)` 并按下表处置：

| kind | 含义 | 处置 |
|---|---|---|
| `stuck_inbox` | `inbox` 卡在 `preprocess_pending` / `dispatch_pending` 且 `updated_at < before` | 检查预处理与 dispatch 链路（见 §7.1） |
| `expired_outbox_claim` | `outbox` 为 `claimed` 且 `claim_until < before` | 清理孤儿 claim，回到 `pending` 由 relay 重发 |
| `parked_input` | `execution_record` 为 `pending` 且 `not_before < before` | 重新触发 park 续处理 |
| `missing_reply_outbox` | 存在终态 `session_commit` 但无对应 `reply` outbox | 按 `idempotency_key` 收敛补发（会与既有唯一约束对齐，不会重复） |
| `stuck_delivery` | `delivery_ledger` 为 `sending` 且 `updated_at < before` | 回收为 `ambiguous` 后重 claim |
| `ambiguous_delivery` | `delivery_ledger` 为 `ambiguous` 且 `updated_at < before` | 触发对账或人工（见 §7.4） |

> 所有处置**不得破坏在途接管证据**；任何补发必须走既有幂等键（`outbox` 幂等键 / `client_request_id` / `(delivery_key, segment_no)`）。

---

## 九、变更评估清单（上线前自审）

- [ ] 是否改了 `commit_turn` / `prepare_dispatch` / `claim_inbox` 的 CAS 条件？若是，须先排空相关 `sending` / `pending` 再切版本，并重做 P1–P3 验证。
- [ ] 是否改了 `ClaimDelivery` / `FinishDelivery` 的 `owner` / `version` / `state` / `client_request_id` 条件？若是，必须验证不双发（I6）。
- [ ] 是否改了 `commit_turn` 的 `p_fence < last_fence` 判断或其 `stale fence` 错误消息？若是，🔴 必须重做全套接管验收（该字样是 fence 与普通乐观锁的唯一区分信号）。
- [ ] 是否引入新的外部写工具？若是，必须先定义业务幂等键与对账路径，禁止自动盲重试。
- [ ] 是否改了 barrier 启用契约？若是，确认生产默认不设置 `TRPC_INFLIGHT_TEST_*`，且 release 端点不经公网暴露。
- [ ] 是否调整了 `LeaseTTL` / `ReclaimInterval` / `ClaimTTL` / `MaxAttempts`？若是，重新核算 `T_TASK` / `T_DELIVER` 阈值并记录。
- [ ] 是否新增 delivery 状态或 outbox kind？若是，必须先发布"能安全遍历未知状态"的 consumer（两阶段发布）。

---

## 十、边界与不承诺

- ❌ 不承诺跨主机 / 整机断电 / Docker daemon / 共享磁盘 / 共享 PostgreSQL·Redis 故障下的可用性（同为单一故障域）。
- ❌ 不承诺模型调用次数恒为 1；P2 接管**会重跑模型**。
- ❌ 不承诺下游无法按 `client_request_id` 去重或查询时 P4 的绝对 exactly-once。
- ❌ 不把 Docker 自动重启（`restart:"no"` 已禁用）当作存活节点接管。
- ❌ 不把健康恢复 / 连接恢复 / 队列恢复单独当作完整业务 RTO。

---

## 十一、与相邻包的组合演练

本包可与另两个能力包组合，顺序建议如下：

1. 先跑 `single-host-multicontainer`，验证节点可重新加入且进程身份不复用（I9）；
2. 再跑本包 P1–P4，验证**原在途消息**自动接管（重点是 `request_id` 不变）；
3. 最后跑 `conversation-continuity`，验证接管后**原会话**语义连续、租户不串。

三者独立成立、组合不冲突，且都依赖同一共享依赖（PostgreSQL / Redis），同属单一故障域，不承诺跨主机可用性。

> ⚠️ 不要因为"后一条新消息成功"就判定原在途任务已被接管——这是两个不同的证据面。

## 当前实现增量：工具恢复处置

监控 `tool_execution` 的过期 `running`、`effect_unknown`、attempt/fence 增长和 `manual recovery required` 错误。对 `queryable`/`idempotent_key` 工具，优先让 lease 到期后的接管完成；对 `manual` 工具，不要人工重复调用外部系统，应依据 provider 侧证据决定后续业务补偿。发布顺序必须先应用迁移 `000002_tool_execution_recovery`，再部署会写入该表的应用版本。
