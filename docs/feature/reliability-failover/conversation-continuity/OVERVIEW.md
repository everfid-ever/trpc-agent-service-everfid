# 故障恢复后继续对话 — 全景导读

> **给接手者：** 本文说明“两个 Bot 在一个应用节点故障后仍能在原会话继续对话”要解决什么，**不依赖阅读代码**。如果你只想判断本能力是否成立、边界在哪、怎么验收，读这一篇即可。更深入的机制、数据模型与代码走读见 `FULL-GUIDE.md`、`design.md`、`CODE-APPENDIX.md`、`REFERENCE-IMPLEMENTATION.md`。

> **怎么读：** 本文 🟢 全文不含代码（唯一的方框是流程示意），可逐字读完。读不懂某个反引号名字时，查 §3 术语表——那些名字是系统内部账本与动作的代号，不需要编程背景。若只想了解结论，读 §0 一句话、§1 为什么不能只做 X、§5 核心规则、§6 范围与不包含即可。

---

## 0. 一句话

Bot A 属于 Tenant A，Bot B 属于 Tenant B。用户曾分别在两个原会话中输入不同代号；承载应用节点被强杀后，存活节点继续接收两个 Bot 的新消息，并从持久化会话中恢复正确代号和上下文，向正确的 Bot **只发送一条**最终回复，且租户之间不串扰。

---

## 1. 为什么不能只做“两个容器都启动”

以下任何一条单独成立，**都不能**证明“故障后继续对话”：

- **企业微信 callback 到达某个入口 ≠ 业务已可靠接收。** 入口只是转发；业务必须在到达后先写 `inbox`（durable）才算接收，否则进程在解析后、落库前崩溃会丢消息。
- **Redis Stream 能被其他 consumer reclaim ≠ 旧 Worker 覆盖新结果被挡住。** 网络隔离下旧 Worker 可能没死只是连不上协调服务；需要 lease + 递增 fence 双机制，数据库在提交时拒绝更小 fence 的迟到写。
- **进程内 memory 命中 ≠ 服务端 session 恢复。** 必须进程内缓存丢失后仍能答对代号，才证明是服务端 session 恢复，而非缓存碰巧还在。
- **发送 WeCom 回复也会失败。** terminal result、待发事件和投递状态必须可由其他节点恢复；不能“调了 API 就当成功”。
- **Docker 自动重启 ≠ 存活节点接管。** 本包 `restart: "no"`，明确要求强杀后**受害节点保持停止**；若靠 Docker 重启，验证的是“重启恢复”而非“另一节点接管”。

---

## 2. 参与组件（用大白话）

```text
企业微信 Bot A / Bot B
        │  callback（同一公开入口，仅 route key 不同）
        ▼
稳定入口（wecom-ha-entry）：只把请求转给“健康”节点
        ▼
可信入口（Ingress）：按 route key 找 binding → 验签 → 推出 tenant / agent / 配置
        ▼
Inbox（去重 + 持久化原始输入）  →  Dispatch Outbox（产生执行任务）
        ▼
Redis Work Stream  →  Worker N1 / N2（争抢会话 lease + fence）
        ▼
PostgreSQL：session / result / reply Outbox
        ▼
Reply Stream  →  Delivery Ledger  →  企业微信回复 API
```

各角色的“权威事实”位置：

| 参与者 | 职责 | **绝不能**作为唯一事实的内容 |
|---|---|---|
| 企业微信 | 原始消息、签名、最终显示回复 | 服务端是否已持久接收、是否已提交业务结果 |
| 稳定入口 | 把 callback 交给健康节点 | tenant 路由、消息去重、会话状态 |
| N1 / N2 | 验签、执行、转发、投递 | 会话历史、未完成任务、发送完成状态 |
| PostgreSQL | binding、Inbox、session、result、outbox、ledger | 高吞吐消费调度 |
| Redis | 任务传输、pending reclaim、lease/fence | tenant 或 terminal result 的最终裁决 |

分层决定了节点死亡后系统为什么仍能继续：**数据库保存业务事实，队列保存可重放工作，应用节点只保存暂态计算。**

---

## 3. 核心术语表

| 术语 | 含义 |
|---|---|
| 租户（Tenant） | 业务隔离单元，由验签后的 binding 唯一推导，不来自请求体 |
| Bot Binding | 企业微信应用（Corp+Agent）到 tenant / agent_app / 配置版本的映射 |
| 会话键（Session Key） | `(tenant_id, agent_app_id, session_id)` 三元组，会话持久化主键 |
| 四元组持久化链路 | tenant + Bot Binding(agent_app) + session_id + 消息 ID(external_message_id)，串起「路由→inbox→session→消息」 |
| lease | 带 TTL 的临时执行许可，防止同一会话并发执行 |
| fence | 每次成功领取 lease 时递增的单调计数器；数据库在提交时拒绝小于 `last_fence` 的写入 |
| terminal turn | 一轮对话已提交的终态结果（succeeded/denied/failed…） |
| 投递台账（Delivery Ledger） | 每条回复片段（segment）的发送状态机，保证只发一次 |
| 幂等键（idempotency_key） | 业务去重边界，重复调用收敛到同一结果 |
| 稳定入口（wecom-ha-entry） | 唯一暴露给外部 HTTPS tunnel 的代理，按 `/readyz` 轮转转发 |
| ProcessStartID | 每次进程启动随机生成的 8 字节 hex，保证重新加入不复用旧 owner |

---

## 4. 一次故障恢复的业务故事

1. **N1 单独运行**：Bot A 在 Tenant A 会话写入代号 `ALPHA` 并得到回复；Bot B 在 Tenant B 会话写入代号 `BETA` 并得到回复。两者各自进入持久化 `session_commit`。
2. **启动 N2**：两者共享 PostgreSQL、Redis 与版本化 Bot binding，但节点身份（InstanceID）与进程身份（ProcessStartID）不同。
3. **Bot A/B 再次在原会话提问**：系统分别从 Tenant A/B 的会话读取对应代号（ALPHA / BETA）。
4. **强杀实际承担入口、执行或投递职责的 N1**：`docker kill --signal=KILL`，且**保持其停止**（不自动重启）。
5. **N2 接管**：在 lease 到期 + consumer reclaim 周期后取得有效处理权，且 fence 比旧 N1 更大。
6. **两 Bot 发送不含代号的新消息**：系统将其路由至原 tenant/session，回复各自代号，并持久化唯一投递记录（每输入一条可见最终回复）。

关键点：**原会话历史在服务端，不在进程内存。** 即使 N1 内存全失，N2 从数据库读 `session_head` + `session_event` 即可恢复上下文。

---

## 5. 核心规则（给验收者）

| 规则 | 内容 |
|---|---|
| 可信租户 | tenant 只能由验签后的 binding 路由得出；请求体里的 tenant 字段不可信。 |
| 原会话 | 不更换 chat、不创建新 session 规避恢复问题。 |
| 有效 owner | 只有持有未过期 lease 且 fence 最新的 Worker 能提交 turn。 |
| 最终回复 | 每个输入只对应一条最终可见回复；流式更新 / 进度消息单独计数。 |
| 接管边界 | 不删除租约、不手工重新入队、不重启存活节点、不重启受害节点。 |

---

## 6. 范围与不包含

**本包覆盖：**
- 单应用节点的非优雅（SIGKILL）故障后，两个 Bot 在原会话继续正确对话。
- 租户归属不串扰、上下文不丢、最终回复每输入仅一条且可恢复发送。

**明确不包含（见相邻文档包或说明）：**
- 原消息已在处理中的 P1–P4 精确接管（在途任务）：见相邻 `inflight-task-takeover` 文档包。
- 同主机容器重新加入与反向故障：见相邻 `single-host-multicontainer` 文档包。
- 主机级断电、网络分区、跨主机 HA、**共享 PostgreSQL / Redis 故障**：本包不承诺（同为单一故障域）。
- 模型调用次数恒为 1：不承诺，P2 接管可重跑模型（见 `FULL-GUIDE.md` 计数区分）。
- 下游无法按 `client_request_id` 去重或查询时 P4 的绝对 exactly-once：诚实 `ambiguous`，不谎报成功。

---

## 7. 接下来读什么

- 想看机制与因果链 → `FULL-GUIDE.md`
- 想看架构图 / 数据模型 / 状态机 → `design.md`
- 想照着代码实现 → `implementation-walkthrough.md` + `CODE-APPENDIX.md`
- 想从零复现（DDL / Lua / 接口 / 算法 / 配置 / 启动顺序 / 验证清单）→ `REFERENCE-IMPLEMENTATION.md`
- 想知道怎么测、怎么判定通过 → `testing-and-acceptance.md`
- 想知道怎么上线、怎么排障 → `release-and-operations.md`
