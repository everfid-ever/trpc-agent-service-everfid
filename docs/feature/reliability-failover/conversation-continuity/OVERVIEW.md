# 故障恢复后继续对话 — 全景导读

## 场景与保护对象

保护两个独立 Bot 的租户归属、原会话上下文、请求身份与回复目的地。故障发生在一轮已完成之后，再发送新消息验证会话连续性；处理中的旧消息另按在途接管判定。

Bot A 只记住代号 A，Bot B 只记住代号 B。持有应用职责的节点被 SIGKILL 且保持停止后，存活节点应在相同 Bot 和原会话中回答各自代号；不重新绑定、不更换会话、不提示代号。

## 术语与保证等级

Bot Binding 是经过验签的渠道配置；tenant 是强制业务边界；agent app 是租户内应用；session 是持久化会话；external_message_id 是渠道消息身份；request_id 是内部原始任务。

本能力假定共享依赖正常、配置/密钥版本一致、存活实例能承担流量；真实渠道已持久接收前丢失的输入不属于 durable 接管承诺。当前 WeCom 使用 HTTP callback，不存在迁移 Bot 长连接的实现。连接恢复在这里指稳定入口可把回调转发至 ready 节点，Worker 的执行权另由 lease/fence 控制。

内部保证与外部交付分别判定：同一输入只有一个有效 terminal commit；每个回复 segment 在 Ledger 中只有一行权威状态，`sent` 后自动路径只跳过发送。该行唯一不等于渠道只接收一次。下游已接受、本地确认未提交的 P4 窗口需要下游可靠去重或查询才能自动收敛。当前 WeCom 没有请求键查询适配器，内容级有限时间去重不足以提供永久请求级幂等；取消、HTTP 5xx 和响应丢失均可能留下不确定结果。系统不盲目补发，先保留 `ambiguous`；核对次数耗尽可转为 `failed/reconcile_exhausted`，仍需运维查证。

## 三类数字及业务效果

| 计数 | 权威记录 | 解释 |
|---|---|---|
| Worker 尝试 | `execution_attempt` 的行数 / `attempt_no` | 每次进入执行器一行；新尝试把旧 `running` 标为 `superseded`。completed 仅表示执行器正常返回。 |
| 逻辑模型调用 | `execution_record.model_call_count` | 进入模型适配器的逻辑调用，可因恢复增加。 |
| 模型 HTTP 尝试 | `execution_record.model_request_count` | transport 尝试，含连接失败及内部重试；不等于接受数或计费数。 |
| 工具尝试 / 业务效果 | `tool_execution.attempt` / 外部业务幂等记录 | 同键工具重试可大于一次，业务效果应按工具契约去重。 |
| 回复发送 / 用户可见答案 | Ledger `attempt`、`segment_no/count` / 客户端证据 | 一条答案可有多个分段；唯一 Ledger 行不能单独证明用户只见一次。 |

## 当前实现组成

| 能力组成 | 当前实现 |
|---|---|
| 身份与上下文 | 已验签 Binding → tenant scoped identity/session → Inbox/payload/冻结 ReplyRoute；SDK PostgreSQL session_events 保存历史。 |
| 任务与工具 | Redis pending reclaim、会话 lease/fence、execution_attempt；工具执行账本、加密结果和恢复契约。 |
| 结果与投递 | 冻结 result_payload → terminal commit/Outbox 同事务 → Relay → 每 segment Ledger。 |
| 实例与入口 | 进程启动身份、并行健康探测、过期摘除与连续成功重加入、有界排空、只读技能包和私有 /tmp。 |

## 不承诺的范围

不恢复模型 provider 内部生成或工具函数内存断点；不保证任意外部工具自动幂等；manual 工具可能以 tool_effect_unknown 安全终结。单主机故障、Docker daemon、共享磁盘/数据库/Redis、稳定入口自身和外部渠道整体故障不因双应用实例获得冗余。容量、真实渠道响应时间及 RTO 必须通过对应环境测量。

## 阅读入口

机制与取舍见 [FULL-GUIDE.md](FULL-GUIDE.md)，字段/状态与模块见 [design.md](design.md)，运行顺序见 [implementation-walkthrough.md](implementation-walkthrough.md)。[CODE-APPENDIX.md](CODE-APPENDIX.md) 提供当前代码， [REFERENCE-IMPLEMENTATION.md](REFERENCE-IMPLEMENTATION.md) 提供数据、契约与配置；[testing-and-acceptance.md](testing-and-acceptance.md) 和 [release-and-operations.md](release-and-operations.md) 给证据及处置。CURRENT-IMPLEMENTATION 是阅读摘要，正文均按当前实现编写。

## 用一段业务故事理解三个阶段

第一阶段是“记住”。同一企业微信用户分别告诉Bot A和Bot B不同的测试代号。系统持久化的是各自租户中的会话事件；两个Bot可以调用同一个模型服务，但模型服务的账号不是本系统隔离上下文的依据。

第二阶段是“节点退出”。N1被强杀后，N1的内存、SDK调用栈、临时文件和连接均不可依赖。原消息、会话历史、提交记录和回复路由仍在共享存储中。N2不需要复制N1内存，它取得自己的执行权后重新读取这些事实。

第三阶段是“在原会话继续”。用户向同一Bot发送不含代号的新问题。新的输入仍映射到原tenant/app/user/session；SDK加载原历史，模型才有条件回答正确代号。若用户进入了另一个Bot，或部署改变了身份派生密钥，不能把那条新会话的回答当作原会话恢复。

| 用户动作 | 平台应该保持什么 | 需要观察什么 |
|---|---|---|
| 再次向Bot A发消息 | tenant A、app A、原session A | 服务端身份链及A的上下文。 |
| 同时向Bot B发消息 | tenant B、app B、原session B | B的上下文与回复目的地。 |
| 模型尝试增加 | 原request不变、仅一个有效终态 | 尝试日志和terminal记录。 |
| 节点重新加入 | 新process_start_id、原业务身份不变 | owner变化，但session不迁移到新ID。 |

## 为什么“回忆正确”和“没有串租户”要分开

模型答对代号只能证明这次输出包含正确内容，还不能证明查询和路由没有访问另一租户。完整的隔离检查需要看到输入Binding属于A、会话读取属于A、结果密钥属于A、回复目的地也属于A，并确认没有B内容。

反过来，服务端记录全部属于A，也不能保证模型每次都正确回忆长历史。上下文窗口、摘要与模型能力会影响答案质量。这里的恢复能力确保正确的持久历史可以被同一会话读取，不把模型对任意历史细节的记忆准确率当成无限保证。

## 什么是系统内部完成，什么是用户完成

内部terminal表示这一轮已有权威业务结果，Outbox表示需要继续投递。用户完成还要求渠道接受和客户端实际显示最终回复。两者之间的异步窗口允许应用节点更换。

因此故障后可能出现“任务已经成功、回复仍在发送”的短暂状态。此时应继续发送保存的答案；重新执行模型可能产生另一个答案，反而破坏结果一致性。若已经进入P4不确定窗口，安全策略是保留证据并核实外部状态。

## 与另外两项能力的边界

本篇的核心问题是原会话是否还在、路由是否正确。在途接管回答旧输入是否无需重发继续完成，多容器HA回答两个实例如何共存、摘除、重新加入。一次演练可以同时观察三者，但三个结论不能相互代替。
