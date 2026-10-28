# 在途任务接管与幂等投递 — 全景导读

## 场景与保护对象

保护系统已经持久接收、尚未完成最终回复的同一原始输入。执行或发送节点故障后，用户不重发，由存活节点回收工作、复用结果或按协议安全重试。

一条消息已入库，模型/工具正在执行，节点突然退出。新节点必须处理该 request_id，不能用后来新消息的成功替代旧任务完成。模型尝试可以增加；业务效果和最终交付分别核对。

## 术语与保证等级

P1 是持久接收至执行前；P2 是模型/工具执行中；P3 是结果已提交而发送未完成；P4 是下游已接受、本地确认未落盘。attempt、fence、outcome 与可见回复是不同维度。

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

## 一个长程任务如何经历多个owner

假设用户要求“先查询资料，再创建一条笔记，最后总结”。这不是一个能在节点间搬移的进程。系统保存原始输入、工具逻辑操作、必要结果和最终回复，节点负责推进这些记录。

N1在模型调用中退出时，N2可能重新调用模型；N1在笔记写入后、工具结果提交前退出时，N2通过相同业务键恢复笔记效果；N1在最终答案冻结后退出时，N2继续提交保存的答案。三个窗口的动作不同，不能统一称为“整条任务重跑”。

| 已留下的事实 | 下一节点可以做什么 | 不应该做什么 |
|---|---|---|
| 只有持久输入 | 自动调度原输入。 | 要求用户再发一遍来触发恢复。 |
| 工具正在执行 | 等工具租约失效后按恢复契约行动。 | 假装知道外部系统有没有执行成功。 |
| 工具结果已加密保存 | 校验并复用结果。 | 因新模型call ID而重复创建业务对象。 |
| 最终答案已冻结 | 续提交该答案。 | 生成另一个竞争答案。 |
| terminal与reply Outbox已提交 | 只恢复发布和投递。 | 再次调用工具或模型。 |
| 下游已接受、本地未确认 | 查询/去重或异常处置。 | 把超时当作“肯定没收到”。 |

## 两种“继续”的区别

协议级继续是读取保存结果、查询外部操作或使用同一业务键重试。本实现已为工具建立这套协议。内存级继续则是恢复某个函数的指令位置、局部变量和网络调用栈，本实现没有承诺。

例如一个外部报表任务已有可查询的任务编号，queryable工具可恢复查询并获得结果；一个工具只是本地函数中循环了几分钟、没有外部查询或分步持久状态，节点被杀后无法从那个循环位置继续。是否安全重放取决于工具声明，而不是任务运行时间长短。

## 授权策略和恢复策略是两个问题

allow表示该次工具调用不需要额外人工确认，ask表示需要一次性授权。两者都可能是读操作或写操作，都需要明确故障后的处理方式。

恢复策略另行声明：replay_safe说明重放安全，idempotent_key说明同键折叠效果，queryable说明可以确认外部状态，manual说明无法自动安全判定。把工具改成ask不会自动使它幂等；用户确认过也不代表节点恢复后可以重复执行任意写操作。

## 什么结果可以接受

任务恢复目标首先是保护原输入和一致性。存在安全自动恢复契约时应继续完成；不确定工具只能安全终结或等待处置时，不能谎报成功。最终业务是否完成与“系统安全保存了失败原因”应分别报告。

对P4同样如此：正确进入ambiguous说明没有盲重发，但并不等于严格的“无需人工介入且一次最终可见回复”已经完成。这一边界来自跨系统接受与本地确认之间缺少原子协议。
