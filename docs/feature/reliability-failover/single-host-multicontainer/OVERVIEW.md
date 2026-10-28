# 单主机多容器实例可用性 — 全景导读

## 场景与保护对象

保护同一主机内两个应用实例的共存、独立身份、强杀后的存活实例接管、重新加入和反向故障。两个实例共同使用一份数据库、队列和协调服务。

N1 与 N2 同版本运行。N1 被强杀并保持停止时 N2 服务两个租户；恢复 N1 后等待稳定，再强杀 N2，验证 N1 可以承担同样职责。计划内停止另验证有界排空。

## 术语与保证等级

instance_id 表示部署槽位；process_start_id 表示本次进程 incarnation；owner 是角色实际执行者；ready 表示当前依赖和组件可服务；入口健康是观测结果，不能代替任务终态。

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

## 把“两个容器”拆成四个问题

第一个问题是能否共存：端口、节点身份、临时目录和文件权限不能冲突。第二个问题是能否看到同一业务事实：两个实例要共享可并发读写的数据库与队列。第三个问题是执行权：同一个任务被两个进程观察到时，必须由lease/fence/条件更新决定谁能推进。第四个问题是用户入口：客户端原地址要能到达存活实例。

单纯复制容器只能解决进程数量；如果四个问题中任何一个没有处理，强杀后仍可能丢上下文、卡任务或重复发送。

## 日常状态与故障阶段

| 阶段 | 应用实例 | 入口 | 业务应如何表现 |
|---|---|---|---|
| 仅N1运行 | N1就绪，N2停止 | 至少N1可选 | 两Bot建立业务基线。 |
| N2加入 | 新身份、依赖及组件真实就绪 | 初次成功可纳入 | 两节点共享同一状态，无会话重建。 |
| N1强杀 | N1保持停止，N2原进程持续运行 | 失败/过期后不再选N1 | N2处理后续输入，并回收原有工作。 |
| N1重新加入 | 新process_start_id | 连续成功达到阈值后纳入 | 不争抢有效lease、不重发已sent分段。 |
| N2反向强杀 | N1承担职责 | N2摘除 | 证明接管能力不依赖固定节点名称。 |
| 计划内停止 | 停准入并排空 | 应用ready撤销 | 已接纳工作有机会完成，再退出。 |

表格描述预期行为；实际耗时必须记录，不是预先给出的通过结论。

## 稳定不等于所有实例始终平均忙碌

队列与租约竞争不要求每条消息平均分配，也不要求两个实例同时持有相同职责。某轮任务集中在一个实例并不单独说明失效；应检查是否存在饥饿、持续积压以及故障后能否由另一个实例推进。

入口的轮转只分配新的HTTP回调，不直接分配已入库任务。哪个节点接收回调、哪个节点执行、哪个节点投递可以不同，观测时要分别记录。

## 共享内容和私有内容

共享技能包是发布好的不可变文件，两个节点只读加载。临时文件和密钥投影是每容器私有数据；它们随容器退出消失不会丢失会话或任务真相。

“共享一个目录”不能普遍解决恢复：不支持并发的本地状态文件可能被两个实例写坏。业务事实放在数据库，协调放在Redis/数据库协议，只有经过发布约束的内容才以共享文件形式读取。

## 单主机冗余的适用边界

本能力隔离单个应用进程或容器故障。整机网络、磁盘、内核或运行时失效可能同时影响所有实例与共享依赖；独立入口也没有第二个副本。上线容量还要求一台存活应用实例能承担预定流量，这需要单独测量，不能从两容器健康推导。
