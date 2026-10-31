# 在途任务接管与幂等投递

保护系统已经持久接收、尚未完成最终回复的同一原始输入。执行或发送节点故障后，用户不重发，由存活节点回收工作、复用结果或按协议安全重试。

## 当前实现状态与阅读顺序

正文按当前工作区的迁移000001–000004、运行时、入口与Compose配置编写。CURRENT-IMPLEMENTATION仅提供摘要；各篇正文直接给出当前机制，不通过“历史与基线冲突时覆盖”维持两套说法。

| 文件 | 层级 | 负责回答 | 读者与可跳过范围 |
|---|---|---|---|
| README.md | 导航 | 范围、当前状态、20角度索引 | 所有人先读。 |
| OVERVIEW.md | L1 | 场景、保证、术语、计数、边界 | 产品/管理/交接必读。 |
| FULL-GUIDE.md | L2 | 故障因果链、恢复与不能只做X | 架构与实现评审；非代码读者可只读边界。 |
| design.md | L2→L3 | 真实字段表、状态、并发和模块 | 实现者；非代码可跳。 |
| implementation-walkthrough.md | L3 | 运行顺序、durable truth与此刻被杀 | 对照实现；纯验收可跳。 |
| CODE-APPENDIX.md | L3 | 当前真实代码与状态解释 | 查不变量证据；非代码可跳。 |
| REFERENCE-IMPLEMENTATION.md | L3 | DDL原文、接口/算法、Redis、配置、启动和复刻前提 | 外部复刻/实现；概览读者可跳。 |
| testing-and-acceptance.md | L4 | 判定、证据、反例、脚本实际覆盖 | QA/验收。 |
| release-and-operations.md | L4 | 迁移、发布、告警口径与处置 | 实现/SRE/上线。 |
| CURRENT-IMPLEMENTATION.md | 摘要 | 已落地路径快速回顾 | 可选；不替代各层正文。 |

## 本能力已落地的模块

`000005_task_checkpoints` 增加业务计划与步骤级恢复：先定义步骤、依赖与完成条件，再绑定工具操作；新 owner 校验已有工具结果并补齐检查点，模型收到原计划、分析笔记与可验证结果，最终成功前校验全部声明步骤并封存计划。工具执行权仍由原恢复协议控制，步骤共享会话 lease，计划自身 fence/version 拒绝旧写。模型未声明的纯文本子目标不自动成为可验证义务；分析笔记摘要校验也不等于语义正确性证明。

详细阅读时，FULL-GUIDE 先定义输入、结果和交付三种“没有丢”，再列责任转移、工具六个检查点、恢复策略证明义务与分段发送边界；design 把每条不变量对应到写入条件，区分事务、版本、fence 和尝试号。

implementation-walkthrough 给出按 durable truth 选择恢复入口的实际顺序，并解释本次尝试在各类错误后由谁继续。testing-and-acceptance 为 P1–P4、工具效果、冻结答案和人工处置列前后证据；release-and-operations 提供从原 request 定位积压及 resolve 的完整操作单。

持久输入/dispatch、pending reclaim、执行尝试和模型HTTP独立计数、冻结答案续提交、allow/ask统一工具恢复与加密结果、幂等笔记写入、reply Outbox/Ledger和有证据的异常投递处置。

## 保证与边界

应用节点故障后已durable的工作由系统继续处理；模型/任务尝试可增加。内部terminal唯一与外部最终可见交付分开判断。真实WeCom P4缺少请求级查询/可靠去重时保持不确定异常，核对耗尽可进入failed/reconcile_exhausted，不盲目重发。外部工具须遵守恢复契约；宿主机、入口和共享依赖整体故障不属于双应用实例冗余。

## 五层20角度：为什么必须有、怎样问、去哪读

本目录侧重持久化任务、工具执行与外部发送边界；下表逐项追踪，同一问题在L1是承诺、L2是原因、L3是状态和条件、L4是证据。

| 角度 | 为什么必须有 | 提问式判据 | 文档位置 |
|---|---|---|---|
| A1 保证等级 | 把应用故障承诺扩展成所有系统故障保证。 | 在共享依赖正常、下游能力明确时保证哪些结果？ | [OVERVIEW.md](OVERVIEW.md) |
| A2 保护对象 | 遗漏原输入、会话或外部业务效果。 | 输入、上下文、工具效果和最终回复各由谁保护？ | [OVERVIEW.md](OVERVIEW.md) |
| A3 计数语义 | 把模型重试当成重复回复。 | Worker/模型HTTP/工具/业务效果/可见回复如何分别计数？ | [OVERVIEW.md](OVERVIEW.md) |
| A4 粒度和术语 | 把Bot、tenant、session、request混为一个身份。 | 一条原消息如何关联每段回复和每次尝试？ | [design.md](design.md) |
| A5 故障对象 | 把共享依赖或入口故障归入应用实例冗余。 | 应用、入口、宿主机、数据库和外部系统哪些假定正常？ | [FULL-GUIDE.md](FULL-GUIDE.md) |
| A6 故障窗口 | 用提交后的暂停点代替执行中故障。 | 每个窗口已durable的事实与接管动作是什么？ | [testing-and-acceptance.md](testing-and-acceptance.md) |
| A7 不可测与假设 | 从超时推断外部未接受。 | 何时无证据，如何保留ambiguous/effect_unknown？ | [FULL-GUIDE.md](FULL-GUIDE.md) |
| A8 失权与死亡判断 | 短暂抖动造成旧owner或后端被错误恢复。 | lease/claim何时到期，ready/健康何时撤销和重新纳入？ | [design.md](design.md) |
| A9 权威状态 | 把日志、临时目录或平台session_event当聊天历史。 | 恢复读哪些表/键，SDK历史与协调事务如何区分？ | [design.md](design.md) |
| A10 状态机 | 终态/可恢复状态被错误重开。 | queued/running/terminal、tool、Ledger及drain如何迁移？ | [design.md](design.md) |
| A11 顺序策略 | ACK、外部调用或发布早于必要状态保存。 | 每个durable写、claim、调用和ACK的前后关系是什么？ | [implementation-walkthrough.md](implementation-walkthrough.md) |
| A12 并发控制 | 旧Worker/sender覆盖新owner。 | version、fence、租期和入口epoch在哪个写点检查？ | [CODE-APPENDIX.md](CODE-APPENDIX.md) |
| A13 幂等边界 | 把Ledger唯一行外推为渠道接收一次。 | Inbox/Outbox/工具业务键/Ledger/provider各去重什么？ | [FULL-GUIDE.md](FULL-GUIDE.md) |
| A14 恢复动作 | 对未知副作用盲重放。 | 哪些复用结果、同键重试、查询、终结或人工处置？ | [implementation-walkthrough.md](implementation-walkthrough.md) |
| A15 租户隔离 | 依赖调用者记得带tenant。 | Binding、HMAC、SQL、SDK key、密钥和ReplyRoute如何约束？ | [design.md](design.md) |
| A16 身份授权 | 用Bot/WebUI凭据开放管理处置。 | 谁可验签、消费grant、领取工具、处置Ledger？ | [release-and-operations.md](release-and-operations.md) |
| A17 证据 | 只看服务ready就声称原任务完成。 | 哪组request/fence/terminal/Ledger/客户端证据组成闭环？ | [testing-and-acceptance.md](testing-and-acceptance.md) |
| A18 时间容量 | 把TTL当测得RTO，把低负载当容量承诺。 | 超时、轮询误差、重试和survivor负载如何预先设定？ | [release-and-operations.md](release-and-operations.md) |
| A19 不承诺 | 无限扩大HA、工具断点或交付保证。 | 哪些故障域和协议能力明确不能无条件保证？ | [OVERVIEW.md](OVERVIEW.md) |
| A20 变更演练 | 升级后破坏旧在途记录/密钥/格式。 | 迁移顺序、回滚、强杀、重新加入和反向故障怎样检查？ | [release-and-operations.md](release-and-operations.md) |

## 验证与结果边界

代码测试覆盖相关状态、竞态与恢复分支；脚本证明其明确列出的拓扑/本地交付行为。真实渠道、持续负载和实测RTO另需运行证据。assumed不是测得通过，recorded仅是外部证据已保存的声明；本文不记录未执行验收为成功。
