# 单主机多容器实例可用性

保护同一主机内两个应用实例的共存、独立身份、强杀后的存活实例接管、重新加入和反向故障。两个实例共同使用一份数据库、队列和协调服务。

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

同机双应用共享PG/Redis、独立槽位/启动身份和端口、稳定callback入口并行probe/epoch/健康过期/重加入窗口、只读root/skills和私有tmpfs、有界drain、强杀重加入/反向故障脚本。

## 保证与边界

应用节点故障后已durable的工作由系统继续处理；模型/任务尝试可增加。内部terminal唯一与外部最终可见交付分开判断。真实WeCom P4缺少请求级查询/可靠去重时保持不确定异常，核对耗尽可进入failed/reconcile_exhausted，不盲目重发。外部工具须遵守恢复契约；宿主机、入口和共享依赖整体故障不属于双应用实例冗余。

## 五层20角度：为什么必须有、怎样问、去哪读

本目录侧重部署、入口、生命周期与节点局部资源；下表逐项追踪，同一问题在L1是承诺、L2是原因、L3是状态和条件、L4是证据。

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
