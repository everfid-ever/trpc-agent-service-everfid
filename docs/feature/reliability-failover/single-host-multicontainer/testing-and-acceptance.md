# 单主机多容器实例可用性 — 测试与验收

## 固定版本、前提与时间口径

本文是可复用流程，不包含实际执行结果。每轮固定完整提交 SHA、镜像摘要、配置/迁移版本、资源限制、拓扑、RUN_ID 和证据目录。两个 Bot 对应不同租户；同一真实用户在各自原会话写入不同随机 TOKEN_A/B，后续提问不再提供代号。使用真实模型时记录调用身份但不以共用上游 key 判断隔离。

关闭受害实例自动重启/替换，共享依赖保持正常，故障节点在本轮业务验收前一直停止。不得手删 lease、重绑 Bot、重投原消息或临时修补业务代码来获得通过。

| 参数 | 执行前必须约定 |
|---|---|
| W_BASELINE | 双节点稳定窗口，覆盖多个心跳、租约与调度周期。 |
| T_READY / T_TASK / T_DELIVER | 整体就绪、原任务完成、最终可见回复时限；不得事后放宽。 |
| W_DUPLICATE / W_REPEAT | 覆盖 lease/reclaim/retry 的重复检查和稳定窗口。 |
| DELTA_POLL | 状态采样间隔，恢复时延结论包含采样误差。 |

t0 为外部确认故障时刻；t_ready 为观察到执行链路就绪；新消息收到至最终可见是请求延迟。人工等待后才发消息不能推算精确端到端 RTO。barrier 主动暂停时长单独记录，不任意扣除。

## 部署前核对

两个实例使用相同提交、共享PG/Redis、相同RedisEnvironment、相容迁移和密钥；instance_id、process_start_id、宿主端口不同。检查root只读、/tmp各自tmpfs、skills卷节点只读而bootstrap可写，stop_grace_period大于应用drain期限。单实例资源足以承担计划流量，记录资源限制，低负载不外推容量。

当前WeCom是HTTP callback，公开origin只指向entry58087，节点58088/58089仅观测。共享依赖/入口自身单点故障另列，不预设能继续服务。

## 生命周期步骤

1. 仅N1运行，完成两个租户写入与回忆代号，确认原会话/最终交付。
2. 加入N2，检查两进程新身份、实际组件ready和入口健康；按W_BASELINE持续重叠对话。
3. 强杀实际持有执行/发送职责的实例，记录t0；保持停止，peer原进程和共享依赖不重启。
4. 等待入口和执行链路自动恢复；原会话连续对话按会话验收，旧在途任务另按P1–P4验收。单实例维持预定负载与观察窗口。
5. 完成本轮业务判定后才重新启动旧实例，检查process_start_id更新，入口经过连续成功重新纳入，旧探测不能恢复失败节点。
6. 等待稳定窗口，检查无健康振荡、长期积压和迟到回复。重建双实例基线，反向强杀另一节点并重复业务判定。
7. 预定轮次重复，检查goroutine/连接/资源趋势。未执行轮次不计结论。

## 计划停止独立判定

SIGTERM应使ready503、停止新业务/新领取，在途任务和发送在期限内继续；完成后退出。超时应记录错误并保留durable恢复状态，不能ACK未完成发送。验证Delivery Supervisor不重建消费者、旧成功poll不能恢复ready。Compose强杀超出grace的进程不能计为优雅排空成功。

| 子用例 | 验证对象 | 不可替代 |
|---|---|---|
| 存活实例接管 | 关闭自动恢复，强杀后持续停止 | 容器重启成功。 |
| 容器自动恢复 | 独立启用重启/替换再故障 | 原任务已完成。 |
| SIGTERM | 撤ready、停止准入、有界排空 | SIGKILL非优雅故障。 |
| 共享依赖故障 | 明确降级、错误与恢复一致性 | 双应用实例高可用。 |

## 入口/隔离代码回归与外部证据

| 检查 | 当前测试证据 |
|---|---|
| 慢节点不阻塞peer probe | wecom_ha_entry_resilience_test.go |
| 502/503/504切换、鉴权拒绝不切换、签名保持 | wecom_ha_entry_resilience_test.go |
| 旧probe不复活节点、健康过期与恢复连续成功 | wecom_ha_entry_resilience_test.go |
| RO root/skills、私有tmpfs、停止宽限配置 | compose_isolation_test.go |
| 文件只读校验与禁止覆盖 | runtime_files_test.go |
| 停新领取、活动发送完成、超时不ACK | delivery/drain_test.go、relay/wakeup_drain_test.go、worker/consumer_test.go |
| 排空后不重新服务 | runtime_drain_test.go |

epoch是内部字段，/statusz不公开；通过状态转移和上述代码测试验证，不要求外部采集不存在的epoch字段。客户端表现、持续负载及真实回复唯一性仍需外部业务证据。

## 最小证据闭环与计数

每次证据至少包含：版本/拓扑/参数、实例及 process_start_id、owner 与 fence/lease 变化、tenant/binding/session、external_message_id/request_id/input_seq、execution_attempt、冻结 result_ref/digest、terminal commit、Outbox、Ledger 的 segment/client_request_id/provider_message_id/state/version，以及客户端最终回复。敏感凭据/个人标识脱敏，完整正文按环境要求保护。

| 计数 | 权威记录 | 解释 |
|---|---|---|
| Worker 尝试 | `execution_attempt` 的行数 / `attempt_no` | 每次进入执行器一行；新尝试把旧 `running` 标为 `superseded`。completed 仅表示执行器正常返回。 |
| 逻辑模型调用 | `execution_record.model_call_count` | 进入模型适配器的逻辑调用，可因恢复增加。 |
| 模型 HTTP 尝试 | `execution_record.model_request_count` | transport 尝试，含连接失败及内部重试；不等于接受数或计费数。 |
| 工具尝试 / 业务效果 | `tool_execution.attempt` / 外部业务幂等记录 | 同键工具重试可大于一次，业务效果应按工具契约去重。 |
| 回复发送 / 用户可见答案 | Ledger `attempt`、`segment_no/count` / 客户端证据 | 一条答案可有多个分段；唯一 Ledger 行不能单独证明用户只见一次。 |

服务端 Ledger 一行 sent 只能证明本地终态；客户端可见重复必须直接核对。模型调用多次不直接失败，正确性看同一原请求提交、业务效果和交付边界。

内部保证与外部交付分别判定：同一输入只有一个有效 terminal commit；每个回复 segment 在 Ledger 中只有一行权威状态，`sent` 后自动路径只跳过发送。该行唯一不等于渠道只接收一次。下游已接受、本地确认未提交的 P4 窗口需要下游可靠去重或查询才能自动收敛。当前 WeCom 没有请求键查询适配器，内容级有限时间去重不足以提供永久请求级幂等；取消、HTTP 5xx 和响应丢失均可能留下不确定结果。系统不盲目补发，先保留 `ambiguous`；核对次数耗尽可转为 `failed/reconcile_exhausted`，仍需运维查证。

## 脚本实际覆盖

```sh
bash scripts/e2e/single-host-multicontainer.sh
```

脚本验证A强杀、A新身份加入、B反向强杀及入口/status/容器状态；默认稳定5秒可配置。TRPC_SINGLE_HOST_KEEP_ENVIRONMENT=true保留环境；默认清理该测试项目和卷，诊断目录保留。TRPC_WECOM_REAL_ACCEPTANCE=assumed/recorded都是外部证据声明，不是自动化真对话验证。wecom-ha-failover.sh为同脚本兼容入口。

## 代码回归入口

```sh
go test ./...
go test -race ./cmd/trpc-service ./trpcservice/worker ./trpcservice/channels/delivery ./trpcservice/relay ./trpcservice/reliability/drain
```

这些命令运行包/竞态测试，不等价于真实 WeCom 故障验收。若某集成测试依赖外部服务，应检查是否实际运行或跳过，不能把普通 go test 退出0视为全部部署验证完成。测试结果、日志与时延保存在独立运行目录，不回填本文。

## 基线、强杀、重新加入的证据时间表

| 采样点 | 节点证据 | 入口证据 | 业务证据 |
|---|---|---|---|
| 仅N1 | N1身份与组件ready | 可转发N1 | 两Bot原会话代号基线。 |
| 双节点稳定 | 两个不同进程身份 | A/B健康快照 | 两租户重叠输入。 |
| N1已强杀 | N1退出且无重启，N2原进程 | A摘除、B有效 | 原任务或后续原会话结果。 |
| N1重新加入 | 新process_start_id | 连续成功后重新健康 | 旧session不变、无迟到重复。 |
| N2反向强杀 | N2退出、N1持续运行 | B摘除、A有效 | 反向接管结果。 |
| 观察窗口结束 | 无意外重启/资源异常趋势 | 无持续健康振荡 | 无不可恢复积压与重复答案。 |

在途任务要额外记录P点及实际执行/发送owner。入口先把回调发到A，并不说明模型执行也在A。

## 部署配置的静态核对

可在不发送业务消息的情况下检查Compose展开结果和容器挂载。重点是实际生效配置，不只看模板文件里是否写了字段。

| 项目 | 期望 | 常见错误 |
|---|---|---|
| PG/Redis | 同一实例与相同协调环境 | 两套独立环境无法接管。 |
| 实例ID与启动ID | 槽位不同；每次启动身份更新 | 同owner造成日志/claim难归因。 |
| 端口 | 容器内可相同、宿主端口不同 | 宿主监听冲突。 |
| root/skills | root只读、技能卷只读 | 节点启动时悄悄修改共享包。 |
| /tmp | 每容器独立tmpfs | 共享密钥投影或临时状态。 |
| bootstrap | 完成后再启动读节点 | 缺包时让节点自己创建。 |
| restart | 接管轮次no | 自动重建掩盖peer接管。 |
| 停止宽限 | 大于drain且留余量 | Docker在应用排空前SIGKILL。 |

静态配置检查无法证明文件读写权限在目标主机上实际可用，应结合启动结果；但读节点不应通过放宽共享目录权限来绕过版本错误。

## 计划内停止的逐步观察

1. 让一个任务执行或回复发送处于可观察的在途阶段，记录原request与owner。
2. 对该实例发送SIGTERM，记录它开始draining与ready撤销。
3. 验证不再接收新的业务/领取下一批；既有活动工作未立即因停止读取而取消。
4. 若工作在期限内完成，核对其terminal/发送确认后实例退出；监督器没有重新创建消费者。
5. 若特意让工作超过期限，核对超时退出错误、未完成发送没有成功ACK，以及peer可按durable状态恢复。
6. 上述两种情形分别记录，不能把“最终被Docker强杀”计为正常排空完成。

已在关闭准入前接纳的HTTP请求仍可能刚保存一条新工作，这条工作可交给peer；不要要求drain清空整个共享队列。

## 持续负载观察应记录什么

| 类别 | 采集方式 | 关注趋势 |
|---|---|---|
| 输入与终态 | RUN_ID消息清单、execution/commit查询 | 原输入最终有明确结果，不永久处理中。 |
| 队列/Outbox | pending与状态分组、最老未完成年龄 | 故障后增长能否收敛，是否持续堆积。 |
| 入口与组件 | status时间序列及失败原因 | 健康持续振荡、反复创建消费者。 |
| 资源 | 容器CPU/内存、连接与临时空间 | 单实例承载是否稳定、重新加入是否累积泄漏。 |
| 回复与业务效果 | 客户端、Ledger、外部业务键 | 迟到重复和重复写入。 |

每轮固定请求速率、并发、请求内容分布、观察时长和资源限制。只有短聊天低并发时，结论限于该负载。若负载生成器失败或渠道限流，分别说明，不把它们静默扣除。

## 时间口径的额外注意

脚本的timeout_seconds是轮询尝试次数，循环中的curl本身还有超时，因此它不是精确秒数。稳定窗口默认5秒是脚本控制值，不自动覆盖所有发送退避或长期资源趋势。

故障时刻、入口首次成功、Worker新尝试、原任务terminal和最终可见回复各有时间戳。前两个事件很快不代表后两者也达到SLO；恢复时间报告应列出对应事件，而不是只写一个“HA耗时”。

## 失败后的处理边界

如果peer仍ready但原任务未完成，保留环境并查durable链路；不要重发消息后覆盖原证据。如果节点重加入后健康震荡，先排查共享依赖/超时/资源与配置一致性；不要为了本轮通过临时改阈值。

共享数据库或Redis被误停，应结束应用接管子用例并单独记录依赖故障。恢复依赖后的成功不能回填为“单实例强杀自动接管通过”。
