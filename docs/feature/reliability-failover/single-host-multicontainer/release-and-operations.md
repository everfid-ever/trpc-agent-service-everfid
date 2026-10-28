# 单主机多容器实例可用性 — 发布与运维

## 故障当班操作顺序

1. 固定故障范围：记录实际退出的应用容器、退出时间、实例槽位和启动身份，确认入口与共享依赖仍在运行。
2. 查 peer 是否为故障前同一进程，再查依赖与组件 ready、入口健康。避免把 peer 被重建后的成功算成原存活进程接管。
3. 按受影响角色抽取原 request/claim，分别追踪执行与投递；不以新 HTTP 请求成功代替原任务恢复。
4. 观察最老未完成任务和两租户完成情况。若仅有不确定工具/投递异常，进入对应处置流程，不把所有积压都交给重启。
5. 业务故障阶段完成记录后才恢复受害实例，核对新启动身份与连续成功窗口，再检查迟到旧事件。
6. 保留故障到重新稳定的证据，后续反向故障使用新轮次和新输入。

上述顺序可用于排障，不要求对真实故障再次注入破坏。故障节点是否自动重启按实际部署策略记录；只有在关闭自动恢复的独立用例中，才得出“保持停止期间由 peer 接管”的结论。

## 四类现象的处理分支

| 现象 | 先读什么 | 安全处理方向 |
|---|---|---|
| 所有后端不健康 | 节点 /readyz 与入口 checked_at/failure。 | 区分入口网络、节点组件和共享依赖，修复真实失败。 |
| 只有某个新节点不 ready | 启动日志、owner、只读文件、依赖与组件观察。 | 修复该节点配置/资源，保持现有可用节点。 |
| 入口正常但旧任务不结束 | 原 input_seq、lease、尝试、工具、Outbox/Ledger。 | 按在途阶段定位，入口参数通常不是根因。 |
| 重新加入后频繁波动 | 时间序列、资源、probe timeout、连续成功数。 | 识别抖动源，先保留证据再调整下一轮参数。 |

## 容量不足时的恢复目标

单实例可能在连接和数据一致性上正确接管，但无法在原时限完成全部流量。此时先量化实际输入/完成速率与最老年龄，查 CPU、内存、PG/Redis 连接、模型限额和外部工具时延，再使用既有部署/业务手段调整资源或流量。

当前文档不把资源告警自动扩容、租户公平调度、自动熔断或全链路准入限流写成已具备能力。若需要这些动作，须由相应部署和业务实现提供；仅修改健康探测使节点保持 healthy 不能增加实际吞吐。

## 发布回滚应保留的四份材料

| 材料 | 用途 |
|---|---|
| 新旧镜像、迁移状态及兼容性说明 | 判断旧应用能否读取新状态，不自动删表回退。 |
| 配置与密钥版本引用、技能摘要 | 保证旧在途输入、结果及原路由仍可解析。 |
| 节点/入口/共享依赖拓扑与资源配置 | 区分应用回滚、入口故障与依赖恢复。 |
| 未完成原 request、工具和 Ledger 异常清单 | 发布结束后继续追踪遗留责任。 |

回滚成功应同时解释服务可用与遗留工作，不仅是旧镜像能够启动。对于已发生的外部副作用和已发送回复，应用回滚不会把它们撤回。

## 发布顺序和门禁

固定版本与镜像，备份状态及当前密钥版本。执行000001基础迁移，再000002工具执行恢复、000003投递处置、000004在途增量。当前运行时查询与工具store依赖新字段，不能跳过000004只升级应用。

一次性bootstrap完成配置、双Bot路由和技能发布，应用节点只读加载。两个Bot的(Corp ID,Agent ID)必须不同；相同凭据在其他环境运行可能影响渠道归属。两节点使用相同PG/Redis命名空间、不同槽位和启动身份。依赖/组件ready通过后再开放稳定入口。

工具新增时声明恢复契约并测试；manual不因“需要自动恢复”而临时改成replay_safe。加密结果的旧key version、冻结配置/renderer/format版本在仍有在途工作时必须可解析。迁移字段是兼容性契约，不能凭代码回滚就删除。

## 回滚约束

正常回滚先撤readiness、有界drain，再停实例；保持至少一个兼容实例服务。Compose停止宽限大于drain timeout。不能删除pending、Outbox、tool_execution、Ledger或租约来清理积压；不能把ambiguous改成sent/未发以绕过核对。不要自动执行down迁移删除执行/密文/审计数据。旧代码无法理解新增状态/字段时，先评估兼容性与剩余工作，再决定版本切换。


| 配置 | 默认值 | 校验 / 行为 |
|---|---|---|
| TRPC_LISTEN_ADDRESS | :8080 | 非空，无首尾空白。 |
| TRPC_WECOM_HA_ENTRY_BACKENDS | 必填 | 至少两个不同 http origin；拒绝 user/query/fragment/path，含尾斜杠路径也拒绝。 |
| TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL | 1s | 100ms–1m。 |
| TRPC_WECOM_HA_ENTRY_PROBE_TIMEOUT | 1s | 100ms–30s。 |
| TRPC_WECOM_HA_ENTRY_REQUEST_TIMEOUT | 2s | 100ms–30s，每后端一次转发。 |
| TRPC_WECOM_HA_ENTRY_RECOVERY_SUCCESSES | 2 | 1–10，仅恢复阶段要求连续成功。 |
| HealthTTL | 派生值 | 3×(interval+probe timeout)，没有单独环境变量。 |

| 参数 | 当前本地组合根装配 | 意义 |
|---|---|---|
| Worker lease / renew | 30s / 10s | 失权取消执行；lease 到期才可换 owner。 |
| Worker reclaim / limit | 5s / 100 | 扫描 pending；不代表 5 秒内必完成恢复。 |
| Relay claim / renew / poll | 30s / 10s / 100ms | 先发布，再标记 published；发布后崩溃可重发同一事件。 |
| Delivery claim / renew | 30s / 10s | 每分段发送权。 |
| Delivery reclaim / limit | 5s / 100 | 回收账号回复队列 pending。 |
| Delivery retry | 初始 1s，最大 1m，最多 8 次 | 明确可重试错误的有界退避。 |
| Delivery reconcile | 最多 8 次 | 无查询能力或核对失败最终保留 failed/reconcile_exhausted 事件。 |
| 工具 lease / renew | 默认 30s / TTL÷3 | PostgreSQL 工具 lease，与会话 lease 是两套执行权。 |
| 应用排空 | 默认 30s，可配置 1s–5m | 停止新领取，保留已接纳工作，超时再取消。 |
| Compose 停止宽限 | 默认 45s | 必须比应用排空更长并留退出余量。 |

这些值是当前 `webui-local`/`wecom-local` 装配值，不能与 Consumer 未配置时的包级兜底值混用。恢复耗时还包含排队、上游延迟、退避和采样误差，不能把 TTL 相加写成已验证的 RTO。

## SIGTERM 排空顺序

`signalCtx` 接收 SIGTERM/父 context 取消；活动工作使用独立 `processCtx`。主运行时先 `health.beginDrain()`，再 `drainGate.Begin()`，同时调用 HTTP Shutdown 并等待参与排空的角色退出。业务 HTTP 在排空期间返回 503，live/ready/status 路径可检查；ready 不会被迟到的成功轮询恢复成 true。

Worker、Delivery 和 Wakeup 的读取 context 先取消，活动工作 context 保留。预处理和 Relay 通过 drainPoll 停止下一批；Delivery Supervisor 停止 refresh/新建消费者，runUntilDrained 不重启已排空的消费者。活动进度发布在最终 processCtx 取消时退出。

默认排空 30 秒；Worker/Delivery 的局部超时装配为应用时限加 1 秒，让主运行时先控制全局期限。正常结束后取消 processCtx；超时则报告排空超时、取消工作并退出。未完成的 durable 工作留给存活节点。排空不是共享队列清零，也不会人工删除 lease。SIGKILL 完全跳过本流程。

## 观测、告警与容量口径

优先使用当前实际接口和日志：/readyz、可选/statusz、/ops/reliability、/ops/session/lease、投递错误日志、drain started/completed或timeout。本文不把示意性的Prometheus名称当作已注册指标。任何监控规则先确认当前exporter真实名称/标签，避免部署不存在的告警。

建议对持续无ready后端、组件持续degraded、请求长时间无terminal、pending/Outbox持续增长、过期running/sending、ambiguous/reconcile_exhausted、租户不一致、重复最终回复和drain超时建立告警；阈值由租约周期、流量和业务SLO预先设定。明细最多100条的管理快照不是完整历史导出，严谨计数须查询权威记录。

一个survivor必须承担全部计划流量，模型重试会扩大上游请求量。PG连接池、Redis容量、模型限额、tmpfs256MiB和技能加载应一起计入资源预算。只增加应用实例不能消除共享依赖单点。


## 分场景处置

| 现象 | 先核实 | 安全动作 |
|---|---|---|
| 节点live但不ready | /readyz实时依赖；status组件state/reason；对应owner | 修复实际依赖或角色错误，等待真实poll恢复；不伪造ready。 |
| 同一原任务长期pending | request/input_seq、park期限、前序输入、lease TTL和execution_attempt | 排查前序阻塞/依赖；保持原消息，不手工新建请求。 |
| running owner已退出 | 进程启动身份、lease/claim_until、数据库时钟 | 等自动回收；超时按故障证据排查，不提前删lease抢占。 |
| 冻结结果已存在无terminal | result_ref/digest/key version、取消意图和新fence | 修复读取/版本错误，由Runner续提交；不手工生成新答案。 |
| 工具effect_unknown | 恢复策略、业务键、外部效果、是否有加密成功结果 | queryable核对/同键幂等恢复；manual保留安全失败，不无证据重放。 |
| Ledger ambiguous/reconcile_exhausted | client_request_id、接受证据、外部实际状态 | 通过管理接口带证据/版本处置。 |
| 新节点技能加载失败 | bootstrap是否成功、RO卷路径、内容版本/摘要 | 修复发布和挂载，以bootstrap发布正确内容；读节点不改共享包。 |
| 入口健康反复变化 | checked_at/healthy_since、failures/last_failure、组件ready | 排查依赖、超时、资源及真正错误；阈值改变需重测，不靠盲目放宽掩盖。 |
| drain超时 | 实际在途工作、外部取消响应、Compose grace | 保留durable工作给peer；排查不响应取消的依赖，不谎报优雅完成。 |


## 管理接口与不确定投递处置

设置独立的 `TRPC_RUNTIME_OPERATIONS_TOKEN`（至少 32 字符、不得复用 WebUI token）才注册管理接口。使用请求头 `X-TRPC-Local-Token`，直接访问节点；稳定 WeCom 入口不转发 `/ops/*`。响应 `Cache-Control: no-store`，查询显式带 tenant，不返回正文或密钥。

| 接口 | 参数 / 作用 |
|---|---|
| GET /ops/reliability | tenant_id；单条 SQL 一致快照，含 Inbox/执行/Outbox/Ledger/工具计数、最近执行尝试和异常/处置；明细各最多 100 条。 |
| GET /ops/session/lease | tenant_id、agent_app_id、session_id；原子读 owner/fence/剩余租期，不返回 LeaseID。 |
| POST /ops/delivery/resolve | 外部证据核实后的版本化处置；冲突 409、输入错误 400、未授权 401、成功 204。 |

处置输入为 `tenant_id, delivery_key, segment_no, expected_version, actor, evidence_ref, decision`。decision=delivered 必须带 provider_message_id；另外两种 decision 必须不带。actor 是管理调用方声明的操作人。

| decision | 状态与调度 |
|---|---|
| delivered | 分段置 sent；重调度原 reply Outbox 以继续剩余段，当前段只跳过发送。 |
| not_delivered | 分段置 failed，关闭事件，不补发。 |
| retry_not_delivered | 有未送达证据后置 retry_wait，重调度原 reply，沿用 DeliveryKey/ClientRequestID/内容与累计尝试数。 |

只能处置 ambiguous 或 failed/reconcile_exhausted。Ledger CAS、delivery_resolution 审计和必要的原 Outbox 重调度同事务提交；Outbox 正被领取时返回冲突，不能手改状态绕过。超时不能证明未送达。


## 日常巡检与变更影响

| 变更 | 必查影响 |
|---|---|
| Binding/身份密钥/SDK app编码 | 原tenant/session稳定、跨Bot隔离与原ReplyRoute可解析。 |
| schema/工具恢复策略 | 旧在途记录、加密结果和业务幂等键兼容。 |
| lease/renew/reclaim/claim TTL | 丢权与回收时限、GC/网络抖动及重复执行风险。 |
| entry timeout/recovery threshold | 回调耗时、摘除与重加入震荡；epoch旧结果仍被拒绝。 |
| drain timeout/Compose grace | 应用先完成或报告超时，再由容器停止；grace留足余量。 |
| renderer/format/内容分段规则 | 已冻结delivery plan不被新版本重新解释。 |
| local files/secrets | RO节点可读、bootstrap发布、/tmp私有且空间足够。 |

发布前运行相应代码回归，部署前约定故障窗口和证据计划；强杀/重新加入/反向故障、双Bot原会话与P1–P4分别记录结论。真实验收未做时标注未验证；assumed只作为本轮明确前提，不转写成测得成功。

内部保证与外部交付分别判定：同一输入只有一个有效 terminal commit；每个回复 segment 在 Ledger 中只有一行权威状态，`sent` 后自动路径只跳过发送。该行唯一不等于渠道只接收一次。下游已接受、本地确认未提交的 P4 窗口需要下游可靠去重或查询才能自动收敛。当前 WeCom 没有请求键查询适配器，内容级有限时间去重不足以提供永久请求级幂等；取消、HTTP 5xx 和响应丢失均可能留下不确定结果。系统不盲目补发，先保留 `ambiguous`；核对次数耗尽可转为 `failed/reconcile_exhausted`，仍需运维查证。

## 上线前按角色核对职责

| 角色 | 必要条件 | 故障时由谁继续 |
|---|---|---|
| bootstrap | 依赖正常、可写技能卷、配置版本明确 | 初始化失败应修复后重试，不由读节点改包。 |
| callback入口 | 至少两个合法origin、探测参数 | 本拓扑没有入口冗余；自身故障需恢复入口。 |
| 应用节点 | 私有/tmp、RO技能、共享业务后端、独立身份 | 存活应用节点。 |
| Worker/Relay/Wakeup | queue/store可访问，owner唯一 | lease/claim回收与持久扫描。 |
| Delivery | 对应账号目标存在且真实poll成功 | 存活节点消费者及Ledger协议。 |

“两个节点都装了同样镜像”不足以证明配置等价。DSN、RedisEnvironment、模型/渠道密钥、Binding版本、技能内容和governance配置都可能让其中一个节点ready失败或处理不一致。

## 计划发布的建议顺序

1. 先确认旧节点仍可服务，备份当前配置/镜像标识与迁移状态。
2. 准备新配置和不可变技能版本，由bootstrap发布，读节点不改包。
3. 对待更新实例执行SIGTERM；观察draining、ready撤销、活动工作完成或明确超时。
4. 启动新实例，核对新的process_start_id、组件ready与入口恢复窗口。
5. 观察完整稳定窗口，核对原会话和积压，再安排另一实例。
6. 记录每一步版本与owner；出现不兼容状态时暂停发布并按回滚约束处理。

该顺序是部署操作建议，不表示当前脚本自动完成滚动发布。故障演练中的SIGKILL属于另一条路径，不用于正常替代排空。

## 入口一直不重新纳入节点时

先检查节点/readyz，再看入口/statusz的checked_at、consecutive_successes、last_failure。如果节点本身不ready，应修复组件/依赖；如果节点ready但探测超时，检查容器网络、origin、probeTimeout和资源。

恢复阈值需要连续成功，中间任何失败都会重置。因此持续抖动会使节点长时间未纳入，这比“每次成功立刻放行”更保守。修改阈值前先保留证据，不能把减小阈值等同修复故障。

## 排空超时的分层排查

| 层 | 核对 | 可采取的动作 |
|---|---|---|
| 应用全局 | drain开始时间和timeout、哪些角色尚未结束 | 检查外部调用与活动任务，保留durable记录。 |
| Worker/工具 | 是否收到context取消、lease是否仍有效 | 修复不响应取消的工具；不绕过恢复契约。 |
| Delivery | provider是否已接受、是否记录不确定 | 按P4核对，不能强行标sent。 |
| HTTP | 已接纳handler是否仍在shutdown | 检查连接/长请求；到期有界关闭。 |
| Compose | stop_grace_period是否先于drain截止 | 调整下一次配置并重新验证，记录本轮被强杀。 |

将drain超时调得无限长可能使维护无法结束；调得过短又会制造更多不确定窗口。值应根据可接受的活动工作时长、取消行为和部署宽限共同选择。

## /tmp和技能卷的排障注意

tmpfs容量不足会影响密钥投影或临时任务，即使root只读也不能忽略/tmp用量。应检查实际资源与错误，不把私有目录改为全节点共享来减轻空间压力。

技能内容不匹配需要核对镜像与bootstrap发布版本。正常读节点拒绝修复共享包；若管理员改成RW后启动成功，只证明绕过了隔离，不能算原部署已恢复。

## 故障轮次结束后保留什么

保留容器退出/重启次数、前后process_start_id、入口与组件快照、原请求和实际owner、积压曲线、客户端最终回复及操作时间线。测试项目清理与生产恢复分开，特别避免把脚本的down --volumes用于共享生产数据。

反复故障后新增的僵尸consumer或资源增长应作为问题记录；一次重新启动后指标暂时恢复，不代表长期泄漏不存在。
