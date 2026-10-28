# 故障恢复后继续对话 — 发布与运维

## 发布顺序和门禁

固定版本与镜像，备份状态及当前密钥版本。执行000001基础迁移，再000002工具执行恢复、000003投递处置、000004在途增量。当前运行时查询与工具store依赖新字段，不能跳过000004只升级应用。

一次性bootstrap完成配置、双Bot路由和技能发布，应用节点只读加载。两个Bot的(Corp ID,Agent ID)必须不同；相同凭据在其他环境运行可能影响渠道归属。两节点使用相同PG/Redis命名空间、不同槽位和启动身份。依赖/组件ready通过后再开放稳定入口。

工具新增时声明恢复契约并测试；manual不因“需要自动恢复”而临时改成replay_safe。加密结果的旧key version、冻结配置/renderer/format版本在仍有在途工作时必须可解析。迁移字段是兼容性契约，不能凭代码回滚就删除。

## 回滚约束

正常回滚先撤readiness、有界drain，再停实例；保持至少一个兼容实例服务。Compose停止宽限大于drain timeout。不能删除pending、Outbox、tool_execution、Ledger或租约来清理积压；不能把ambiguous改成sent/未发以绕过核对。不要自动执行down迁移删除执行/密文/审计数据。旧代码无法理解新增状态/字段时，先评估兼容性与剩余工作，再决定版本切换。


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

## 原会话回忆异常的分层排障

### 先确认身份有没有变化

记录原tenant、app、user、session以及当前请求对应值。若session变化，检查Bot Binding、external account、会话类型、identity/session密钥与版本。不要先清空SDK历史或重新绑定Bot，这会破坏定位证据。

同一用户跨Bot属于不同租户会话是预期隔离；同一Bot故障切换后进入不同session才需要排查派生字段或配置兼容。

### 再确认历史是否可读取

检查官方SDK会话backend和tenant/app作用域；确认原session_events仍存在且未被保留策略删除。平台session_event没有新行不能直接判断历史丢失，因为生产聊天历史并不镜像到该平台表。

若解密或版本读取失败，不应把空历史当成正常恢复。先恢复旧密钥/配置解析能力，保留错误以避免静默丢上下文。

### 最后区分模型回答问题

身份和历史都正确时，再检查上下文截断、摘要、工具检索与模型行为。恢复设施确保读到正确持久历史，并不保证模型对任意久远细节都回答准确。

## 发错Bot或重复回复时的处置顺序

发错Bot首先按request追ReplyRoute、Binding/config版本和DeliveryTarget，不要因内容正确而关闭事件。重复回复先辨别流式进度、合法分段与重复最终答案，再关联稳定DeliveryKey和ClientRequestID。

若本地只有一条sent但客户端看见两次，保留外部证据并调查接受/确认窗口；本地唯一行不能排除两次外部接受。不要删除Ledger后“重新试一次”，这会丢掉恢复依据。

## 会话相关变更检查单

| 发布内容 | 发布前 | 发布后 |
|---|---|---|
| 身份派生密钥 | 明确旧会话如何继续、旧版本如何解析 | 核对相同原会话键与两租户隔离。 |
| SDK backend/profile | 固定tenant/app绑定及历史可读性 | 原session查历史、新输入可提交。 |
| Bot配置 | 检查主次Bot独立性及冻结版本保留 | 原request仍发往原Bot。 |
| 历史保留策略 | 覆盖业务期望的上下文和恢复窗口 | 不以删除历史减轻积压。 |
| 模型/提示 | 不把模型自报身份当隔离措施 | 真实原会话回忆与路由分别验。 |

## 交接给下一位运维的材料

至少留下版本、拓扑、两个Bot的脱敏Binding映射、入口地址、节点身份、管理接口使用方式、迁移状态、旧密钥保留要求和未解决异常清单。不要把真实凭据直接写进交接文档；引用部署环境的安全注入位置。

每个未完成request说明它停在哪一层：等待前序、等待确认、工具不确定、Outbox发布、投递核对或P4异常。这样接手者能按责任模块继续，而不用靠用户重发试探。
