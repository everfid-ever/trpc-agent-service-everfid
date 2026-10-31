# 在途任务接管与幂等投递 — 发布与运维

## 步骤级恢复升级与排障

先执行 `000005_task_checkpoints` 再运行启用 TaskPlans 的应用。旧实例不理解计划封存时，可能被数据库门禁拒绝成功提交，不能在仍有新计划的环境无条件回滚到旧应用。保留两张新表、旧 payload key version 和原工具结果。

`/ops/reliability` 返回 task_plans 的 request/fence/version/phase/final_ref/updated_at 与 task_checkpoints 总数。明细最多 100 条；状态存在不证明步骤业务正确。需要检查正文时使用授权的受控数据访问，不把解密计划写入普通日志。

若任务返回 task plan incomplete，按原 request 查未完成步骤、前序依赖和绑定工具；不要删除义务让任务过门禁。若原工具 succeeded 而步骤尚未成功，Recover 应核验回执后补齐；解密、摘要、工具身份或结果断言失败需保留证据。确定性断言不满足会以 task_evidence_rejected 安全失败，不能重新生成另一份回执掩盖业务失败。

计划串行执行且修订只追加；不支持任意修改已绑定工具参数、删除失败步骤、自动补偿外部效果、无限步骤或自动证明分析正确。业务需要新的效果时，应通过受控的新业务输入与授权流程执行，而非手工改当前 checkpoint。

## 原任务长期未完成时按阶段处理

| 已查到的最靠后事实 | 继续检查 | 可采取的处置 |
|---|---|---|
| 只有 Inbox/job | job 状态、claim/退避、payload 是否可读。 | 修复预处理依赖，让原 job 继续；不另造输入。 |
| 已有 execution/dispatch | 发布责任、队列 pending、有效消费者和会话 lease。 | 修复角色或依赖，等待协议回收；不要删除有效 lease。 |
| 后序 parked/blocked | 前序 input、停放期限与相关唤醒状态。 | 处理前序/阻塞原因，不能改 head 跳过未知效果。 |
| 工具 running/effect_unknown | lease、策略、业务键、外部操作状态与成功密文。 | 按工具契约查询或同键恢复；manual 保留未知。 |
| 已冻结结果 | key version、摘要、取消意图、terminal 冲突。 | 修复结果读取/版本问题，复用原结果。 |
| terminal 已有 | reply Outbox、路由和发送账号消费者。 | 聚焦投递链路，不再重跑模型。 |
| Ledger 未结束 | 分段状态、claim、NotBefore 与核对计数。 | 区分正常等待、明确失败与 P4 未知。 |

“最靠后事实”不是只看更新时间最大的记录。应按同一原输入关联核实，并且查完整分段；例如第 0 段 sent 不表示其他段也结束。

## 运维 resolve 前后的完整操作单

1. 读取当前 tenant/delivery/segment 与 version，保存 renderer/format/内容摘要等元数据。
2. 对照稳定 client_request_id、provider 信息与原会话取得外部证据。仅日志超时或“现在没看到”不足以证明未送达。
3. 明确选择 delivered、not_delivered 或 retry_not_delivered，记录 actor 与 evidence_ref；后两者语义不同。
4. 通过受保护接口提交 expected_version。409 表示事实已变化或存在竞争，重新查询并核对；不修改数据库绕开。
5. 重新查询 Ledger 与处置审计；需要重调度的决定还要核对原 Outbox。成功 204 只代表处置事务成功，不等于剩余分段已经发送完。
6. 观察原 reply 的剩余处理、最终客户端内容与重复检查窗口，记录仍未解决的分段。

evidence_ref 是证据位置引用，不是服务端自动验证外部事实的机制。操作令牌持有者对其提交的事实判断负责；共享令牌与声明 actor 也不能被描述为完整个人身份认证和合规审批系统。

## 先保留哪些异常信息

工具效果不确定保留原 request、operation、工具版本、策略、参数摘要、业务键、外部句柄和 lease/fence。发送不确定保留 delivery/segment、稳定请求键、版本、provider 关联证据与最后错误分类。业务正文仅在必要且授权的环境中查阅，通常不需要把密文结果解密后写入工单。

这些记录足以指导恢复策略，但未必能还原工具函数内部的所有步骤。若外部系统也没有业务日志或查询协议，记录证据缺口并维持未知结论，不能通过猜测补齐事实。

## 重试和告警阈值如何落地

先确定 lease/reclaim、发送退避、核对上限、业务最大等待和观察采样。告警按状态持续时间与最老年龄制定；单次模型 HTTP 失败不等同原任务丢失。达到重试/核对上限后，告警应关联持久异常，不以队列 ACK 后计数下降自动关闭。

修改重试上限会影响恢复窗口、上游负载、外部幂等键有效期和重复观察时长。没有外部去重证据时，增加次数不能修复 P4 协议缺口；应补充查询能力或执行证据化处置。

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

## 按原request定位积压，而不是重新造任务

先从Inbox确认原external_message_id对应哪个request，再查execution_record、execution_attempt及input_seq。如果execution处于pending，查前序输入与Wakeup；如果running，查当前owner/租期；若已有result_ref和terminal，问题应转到Outbox/Delivery。

一个原请求可以经历多次attempt，但不能因排障人为创建第二个request绕过它。重新发送用户消息产生的是新业务输入，可能重复工具效果，并不能修复原任务状态。

## 工具不确定事件的处置分支

| 读到的事实 | 建议动作 | 禁止推断 |
|---|---|---|
| succeeded且结果存在 | 由恢复路径复用，排查后续模型/提交 | 成功工具结果一定已经最终回复。 |
| running且租期有效 | 查owner是否仍工作，等待正常协议 | 因worker换节点就强抢工具lease。 |
| effect_unknown+queryable | 按稳定键核对外部操作 | 没查到就必定没接受。 |
| effect_unknown+idempotent_key | 保留同键，确认外部幂等契约成立 | 换新业务键重做。 |
| manual不确定 | 外部查证，按工具业务处置流程处理 | 通用投递resolve接口能修复工具效果。 |
| 加密结果无法解密 | 检查tenant/key version/摘要 | 清空结果后重跑更安全。 |

当前/ops/delivery/resolve只处理投递异常，不是工具结果编辑器。没有安全业务流程时保持工具不确定状态及任务明确失败，不能通过直接改表伪造效果。

## 投递异常处置的操作记录模板

处置前先记录tenant、DeliveryKey、segment、version、当前state/error class、外部证据来源和时间。证据必须能说明该原回复是否送达，不能只引用“等待了几分钟”。

```json
{
  "tenant_id": "<tenant>",
  "delivery_key": "<original-delivery-key>",
  "segment_no": 0,
  "expected_version": 7,
  "actor": "<operator>",
  "evidence_ref": "<external-evidence-reference>",
  "decision": "delivered",
  "provider_message_id": "<confirmed-provider-id>"
}
```

这是数据形状示例，不应直接提交。delivered用于确认该段已送达；retry_not_delivered必须已有未送达证据并授权重试；not_delivered关闭异常而不补发。后两种不带provider_message_id。若返回409，重新读取状态，不盲目换版本再次提交。

## 一次处置为什么还会重新调度Outbox

Ledger已经sent的段不会被重新发出，但同一答案可能还有后续段。delivered处置会重调度原reply，让发送流程跳过当前段并继续其余段。retry_not_delivered则使用原计划再次尝试该段。

旧Redisentry可能在reconcile耗尽时已经ACK，所以只改Ledger而不重调度Outbox会留下“可重试但无人触发”的任务。当前事务同时处理这件事，并防止与正在持有Outbox的Relay冲突。

## 关于计数突增的运维解释

model_request_count增加可能来自HTTP失败/内部重试；model_call_count增加可能来自一轮重新推理；execution_attempt增加表示执行器再次进入。先查这些变化是否伴随terminal、工具业务效果和客户端重复，不能仅凭一个计数增加判定幂等失效。

相反，业务效果数量增加即使terminal仍只有一个，也需要处理。会话fence保护平台提交，不能回滚外部系统写入。

## 迁移与密钥回滚的实际风险

删除000004表会丢失工具成功结果和尝试日志；删除旧key version会让仍保留的密文不可恢复。只回滚应用版本也可能让旧代码不理解当前恢复记录或缺少必要接口。

回滚计划应先列出未terminal输入、未完成工具和未sent投递，明确目标版本能否读取它们；必要时保持兼容版本处理完，再切换。不能把清空业务表作为标准回滚步骤。
