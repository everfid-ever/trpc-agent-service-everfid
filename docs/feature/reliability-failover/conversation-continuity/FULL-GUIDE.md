# 故障恢复后继续对话 — 完整实现说明

## 从渠道身份到持久化会话

本能力保护 身份映射与官方会话后端。BindingRoute 先由 opaque route key 定位渠道配置，协议验签后才形成可信 Binding。用户不能通过正文任意选择租户。身份映射按租户 identity/session 密钥做 HMAC，字段按长度前缀编码，避免拼接碰撞：单聊 session 使用 channel/account/dm/user，群聊使用 channel/account/group/chat。映射密钥版本、账号或身份维度改变可能改变会话，发布时必须保持兼容。

SDK app_name 用 TenantKey 编码 tenant 和 agent app，生产每轮由BufferedTurn的checked facade检查app/user/session；TenantGetSessionHook为单作用域调用提供相应hook。聊天内容由官方SDK session_events保存；平台session_head/session_commit管理输入顺序、提交和fence。BufferedTurn.Commit不再给平台session_event写第二份聊天真相；平台LoadSession方法存在，不代表生产上下文从它加载。

## 为什么消息和路由必须同时保存

验签完成后先保存 Inbox、加密输入与预处理工作，并冻结 ReplyRoute。Inbox 的渠道消息唯一键防止入口重试或 provider 重传创建新任务。request_id 在 dispatch、worker、result 和 reply event 之间贯穿。若只保存消息文本而恢复时重新猜 Bot/tenant，节点接管可能把结果发往错误账号；冻结路由避免这一问题。

## 故障瞬间发生什么

1. 稳定入口发现故障节点 ready 失败或转发失败，后续回调选择存活节点；两 Bot 仍使用相同公开 origin。
2. 未确认工作保留在 Redis pending；Worker reclaim 后读 PostgreSQL last_fence，校准 Redis 计数，再取得会话 lease。
3. 原会话持久历史被新运行读取，input_seq 与 session version 限制顺序；旧 owner 失权后取消执行，迟到写入仍须通过数据库 fence 校验。
4. 已 terminal 的输入直接识别终态，不重跑模型。未 terminal 但已有冻结 result 的输入校验原答案后续提交；其余进入模型/工具恢复。
5. terminal commit 与 reply Outbox 同事务产生。Relay 和 Delivery 可由其他节点继续，不要求执行节点也持有发送职责。

## 为什么 lease、fence、Inbox、Outbox 都要有

| 只保留一个机制 | 会遗漏什么 |
|---|---|
| 只健康探测 | 入口摘除不能撤销旧 Worker 的写权限。 |
| 只 lease | 旧进程在暂停后恢复可能迟到写，必须用数据库 fence/version 拒绝。 |
| 只数据库 fence | 无法限制并发昂贵执行，也需要校准 Redis 计数以避免回退。 |
| 只队列 | 无法原子保存业务结果与“应发回复”，提交后崩溃可能漏发。 |
| 只 Ledger | 不保存身份、上下文或结果；唯一行也不能让无幂等协议的渠道实现 exactly-once。 |

## 稳定入口的当前算法

`wecomHAEntry` 对每个后端并行执行有超时的 `/readyz` 探测。HTTP 200、响应有效且探测 context 未失效才算成功；探测 body 最多读取 1024 字节。每次探测记录开始时的 epoch，较新的失败会递增 epoch，旧结果因 epoch 不匹配被丢弃。

路由须满足健康且 `checked_at` 未过期。初次成功可立即纳入；探测/转发失败或健康过期后需连续成功次数达到阈值。健康有效期为 `3 × (ProbeInterval + ProbeTimeout)`。epoch 是进程内竞态控制字段，不在 /statusz 中公开。

`serveHTTP` 只为 callback 路径做转发。正文上限 2 MiB；按轮转起点选择有效健康节点，每次回调对每个节点最多一次。网络失败、502/503/504、非 4xx 的响应读取失败或超限会摘除该后端并尝试另一节点。4xx 拒绝不切换，4xx body 读取失败时仍返回该状态；客户端取消不据此惩罚后端。完整响应缓冲成功后才写给客户端。

`forward` 保留 RequestURI、正文和签名相关头，`copyHeaders` 去掉逐跳头及 Connection 指定的头，不新增 X-Forwarded-*，也不跟随重定向。入口重试靠后端 durable Inbox 去重；不能代替投递端的幂等协议。

## SIGTERM 排空顺序

`signalCtx` 接收 SIGTERM/父 context 取消；活动工作使用独立 `processCtx`。主运行时先 `health.beginDrain()`，再 `drainGate.Begin()`，同时调用 HTTP Shutdown 并等待参与排空的角色退出。业务 HTTP 在排空期间返回 503，live/ready/status 路径可检查；ready 不会被迟到的成功轮询恢复成 true。

Worker、Delivery 和 Wakeup 的读取 context 先取消，活动工作 context 保留。预处理和 Relay 通过 drainPoll 停止下一批；Delivery Supervisor 停止 refresh/新建消费者，runUntilDrained 不重启已排空的消费者。活动进度发布在最终 processCtx 取消时退出。

默认排空 30 秒；Worker/Delivery 的局部超时装配为应用时限加 1 秒，让主运行时先控制全局期限。正常结束后取消 processCtx；超时则报告排空超时、取消工作并退出。未完成的 durable 工作留给存活节点。排空不是共享队列清零，也不会人工删除 lease。SIGKILL 完全跳过本流程。

## 回复承诺与故障假设

内部保证与外部交付分别判定：同一输入只有一个有效 terminal commit；每个回复 segment 在 Ledger 中只有一行权威状态，`sent` 后自动路径只跳过发送。该行唯一不等于渠道只接收一次。下游已接受、本地确认未提交的 P4 窗口需要下游可靠去重或查询才能自动收敛。当前 WeCom 没有请求键查询适配器，内容级有限时间去重不足以提供永久请求级幂等；取消、HTTP 5xx 和响应丢失均可能留下不确定结果。系统不盲目补发，先保留 `ambiguous`；核对次数耗尽可转为 `failed/reconcile_exhausted`，仍需运维查证。

| 计数 | 权威记录 | 解释 |
|---|---|---|
| Worker 尝试 | `execution_attempt` 的行数 / `attempt_no` | 每次进入执行器一行；新尝试把旧 `running` 标为 `superseded`。completed 仅表示执行器正常返回。 |
| 逻辑模型调用 | `execution_record.model_call_count` | 进入模型适配器的逻辑调用，可因恢复增加。 |
| 模型 HTTP 尝试 | `execution_record.model_request_count` | transport 尝试，含连接失败及内部重试；不等于接受数或计费数。 |
| 工具尝试 / 业务效果 | `tool_execution.attempt` / 外部业务幂等记录 | 同键工具重试可大于一次，业务效果应按工具契约去重。 |
| 回复发送 / 用户可见答案 | Ledger `attempt`、`segment_no/count` / 客户端证据 | 一条答案可有多个分段；唯一 Ledger 行不能单独证明用户只见一次。 |

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

## 验证含义

双 Bot 需要不同随机代号，原会话恢复后提问不得再带代号；服务端租户/会话/路由和客户端回复一起核对。基线全部交付后强杀属于本能力；任务处理期间强杀要按在途窗口独立判定。ready/脚本成功只证明各自可观察项，不能替代真实上下文和最终可见性证据。

## 正常一轮的完整时序

下面的图按持久化职责拆开组件。省略的验签细节并不表示允许绕过验签；图中所有内部身份均来自可信Binding和固定配置。

```mermaid
sequenceDiagram
    participant U as 用户/Bot
    participant E as 稳定入口
    participant I as 渠道接收
    participant P as PostgreSQL
    participant Q as Redis队列
    participant W as Worker
    participant H as SDK会话后端
    participant D as Relay/Delivery
    U->>E: 原Bot中的消息
    E->>I: 原签名材料与正文
    I->>I: 验签、解析Binding、派生user/session
    I->>P: Inbox/输入/预处理工作与回复路由
    Note over I,P: durable接收后可异步推进
    W->>Q: 消费dispatch delivery
    W->>P: 读取session head与last_fence
    W->>Q: 校准并获取会话lease
    W->>H: 同tenant/app/user/session读取历史
    W->>H: SDK同步保存本轮事件
    W->>P: 保存冻结result
    W->>Q: beforeCommit校验/续租
    W->>P: terminal+会话推进+reply Outbox
    W->>Q: 对已有durable disposition ACK
    D->>P: 领取reply Outbox与冻结路由
    D->>Q: 发布账号reply event
    D->>P: claim回复segment
    D->>U: 调用渠道发送
    D->>P: 确认sent或保存不确定状态
```

图中的SDK事件写入和terminal提交是两次持久化边界。如果后者失败，前者可能仍在历史中；这正是恢复时不能把SDK转写当作“本轮最终业务已经成功”的原因。

## 会话身份为什么在节点更换后不变

### 用户身份与会话身份分开派生

identity密钥用于派生用户标识，session密钥用于派生会话标识。使用不同purpose可以避免一个用途的密钥被不恰当地用于另一个用途。派生输入带channel、account和会话类型，单聊与群聊的命名空间不会仅因一个字符串恰巧相同而合并。

### 长度前缀避免字段拼接歧义

例如(account="ab",user="c")和(account="a",user="bc")若直接拼接都会得到"abc"。当前derive在每个字段前写入固定宽度长度，再写字段内容，因此这两组输入不同。HMAC又使用租户作用域密钥，使相同外部用户在不同租户中形成不同内部身份。

### 节点身份不能参与业务身份派生

process_start_id用于owner和日志，不能加到session_id里。否则同一用户在N1与N2之间切换会出现两套历史。恢复正确性依赖业务身份稳定、执行身份更新，这两件事同时成立。

## 为什么Bot凭据与数据库tenant都要核对

外部请求只证明“某个渠道消息通过某个Binding验签”。它没有权限直接声明任意内部tenant。持久route把验签配置与tenant/app绑定，后续查询又用tenant筛选，密钥解析再次限定tenant/purpose/version。

这些检查是串联的防线。仅检查一次路由而后续用无tenant条件的结果查询仍会串数据；仅在数据库带tenant而让外部任意填写tenant，也会形成越权入口。ReplyRelay还校验result与route的tenant/request一致，避免最后一跳重新放开隔离。

## 故障恢复的分支树

```text
收到可恢复的原delivery
├─ authoritative输入/版本/租户不匹配 → 拒绝，保留错误证据
├─ 输入已terminal → 读取终态，不重跑模型
├─ 前序输入未完成 → durable停放/阻塞，由唤醒链路继续
└─ 当前输入可执行
   ├─ 有冻结result → 校验后续提交
   └─ 无冻结result → 读取SDK历史并运行模型/工具
      ├─ 已完成工具结果 → 复用
      ├─ 可恢复外部操作 → 按契约恢复
      └─ 无安全恢复契约 → 明确不确定/安全终结
```

这棵树解释了为什么“节点恢复后继续”不等于每次都从模型入口重新做一遍。系统优先使用更靠后的durable事实，但不会跨过尚未满足的权限、取消或顺序条件。

## 排他执行与拒绝迟到提交如何配合

假设N1持有fence=41，在网络异常后停止续租。N2取得新lease的fence=42，不表示数据库立即把所有状态都改成42。旧N1提交前的beforeCommit必须匹配旧lease并续租；旧lease失效时该检查失败。N2提交后，数据库last_fence推进，旧41即使迟到到达数据库也会被拒绝。

这里有两道条件：lease控制当前执行资格，数据库fence/version控制状态推进。不能只看“新节点拿到42”就省略旧节点提交前的检查，也不能认为fence会撤销已经写入外部系统的效果。

## SDK历史与平台协调分别承担什么

| 问题 | SDK历史 | 平台协调 |
|---|---|---|
| 用户此前说过什么？ | 保存并加载消息/状态。 | 不复制第二份完整转写。 |
| 哪条输入现在允许执行？ | 不作为唯一顺序裁决。 | session_head的输入闸门。 |
| 哪个owner可提交业务结果？ | 不能替代lease/fence。 | lease、beforeCommit、commit_turn。 |
| 某个答案是否已经成为最终结果？ | 一条assistant事件不足以证明。 | terminal commit与result_ref。 |
| 最终结果是否需要发出？ | 不负责可靠发送调度。 | reply Outbox与Ledger。 |

平台Rollback不会撤销SDK已同步写入的事件。重试中可能出现重复输入或孤立工具调用痕迹；不能把这些记录机械地计为多条最终回复。反过来，发生SDK会话读取失败时也不能用空历史“正常继续”并声称上下文恢复成功。

## 新消息可用与原任务恢复的分界

节点故障前基线已完成、故障后才发的新问题，主要验证身份映射与历史读取。故障时已经持久接收的旧问题，必须按同一个request_id追踪到完成或明确失败。这两种场景需要分别有证据。

例如N2能回答“你好”只能说明新消息链路可工作，不能证明N1上的长任务没有丢。代号回忆能证明某些历史被加载，也不能证明尚未发出的旧答案已经送达。

## 配置变更可能破坏连续性的方式

| 变更 | 可能结果 | 发布时的要求 |
|---|---|---|
| 更换session派生密钥 | 同一外部会话映射成新session。 | 设计兼容/迁移，不能当普通滚动重启。 |
| 更换Bot Binding或账号 | 路由、权限和目的地变化。 | 保留在途冻结版本可解析，核对新旧Bot。 |
| 删除历史数据/旧key version | 旧上下文或加密结果无法读取。 | 保留覆盖恢复窗口的历史与密钥。 |
| SDK app_name编码变化 | 命中另一个历史命名空间。 | 同步迁移/兼容，不静默创建空会话。 |
| renderer/format变更 | 旧结果分段计划冲突。 | 已冻结投递计划不能被重新解释。 |

## 常见反例的定位顺序

如果“回忆不正确”，先核对session是否相同，再核对SDK作用域和原历史是否存在，最后区分模型回答质量；不要先归因模型。如果“回忆对但发错Bot”，检查冻结ReplyRoute与DeliveryTarget，不能用答案正确掩盖路由错误。

如果“故障后一直处理中”，从原request查execution/input_seq、lease/fence、Outbox和Ledger，不只看HTTP健康。恢复到哪个durable边界，就由哪个模块负责下一步，避免所有问题都落到Worker重试上。

## 一条输入的状态快照：从记住代号到故障后回忆

下面使用示意身份tenant-A/app-A/session-A。它们不是仓库常量，也不是实际运行结果，用来解释字段如何关联。

### 写入代号时

用户向Bot A发送“记住代号M7”。验签后的身份指向tenant-A；Inbox保存外部消息m1与内部request r1。Dispatch为原会话分配input_seq=1，Worker获得会话lease后运行。

| 层 | 示意记录 | 含义 |
|---|---|---|
| Inbox | tenant-A / m1 → r1 / session-A | 原输入身份。 |
| execution_record | r1 / input_seq1 / 固定配置版本 | 该轮执行责任。 |
| SDK会话 | app_name=tenant-A/app-A，session-A | 用户原文和本轮事件。 |
| result_payload | r1 → result-ref-1 | 冻结回复内容。 |
| session_commit | r1 / seq1 / terminal | 第一轮已经成为权威结果。 |
| reply Outbox/Ledger | 原reply键 / segment0 / sent | 发送调度和本地确认。 |

此时即使N1内存完全丢失，代号仍在SDK会话后端。终态和投递完成不需要保存在N1本地目录中。

### N1被强杀时

本用例先确认r1最终交付，再强杀。因为故障时没有未完成测试输入，本轮主要验证后续身份与历史连续性。入口摘除N1，N2能够继续接收；各角色由自己的健康与共享协调正常工作。

这里不必凭空“恢复r1的工具进度”，因为r1已经完成。将这轮结果写成覆盖全部P2工具故障会扩大验证范围。

### 故障后第二条输入

用户仍在Bot A原会话问“我刚才告诉你的代号是什么”，形成m2/r2/input_seq2。m2是新消息身份；session-A和SDK app_name必须保持不变。

N2读取该原会话历史，模型才有条件回答M7。r2产生自己的结果和reply身份，但回复目的地仍来自Bot A原输入及固定配置，而不是从模型输出决定。

## 并发租户对照为什么有用

同时让Bot B记住另一个代号Q9，可以发现全局变量、默认配置或缓存key缺失tenant维度的问题。若只测A，即使历史被放在全局共享对象里，也可能偶然答对。

对照负载还可以揭示“其中一个租户永远无法领取工作”的问题。A成功不应被当成B也能被接管；两个Bot都要有独立request、session和最终投递证据。

## 历史里出现重试痕迹时怎样解释

SDK可能已经保存用户输入和部分助手/工具事件，而平台terminal尚未完成。故障重跑可能让转写出现第二条同内容输入，或上次工具调用缺少配对结果。它说明运行发生过中断，不能直接等同用户看到了两次最终答案。

检查时先用request/input_seq及terminal判断业务结果，再看冻结结果和Ledger，最后对客户端。仅按SDK某类事件行数统计“任务完成次数”会把重试和中间事件误算进去。

同样，只有终态唯一并不能反向证明历史事件从未发生重复。文档将这两个事实分开，便于定位未来的上下文质量优化，而不是把所有问题都归到投递幂等。

## 网络隔离时的安全边界

应用进程可能没有退出，只是暂时访问不到Redis或PostgreSQL。无法续租时取消执行，提交前检查应拒绝旧资格；健康检查也可能撤销ready。另一节点只有在共享协调允许后才能取得执行权，不能因为“感觉N1坏了”立即抢占。

如果共享协调服务本身不可用，两个实例可能都无法继续处理；这属于共享依赖故障，不是“应用冗余保证仍能服务”的情形。安全失败比在没有执行权时继续写更重要。

## 为什么最终路线使用原配置版本

节点切换可能恰好遇到配置更新。若发送者只加载最新默认Binding，一条早先受理的消息可能被新配置发到另一账号。ResolveReplyRoute读取execution_record的原config_version，联结原Inbox与该版本Binding，使路由随原输入稳定。

保留旧配置版本可读是这个机制的前提。版本不可读时明确报错，让运维恢复正确配置；静默回退到最新配置虽然“看起来恢复了发送”，却可能产生跨Bot错误。
