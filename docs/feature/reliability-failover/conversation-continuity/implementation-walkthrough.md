# 故障恢复后继续对话 — 实现走读

## 按运行时顺序走读

| 顺序 | 模块 / 函数 | 本步权威状态 | 此刻被杀会怎样 |
|---|---|---|---|
| 1 | loadWebUILocalConfig、bootstrapWebUILocal | 版本化配置、Bot route、租户密钥、已发布技能 | 未 ready；peer 使用既有共享配置，不接管内存 bootstrap 栈。 |
| 2 | identity.Mapper.Map、渠道验签入口 | verified Binding；tenant scoped user/session | durable 接收前不能宣称已纳入恢复保证。 |
| 3 | Inbox/payload 与预处理入库 | Inbox 唯一身份、加密原输入、preprocess_job、ReplyRoute | 共享扫描领取未完成 job，用户不重发。 |
| 4 | prepare_dispatch 与 Dispatch Relay | 原 input_seq、execution_record、dispatch Outbox | 重复 dispatch 复用序号；发布后未标记可重复 event。 |
| 5 | worker.Consumer.handle | Redis lease/fence；BeginExecutionAttempt | 失权后 pending reclaim；新尝试 supersede 遗留 running。 |
| 6 | RunnerExecutor.ExecuteWithLease | session_head / 官方 SDK 历史 / 输入与版本 | 已 terminal 识别完成；输入未就绪则停放/唤醒，不强行越序。 |
| 7 | frozen result 检查 | tenant/request/key/digest 验证通过的 result_payload | 已冻结时续提交，不再次运行模型。 |
| 8 | run.Run 与 GuardedCallable | SDK session_events、工具账本/密文 | SDK 事件可能已写；平台 rollback 不撤销；工具按恢复契约。 |
| 9 | PutResult → commitFrozenSuccess | 冻结答案；beforeCommit；terminal/Outbox 事务 | result 已写未提交时复用；事务提交后不重跑模型。 |
| 10 | Reply Relay.PublishReply | reply Outbox 与账号 reply stream | 先发布后标记，可重复发布同一 DeliveryKey。 |
| 11 | delivery.Service.Deliver | 分段计划与 Ledger claim | 只有成功 claim 才发送；expired sending 进入不确定路径。 |
| 12 | adapter.Deliver → FinishDelivery | provider 接受消息 ID、Ledger sent | 接受后确认前为 P4；不得因超时判断未发送。 |
| 13 | Broker.Ack / AckReply | terminal/停放等 durable disposition；发送终态 | 未完成执行失败留 pending；发送明确失败可被 ACK，靠异常记录处置。 |

注意 ACK 不全等价于“用户答案完成”：Worker 对已持久停放/阻塞等 dispositions 可正常返回后 ACK，Wakeup/协调记录负责后续推进；Delivery 的 TerminalError 也允许在 failed 持久化后 ACK。审查时要检查 durable 后续责任，不能只查 ACK。


## 身份与原会话恢复的强制点

Mapper 从可信 Binding 取 tenant，单聊以 channel/account/user 派生 session，群聊以 chat 派生。identity/session 使用不同 purpose 的租户密钥。SDK TenantKey 把 tenant/app 编码到 app_name，checked facade 对 app/user/session 任一不匹配拒绝。ReplyRoute 随持久 payload 固定，接管者不能用当前模型输出选择 Bot。

官方会话 service 关闭 async persist，事件同步写入。BufferedTurn.Commit 明确清空平台 Events/StateDelta/SummaryCandidate，避免重复真相源；Rollback 只丢弃进程内协调元数据，不撤销 SDK 写入。故障重跑可能附加第二次同内容输入，因此终态幂等与历史事件去重不能混为一谈。

## 执行恢复分支

| 读到的事实 | 本次动作 |
|---|---|
| 原输入已有 terminal | 返回原终态，避免再次调用模型。 |
| terminal 未提交，result_payload 已冻结 | 校验租户/请求/密钥版本/摘要，检查取消和执行权，commitFrozenSuccess。 |
| 模型尚无完整结果 | 以同一原输入恢复调用；模型计数可增加。 |
| 工具成功密文已保存 | 相同逻辑工具重新进入时 LoadResult 复用。 |
| 工具效果未确定 | 工具 lease 到期后按声明策略恢复或安全终结。 |

## SIGTERM 排空顺序

`signalCtx` 接收 SIGTERM/父 context 取消；活动工作使用独立 `processCtx`。主运行时先 `health.beginDrain()`，再 `drainGate.Begin()`，同时调用 HTTP Shutdown 并等待参与排空的角色退出。业务 HTTP 在排空期间返回 503，live/ready/status 路径可检查；ready 不会被迟到的成功轮询恢复成 true。

Worker、Delivery 和 Wakeup 的读取 context 先取消，活动工作 context 保留。预处理和 Relay 通过 drainPoll 停止下一批；Delivery Supervisor 停止 refresh/新建消费者，runUntilDrained 不重启已排空的消费者。活动进度发布在最终 processCtx 取消时退出。

默认排空 30 秒；Worker/Delivery 的局部超时装配为应用时限加 1 秒，让主运行时先控制全局期限。正常结束后取消 processCtx；超时则报告排空超时、取消工作并退出。未完成的 durable 工作留给存活节点。排空不是共享队列清零，也不会人工删除 lease。SIGKILL 完全跳过本流程。

## 关键模块的真实入口

| 模块 | 当前代码 |
|---|---|
| 运行时装配、Bot 配置、故障端点 | cmd/trpc-service/webui_local_role.go |
| 就绪与排空 | cmd/trpc-service/runtime_health.go、runtime_drain.go |
| 渠道身份映射 | trpcservice/channels/identity/identity.go |
| Worker 执行权和 ACK | trpcservice/worker/consumer.go |
| 原结果恢复与提交 | trpcservice/worker/runner.go |
| SDK 作用域与同步历史 | trpcservice/storage/session/postgres/official.go、storage/session/buffered_turn.go |
| 工具恢复 | trpcservice/tool/durable.go、tool/execution/postgres/ |
| 发送、claim 续期与核对 | trpcservice/channels/delivery/service.go |
| 发送消费与监督器 | trpcservice/channels/delivery/consumer.go、supervisor.go |
| 冻结结果/Outbox/Ledger/处置 | trpcservice/storage/messaging/postgres/ |
| 稳定 callback 入口 | cmd/trpc-service/wecom_ha_entry_role.go |

## 从入口到SDK读取：逐步核对表

### 步骤一：定位可信Binding

外部route key只能定位一个候选路由，不应直接被当成租户授权。协议验签需要使用对应版本的渠道密钥；验签通过后Binding中的tenant、account、app才参与后续映射。原始正文中出现tenant名称不改变这条可信路径。

审查时应同时看路由解析和验签结果，不能只看到URL里有local-wecom就认定租户归属正确。次Bot独立路由与密钥作用域也必须同时存在。

### 步骤二：派生稳定user/session

identity.Mapper.Map先校验Binding和event的channel/account一致，并要求外部用户存在。随后按密钥purpose解析identity和session密钥。单聊与群聊使用不同字段组合，最终内部标识不包含节点ID。

本步本身主要是确定性计算。它的输出进入持久记录后才成为恢复链路的一部分；节点在计算中退出，不应伪造持久接收成功。

### 步骤三：保存输入和调度事实

Inbox唯一键合并渠道重传，payload保存原输入，preprocess_job保存下一步工作。异步执行读取这些记录，不依赖接收handler留在内存中的参数。

恢复者校验同一个外部消息是否出现内容碰撞。如果把同一外部消息ID配上不同输入内容，应拒绝而不是静默覆盖。否则合法重试会变成可篡改原任务的入口。

### 步骤四：按原输入分配会话顺序

prepare_dispatch把输入接入session顺序并固定execution envelope及版本。重复处理同一原请求时复用已分配input_seq，而不是让原消息在每次节点故障后占一个新序号。

后续消息早于前序完成到达Worker时，可以被durable停放；唤醒链路负责重新激活。只看到原Redisentry被ACK不足以判断消息丢失，要查它是否已有停放/阻塞等后续责任。

### 步骤五：获得执行权再打开会话

Consumer读取持久last_fence，校准Redis后Acquire。handle启动取消观察和续租，beforeCommit再次确认租期。取得会话lease不等于跳过配置校验，Runner仍读取权威envelope并核对固定版本。

OpenForRun检查当前输入是否可执行；已终态时读取原terminal返回。没有终态才建立SDK作用域facade，防止runner以其他app/user/session读写。

## 运行期间哪些信息会立即持久化

| 内容 | 写入时机 | 是否与terminal原子 |
|---|---|---|
| SDK收到的用户/助手/工具等事件 | SDK同步持久化路径 | 否。 |
| 原输入、预处理产物 | 相应接收/预处理阶段 | 不与最终terminal同事务。 |
| 工具执行状态与结果 | 工具claim/Complete | 成功与该工具密文同事务，不与最终答案同事务。 |
| 渲染后答案 | PutResult | 在terminal之前单独冻结。 |
| terminal、会话顺序推进、reply Outbox | turn.Commit/commit_turn | 是，属于平台协调事务。 |
| provider接受与Ledger确认 | 先外部调用、再本地确认 | 否，形成P4。 |

这张表用于判断“现在被杀会丢什么”。例如SDK已出现助手文本但还没有result_payload/terminal时，该文本不是已交付答案；恢复不能仅凭这段历史认定已完成。

## 逐个恢复分支的返回语义

### 已终态

OpenForRun返回ErrAlreadyTerminal，Runner读取GetTerminalByInputSeq确认原terminal可取。成功返回后delivery可以被确认，不再次创建模型运行。若原terminal读取异常，应暴露错误，不能直接吞掉当成成功。

### 等待确认

存在pending confirmation时，Runner可正常返回，后续责任在确认流程中。execution_attempt.completed因此不等于整个任务succeeded。确认批准/消费后的continuation另走工具恢复和后续模型运行。

### 冻结结果存在

Runner比对result的tenant、request、ref、key version和digest。匹配才调用commitFrozenSuccess。这个函数再次读Tasks.GetExecution，验证权威envelope和CancelRequested，再执行beforeCommit。

这里不重新把旧SDK事件复制进平台session_event，也不重跑模型。若校验失败，保留错误而不是选择“以当前输入覆盖旧答案”。

### 普通模型/工具运行

没有可复用最终结果才进入Run。模型可根据持久历史重新推理，普通工具依靠稳定operation身份复用或恢复。若新推理改变工具参数，则按不同逻辑操作处理，业务工具须明确约束业务键。

## 回复路由最后一跳的核对

ReplyRelay读取冻结结果和原ReplyRoute，要求两者tenant/request一致、Binding/account/config version有效，然后构造ReplyDestination与ReplyEvent。这样发送者不必与接收者或Worker同节点。

Delivery再根据event的版本化Binding解析adapter。不能把当前“默认Bot”代替原Binding；默认配置变化不能改变已经受理输入的回复去向。

## 用一个例子读日志与状态

假设日志出现request=r-demo，Worker尝试从N1换为N2，lease fence从41变为42。接着看到相同result_ref和reply DeliveryKey，只说明内部身份与答案保持一致。

还需核对两个事实：SDK会话键未变，最终回复账号也未变。若只看到新Worker ID而没有session和route证据，仍无法判断这次接管是否保持原会话连续性。此例的ID和数字仅用于说明字段关系。
