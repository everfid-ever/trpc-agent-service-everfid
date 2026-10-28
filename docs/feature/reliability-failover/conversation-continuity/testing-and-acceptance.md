# 故障恢复后继续对话 — 测试与验收

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

## 分阶段步骤

1. 只启动 N1，N2 停止。确认两个 Bot 路由、组件 ready；各自写代号，随后不带代号回忆，核对服务端 tenant/session/最终交付。
2. 启动 N2，记录两个不同 process_start_id，观察完整 W_BASELINE。两租户重叠提问，仍使用原会话。当前 callback 架构记录入口 healthy backends 与 Worker/Delivery 实际 owner，不填写不存在的 Bot 长连接 owner。
3. 等待基线测试输入全部最终交付后，非优雅终止实际承载职责的节点，记录 t0，确认存活进程及共享依赖未重启。
4. 不执行人工修复，观察 /readyz、/statusz 和 /ops/session/lease，记录入口重新可转发、Worker/Delivery 可工作时刻。超过阈值按本轮失败/未恢复记录。
5. 确认接管完成后分别发带新RUN_ID标记的消息：“你属于哪个租户？我此前告诉你的代号是什么？”不得再次提示代号。
6. 核对各自原 session、正确代号、冻结 reply route、接管者和客户端最终回复。W_REPEAT 内至少再一轮，两租户请求重叠，检查迟到重复与积压。
7. 如实现提供明确创建新会话方式，另列一轮新会话不读取旧代号；关闭聊天窗口不算创建新会话。

## 判定表与反例

| 项目 | 满足条件 | 反例 |
|---|---|---|
| 原会话连续 | 同tenant/app/session读持久历史，正确回忆代号 | 换会话、问题中重带代号或仅模型自报租户。 |
| 自动接管 | 受害实例一直停止，存活实例实际接收/执行/投递 | Docker自动重建或手删租约后成功。 |
| 租户隔离 | Binding、payload、SDK key、结果和路由同tenant | A回复B代号/账号，跨租户解密。 |
| 最终交付 | 客户端与Ledger/原request一致，无迟到重复 | 仅有进度消息、只看一行sent。 |
| 窗口范围 | 故障在基线完全交付之后 | 处理中的任务故障却只验证后来新消息。 |
| P4协议边界 | 无能力时如实异常待处置 | ambiguous被写成无条件自动完成。 |

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

## 自动脚本的覆盖范围

`bash scripts/e2e/single-host-multicontainer.sh` 验证生命周期和入口；`wecom-ha-failover.sh` 直接调用同脚本。TRPC_WECOM_REAL_ACCEPTANCE=assumed 默认仅声明真实会话是外部前提；recorded 也只声明外部证据已保存，脚本不检查客户端对话。两者都不能独立证明代号回忆正确。

## 代码回归入口

```sh
go test ./...
go test -race ./cmd/trpc-service ./trpcservice/worker ./trpcservice/channels/delivery ./trpcservice/relay ./trpcservice/reliability/drain
```

这些命令运行包/竞态测试，不等价于真实 WeCom 故障验收。若某集成测试依赖外部服务，应检查是否实际运行或跳过，不能把普通 go test 退出0视为全部部署验证完成。测试结果、日志与时延保存在独立运行目录，不回填本文。

## 把双Bot业务消息安排成可复查序列

以下仅为用例模板，尖括号内容在每轮生成，不把真实代号写入系统提示、知识库或配置。两租户使用不同随机代号，并记录原external_message_id。

| 阶段 | Bot A | Bot B | 本阶段证明什么 |
|---|---|---|---|
| 单节点写入 | RUN-S1-A：记住TOKEN_A并回显标记。 | RUN-S1-B：记住TOKEN_B并回显标记。 | 原身份和持久会话建立。 |
| 单节点回忆 | 不带TOKEN，询问此前代号。 | 同左，属于B。 | 代号不是靠提问回显。 |
| 双节点基线 | RUN-S2-A：回忆代号和租户。 | RUN-S2-B：回忆代号和租户。 | 加入实例不改变原会话。 |
| 故障后首轮 | RUN-S3-A：回忆代号并回显标记。 | RUN-S3-B：回忆代号并回显标记。 | 接管后原会话可继续。 |
| 持续第二轮 | 新标记，两边重叠请求。 | 新标记，两边重叠请求。 | 不是偶然一次成功，且无迟到重复。 |

用户在企业微信关闭再打开聊天窗口通常仍是同一会话；若没有明确服务端新session动作，不把它记录成“新会话隔离通过”。

## 每一轮至少保存一张身份对照表

| 字段组 | 期望关系 | 不通过或覆盖缺口 |
|---|---|---|
| Bot/Binding/tenant | A始终对应A，B始终对应B | 只凭模型口头回答。 |
| app/user/session | 故障前后原会话键相同 | 故障后创建空历史新session。 |
| 消息/request/input_seq | 新输入各有身份，原消息不重复建任务 | 同external_message_id产生第二个业务request。 |
| owner/process/fence | 从故障实例迁移至存活实例，执行权有效 | 同一个旧process偷偷重启恢复。 |
| result/ReplyRoute | 结果和目的地属于同tenant/request | 内容正确却发送到另一个Bot。 |
| Ledger/客户端 | 服务端状态与可见回复一致 | 仅一个sent行就声明绝无重复。 |

明细表保存在运行目录，可以只用脱敏哈希引用完整个人标识。身份对照是证据关联，不要求把密钥或会话全文导出。

## 只读SQL证据示例

以下使用psql变量，先通过安全方式设置tenant_id和request_id。仅查询元数据，不解密聊天正文；示例不替代客户端记录。

```sql
SELECT request_id, agent_app_id, session_id, input_seq, state, result_ref
FROM inbox
WHERE tenant_id = :'tenant_id' AND request_id = :'request_id';

SELECT agent_app_id, session_id, last_fence, version, next_input_seq
FROM session_head
WHERE tenant_id = :'tenant_id';

SELECT request_id, input_seq, fence, outcome, result_ref
FROM session_commit
WHERE tenant_id = :'tenant_id' AND request_id = :'request_id'
ORDER BY created_at;

SELECT delivery_key, segment_no, segment_count, state, attempt,
       client_request_id, provider_message_id, last_error_class
FROM delivery_ledger
WHERE tenant_id = :'tenant_id'
ORDER BY updated_at DESC;
```

最后一个查询返回该租户最近投递；应通过原Outbox幂等键把delivery_key关联到request，而不是只按答案文本猜是哪一次回复。一个租户同时进行其他测试时尤其需要这层关联。

## 如何记录时间而不夸大结论

先记录外部强杀动作与容器退出时刻，再记录入口可用和组件ready的首次观察。轮询每DELTA_POLL采样一次，因此实际恢复发生在上次失败与首次成功之间。

故障后人工等待一分钟才发消息，即使秒回，也不能声称业务RTO为一秒。应分别报告“首次观察到ready距故障多久”和“接管后请求响应多久”，并标注采样误差。

本能力基线要求旧测试输入已交付；若故障时还有旧任务运行，就需要在途验收补齐，不能将它们忽略后仍声称覆盖所有恢复窗口。

## 常见伪通过与纠正动作

| 伪通过 | 为什么无效 | 正确核对 |
|---|---|---|
| 模型回答“我是租户A” | 文本可来自提示，无服务端归属证据 | 查询Binding/tenant/route。 |
| 问题里带代号并得到回显 | 没证明历史恢复 | 后续提问不带代号。 |
| 重绑Bot后正常 | 改变了被测路由 | 保持原Binding配置。 |
| 重发原消息后有回复 | 不能证明原任务自动续办 | 保持原request不变。 |
| 重启peer后正常 | 无法归因自动接管 | peer原进程持续运行。 |
| 未出现错误日志 | 缺少业务完成证据 | terminal、Ledger与客户端关联。 |

## 结论模板

```text
版本/镜像/配置/迁移：
RUN_ID与证据目录：
故障实例及process_start_id：
存活实例及process_start_id：
故障时刻、ready观察区间、采样间隔：
Bot A/B原session及服务端tenant：
首轮与重复轮客户端最终回复：
未验证窗口/下游P4边界：
结论：通过 / 未通过 / 有明确覆盖缺口
```

不要在未执行时填入估算成功值。代码回归、生命周期脚本和真实对话证据各自说明覆盖范围。
