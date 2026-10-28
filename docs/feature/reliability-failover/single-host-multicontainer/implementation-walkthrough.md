# 单主机多容器实例可用性 — 实现走读

## 1. Bootstrap 与文件隔离

Compose 等待共享依赖可用、技能卷初始化和一次性 bootstrap 成功，再启动节点。bootstrap 使用可写技能卷；四个多实例应用 root read_only、技能卷 ro、独立 /tmp tmpfs。ensureSkillDirectory 的读模式只 Stat/isDir；ensureSkillFile 校验内容，缺失即报错。写模式在 stagingRoot/.publishing 创建临时文件，写入 Sync/Close 后 link 到目标，EEXIST 只接受完全相同内容。

此刻被杀：未完成临时文件不进入包摘要；读节点不能靠修改文件“修复”版本。再次 bootstrap 可验证/补齐未发布内容，不能覆盖已发布版本。

## 2. 进程身份和组件 readiness

loadWebUILocalConfig 每次生成新的 process_start_id；instanceName 为 preprocess、worker、dispatch/reply/wakeup Relay、wakeup 和 delivery 分别生成 owner。runtimeHealth 从 starting 开始，真实成功 poll 置 ready，失败置 degraded；beginDrain 后不再接受成功观察恢复 readiness。

组件状态只记录实际操作的结果；空闲 poll 可成功，长模型调用不被误判为死亡。/readyz 还做实际依赖检查；status 中 components_ready 不能替代它。

## 3. 配置、初始探测和路由

| 配置 | 默认值 | 校验 / 行为 |
|---|---|---|
| TRPC_LISTEN_ADDRESS | :8080 | 非空，无首尾空白。 |
| TRPC_WECOM_HA_ENTRY_BACKENDS | 必填 | 至少两个不同 http origin；拒绝 user/query/fragment/path，含尾斜杠路径也拒绝。 |
| TRPC_WECOM_HA_ENTRY_PROBE_INTERVAL | 1s | 100ms–1m。 |
| TRPC_WECOM_HA_ENTRY_PROBE_TIMEOUT | 1s | 100ms–30s。 |
| TRPC_WECOM_HA_ENTRY_REQUEST_TIMEOUT | 2s | 100ms–30s，每后端一次转发。 |
| TRPC_WECOM_HA_ENTRY_RECOVERY_SUCCESSES | 2 | 1–10，仅恢复阶段要求连续成功。 |
| HealthTTL | 派生值 | 3×(interval+probe timeout)，没有单独环境变量。 |

## 稳定入口的当前算法

`wecomHAEntry` 对每个后端并行执行有超时的 `/readyz` 探测。HTTP 200、响应有效且探测 context 未失效才算成功；探测 body 最多读取 1024 字节。每次探测记录开始时的 epoch，较新的失败会递增 epoch，旧结果因 epoch 不匹配被丢弃。

路由须满足健康且 `checked_at` 未过期。初次成功可立即纳入；探测/转发失败或健康过期后需连续成功次数达到阈值。健康有效期为 `3 × (ProbeInterval + ProbeTimeout)`。epoch 是进程内竞态控制字段，不在 /statusz 中公开。

`serveHTTP` 只为 callback 路径做转发。正文上限 2 MiB；按轮转起点选择有效健康节点，每次回调对每个节点最多一次。网络失败、502/503/504、非 4xx 的响应读取失败或超限会摘除该后端并尝试另一节点。4xx 拒绝不切换，4xx body 读取失败时仍返回该状态；客户端取消不据此惩罚后端。完整响应缓冲成功后才写给客户端。

`forward` 保留 RequestURI、正文和签名相关头，`copyHeaders` 去掉逐跳头及 Connection 指定的头，不新增 X-Forwarded-*，也不跟随重定向。入口重试靠后端 durable Inbox 去重；不能代替投递端的幂等协议。

## 4. SIGKILL 后的顺序

入口摘除与任务回收是两条不同链路。入口在探测/转发失败或健康过期后停止选用节点。Worker/Relay/Delivery 依靠 TTL、pending reclaim 和持久扫描接管；它们不等待入口下达执行命令。新的 owner 必须取得有效 lease/claim，旧进程不能复用过期身份提交。

## 5. 重新加入

重新启动同一个槽位会生成新 process_start_id；它重新访问共享依赖和技能包、实际轮询通过后 ready。入口经历故障/过期的 backend 必须累计连续成功，旧 probe 的 epoch 不匹配即忽略。新进程只竞争可领取工作，不抢占存活实例的有效 lease。反向故障用同样路径处理。

## SIGTERM 排空顺序

`signalCtx` 接收 SIGTERM/父 context 取消；活动工作使用独立 `processCtx`。主运行时先 `health.beginDrain()`，再 `drainGate.Begin()`，同时调用 HTTP Shutdown 并等待参与排空的角色退出。业务 HTTP 在排空期间返回 503，live/ready/status 路径可检查；ready 不会被迟到的成功轮询恢复成 true。

Worker、Delivery 和 Wakeup 的读取 context 先取消，活动工作 context 保留。预处理和 Relay 通过 drainPoll 停止下一批；Delivery Supervisor 停止 refresh/新建消费者，runUntilDrained 不重启已排空的消费者。活动进度发布在最终 processCtx 取消时退出。

默认排空 30 秒；Worker/Delivery 的局部超时装配为应用时限加 1 秒，让主运行时先控制全局期限。正常结束后取消 processCtx；超时则报告排空超时、取消工作并退出。未完成的 durable 工作留给存活节点。排空不是共享队列清零，也不会人工删除 lease。SIGKILL 完全跳过本流程。

## 6. 每步 durable truth

| 阶段 | 持久事实 | 再被杀的结果 |
|---|---|---|
| 启动前 | 共享配置、已发布技能、业务表 | 其他进程可继续；新进程尚未 ready。 |
| 正常服务 | Inbox、SDK history、execution/commit/Outbox/Ledger | 参照前两能力恢复；不依赖 /tmp。 |
| 强杀 | 旧 lease/claim、pending entry | 等待到期后接管；无人工删除。 |
| 重加入 | 新 owner，旧业务身份不变 | 已 sent 段不再发，未完成工作仍可回收。 |
| 排空 | 本实例停止新领取，活动 durable 工作 | 完成后退出；超时取消让 peer 按同一规则恢复。 |

## 7. 脚本覆盖的真实范围

single-host-multicontainer.sh 检查基线、A 强杀保持停止、A 新启动身份、B 反向强杀和入口健康，保存 status/Compose/log。默认稳定窗口5秒可配置，不等于持续负载验证。wecom-ha-failover.sh 是同脚本兼容入口，已非独立旧 smoke。脚本不会伪造真实 WeCom 消息；assumed/recorded 是外部证据声明。

## 按生命周期把调用方与持久事实对上

### 容器创建与配置加载

Docker提供每容器独立进程、根文件系统和tmpfs。Compose提供不同宿主端口和instance_id；应用自己生成process_start_id。端口隔离只保证监听共存，数据库和队列的租户隔离仍由业务键实现。

配置loader校验绝对路径、密钥配置、次Bot完整性、drain时限等。无法满足条件时启动失败而不是半配置服务；entry也拒绝包含path/query/user等非origin后端地址。

### Bootstrap发布

volume初始化与bootstrap先完成。bootstrap可写技能内容，运行节点只读；文件内容版本不匹配会阻止节点进入正常运行。配置发布和文件发布都须纳入启动观察，不能把进程存在当成全部ready。

### 组件启动

组件健康从starting开始。预处理、Worker、Relay、Wakeup和Delivery实际poll结果驱动状态；确认过期调度也有观察。需要的账号消费者尚未成功访问队列时，Delivery不ready。

不同角色owner来自同一instance/process前缀但component不同，便于判断到底是执行、发布还是发送责任发生迁移。

## Supervisor如何避免旧消费者状态污染新消费者

Supervisor按ReplyDestination建立消费者，账号/Binding/config版本决定目标。每次创建有generation；健康观察和退出事件携带generation，不能让旧消费者迟到的消息替代新消费者状态。

刷新catalog会添加/移除目标。进入drain后停止刷新和创建，等待现有消费者退出；这保证排空不是“刚停掉一个又被监督器拉起一个”。生产消费者通过WithDrain接收同一信号。

## 入口一次请求的执行路线

```text
读取原callback body（上限2MiB）
  → 计算轮转起点
  → 选择健康且未过期、尚未尝试的backend
  → 为本次backend尝试创建requestTimeout context
  → 转发原RequestURI/body/签名头
     ├─ 网络失败 / 502、503、504 → failBackend，尝试peer
     ├─ 客户端已取消 → 返回，不把取消当backend失败
     ├─ 4xx拒绝 → 返回原状态，不换节点
     └─ 正常响应 → 有界完整读取后写回
  → 没有可用backend → 503
```

这条路线不会代理ops或任意路径，也不会把后端重定向当成可跟随的新callback地址。输入HTTP重试安全依赖Inbox，最终回复安全依赖Ledger和下游。

## 进入drain时各角色分别做什么

| 角色 | 停止什么 | 继续什么 | 截止后 |
|---|---|---|---|
| HTTP | 新业务接入/监听接纳 | 已接纳handler的有界shutdown | process取消/Close结束。 |
| Worker | Consume/Reclaim继续读新任务 | 当前执行及必要提交 | 取消工作，durable责任留待回收。 |
| preprocess/Relay | 下一轮批次 | 已进入RunOnce的批处理 | 全局context取消。 |
| Wakeup | 新consume/reclaim | 已进入的Publish/MarkWoken/ACK | 全局context取消。 |
| Delivery Consumer | 新读取/新处理 | 当前发送和确认 | 未完成发送不以成功ACK。 |
| Delivery Supervisor | refresh和新建consumer | 等待当前消费者 | 不正常重建已drained消费者。 |
| progress publisher | 不单独阻塞draining WaitGroup | 到全局工作取消前可继续 | processCtx取消退出。 |

现有HTTP handler即使在关闭准入前被接纳，也可能在角色停止领取后只保存新的durable工作；这些工作可以由peer处理。排空只保证本实例退出有序，不保证全局所有待办都处理完。

## 文件发布的每一步与崩溃窗口

1. 若目标已存在，读取并比较内容；相同直接成功，不同报错。
2. 只读模式下目标不存在，报错返回；不创建父目录。
3. 写模式创建目标父目录与stagingRoot/.publishing。
4. 在publishing写临时文件，Sync并Close。
5. link临时文件到正式目标，目标已存在时仍核对内容。
6. 清理本次临时文件。

第4步退出可能留下未正式发布临时文件；它位于摘要根之外。第5步后退出，正式目标已有完整内容，后续重复bootstrap按内容校验复用。不用“最后一次启动覆盖前一个版本”作为并发策略。

## 反向故障时重点观察的差异

第一轮强杀N1之后，N2可能暂时成为唯一实际执行者。N1重新加入不要求抢回同样工作；第二轮应重新记录实际owner再强杀N2。

如果只看到N1重新ready而没有原会话和任务证据，不能得出反向业务接管成功。相同机制在两个方向都装配，是代码事实；对应方向在真实部署下是否达时限，是外部验收事实。
