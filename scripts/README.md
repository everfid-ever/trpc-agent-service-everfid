# 脚本入口

`scripts/` 按调用者分层。所有面向开发者的脚本都会自行定位仓库根目录，因而可从
任意工作目录以 `bash /absolute/path/to/script.sh` 调用。

| 目录 | 调用者 | 内容 |
| --- | --- | --- |
| `e2e/` | 开发者 | 故障恢复、在途接管和单主机双节点演练 |
| `compose/` | Compose 容器 | 仅保留容器内部的运行切片入口 |
| `lib/` | 其他脚本 | 零-skip 等无独立业务语义的 helper |

稳定的验收入口以 [README](../README.md) 和
[可靠性功能文档](../docs/feature/reliability-failover/README.md) 为准。新增脚本必须先归类：不要把
一次性调试脚本再放回 `scripts/` 顶层。
