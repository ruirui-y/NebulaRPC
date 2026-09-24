# Notes

这里不是开发任务系统，只是 NebulaRPC 的“项目实现 -> 面试复习”知识库。

规则：一个已经完成并验证的核心功能，对应一篇文档。未实现功能不提前写大段八股。

## 索引

| 文档 | 内容 | 状态 |
|---|---|---|
| `00_Roadmap.md` | 整个项目能力地图（分节推进计划 + 完成度） | 持续维护 |
| `01_Baseline.md` | 当前 Reactor / TCP / Protobuf RPC 基线 | 已实现 |
| `02_Async_RPC.md` | 异步 RPC 与回调链路 | 已实现 |
| `03_TimerQueue.md` | 时间轮 / 定时器队列 | 已实现 |
| `04_Timeout_Exactly_Once.md` | RPC Timeout 与 Exactly-Once Completion 初版 | 已实现 |
| `05_工程路线_代码对齐版.md` | 工程路线逐项核对（以真实代码为准，标出半成品与假测试） | 持续维护 |
| `06_Timeout_Cancel_ExactlyOnce_实现细节.md` | Phase 7：RpcCall 状态机接线 / Cancel / 竞争测试 / 四处生命周期加固 | 已实现（待 ASan 验收） |

## 阅读顺序

先看 `00_Roadmap.md` 的能力地图，再看 `05` 的校准总表确认「哪些真的做完了」，
其余按 Phase 编号顺序读。
