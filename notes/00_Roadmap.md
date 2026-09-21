# NebulaRPC 总路线

这份目录只服务于两件事：

1. 记录 NebulaRPC 已经真正实现、验证过的技术；
2. 项目成熟后，用这些实现反推面试八股、设计题和二次优化点。

**项目开发本身不再被学习节点、打卡流程或拆任务软件驱动。**

## 能力总路线

```text
Linux Reactor / epoll
    <- MyMuduo 能力迁入

TCP 字节流 / partial read-write
    <- MyMuduo 能力迁入

多线程 / 生命周期 / 并发安全
    <- 旧能力 + NebulaRPC 重构

Protobuf / RPC Protocol
    <- game_rpc_project 能力迁入

Request Correlation / request_id(seq_id)
    <- game_rpc_project 能力迁入

Async RPC
    <- NebulaRPC 新能力

Timeout / Cancel / Race / Exactly-Once Completion
    <- NebulaRPC 新能力

C++20 Coroutine RPC
    <- NebulaRPC 新能力

Backpressure / Resource Limits / Overload Control
    <- NebulaRPC 新能力

Client Runtime / Connection Pool / LB / Retry / Reconnect
    <- NebulaRPC 新能力

Observability / Metrics / Trace / Structured Log / Graceful Shutdown
    <- NebulaRPC 新能力

性能分析 / Benchmark / perf / FlameGraph / contention / allocation
    <- NebulaRPC 新能力

bRPC / gRPC 对照
    <- 最终工程证据
```

## 工作方式

每次只围绕 NebulaRPC 本身增加一个真实能力：设计 -> 写代码 -> 编译 -> 运行 -> 测试 -> 压测/故障验证 -> code review -> commit。

功能完成后，再在 `notes/` 下增加对应文档，总结：核心原理、当前代码路径、常见面试题、踩坑、可以继续优化的点。

因此笔记永远落后于代码，而不是代码跟着笔记走。
