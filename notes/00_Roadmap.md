# NebulaRPC 技术路线总览

## 1. Linux Reactor / epoll

来源：

    MyMuduo 能力迁入

目标：

-   理解 Reactor 模型
-   epoll 事件分发
-   EventLoop 生命周期
-   Channel / Poller 关系
-   网络事件如何进入业务逻辑

NebulaRPC 中对应：

    EventLoop
    Channel
    Poller
    EPollPoller

------------------------------------------------------------------------

## 2. TCP 字节流 / partial read-write

来源：

    MyMuduo 能力迁入

目标：

-   TCP 无消息边界
-   半包/粘包处理
-   Buffer 设计
-   非阻塞读写
-   EPOLLOUT 发送流程

对应：

    Buffer
    TcpConnection

------------------------------------------------------------------------

## 3. 多线程 / 生命周期 / 并发安全

来源：

    旧能力 + NebulaRPC 重构

目标：

-   EventLoop 线程归属
-   fd 生命周期
-   shared_ptr/weak_ptr 使用
-   对象销毁时机
-   跨线程任务投递

------------------------------------------------------------------------

## 4. Protobuf / RPC Protocol

来源：

    game_rpc_project 能力迁入

目标：

-   protobuf service
-   RPC message framing
-   编解码流程
-   服务注册与调用分发

------------------------------------------------------------------------

## 5. Request Correlation / request_id(seq_id)

来源：

    game_rpc_project 能力迁入

目标：

-   多请求并发关联响应
-   pending request 管理
-   response 匹配 request

核心：

    request_id
        ↓
    PendingCall
        ↓
    Response

------------------------------------------------------------------------

## 6. Async RPC

来源：

    NebulaRPC 新能力

目标：

-   移除阻塞等待模型
-   Reactor 驱动 RPC
-   callback completion
-   pending call 生命周期

------------------------------------------------------------------------

## 7. Timeout / Cancel / Race / Exactly-Once Completion

来源：

    NebulaRPC 新能力

目标：

-   RPC 超时
-   请求取消
-   Response/Timeout 竞争
-   保证一次完成

核心问题：

    Response
         \
          -> Complete()
         /
    Timeout

只能执行一次。

------------------------------------------------------------------------

## 8. C++20 Coroutine RPC

来源：

    NebulaRPC 新能力

目标：

-   callback 转 coroutine
-   co_await RPC
-   coroutine 生命周期
-   resume 调度线程

------------------------------------------------------------------------

## 9. Backpressure / Resource Limits / Overload Control

来源：

    NebulaRPC 新能力

目标：

-   输出缓冲限制
-   请求数量限制
-   过载保护
-   服务降级

------------------------------------------------------------------------

## 10. Client Runtime / Connection Pool / LB / Retry / Reconnect

来源：

    NebulaRPC 新能力

目标：

-   客户端连接管理
-   长连接复用
-   负载均衡
-   重试策略
-   故障恢复

------------------------------------------------------------------------

## 11. Observability / Metrics / Trace / Structured Log / Graceful Shutdown

来源：

    NebulaRPC 新能力

目标：

-   spdlog 日志
-   指标采集
-   请求追踪
-   优雅关闭

------------------------------------------------------------------------

## 12. 性能分析 / Benchmark / perf / FlameGraph / contention / allocation

来源：

    NebulaRPC 新能力

目标：

-   QPS
-   latency
-   CPU 分析
-   内存分析
-   锁竞争
-   分配优化

------------------------------------------------------------------------

## 13. bRPC / gRPC 对照

来源：

    工程验证模块

目标：

-   同场景测试
-   架构对比
-   性能差异分析
-   设计取舍总结

------------------------------------------------------------------------

## 项目推进原则

    先实现功能
        ↓
    测试验证
        ↓
    整理代码
        ↓
    补充技术笔记
        ↓
    形成面试材料

笔记服务于已经完成的项目能力，不反向驱动开发。
