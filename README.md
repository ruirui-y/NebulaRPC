# NebulaRPC

NebulaRPC 是一个 Linux / C++20 RPC 学习与工程化项目。项目不再围绕“学习路线软件”推进，而是直接以可运行、可验证、可压测的 RPC 框架为主线迭代。

当前版本是 **Baseline**：把 MyMuduo 已经掌握的 Reactor/TCP 能力和 game_rpc_project 已经掌握的 Protobuf/RPC/seq_id 能力整理成一个干净的起点，后续所有新能力都直接在这个仓库继续加。

## Baseline 已包含

- Linux epoll Reactor：`EventLoop / Channel / Poller / EPollPoller`
- `eventfd` 跨线程唤醒与 `RunInLoop / QueueInLoop`
- TCP Buffer，使用 `readv` 处理一次读取与缓冲区扩展
- TcpConnection 的 partial-write + `EPOLLOUT` 发送路径
- Acceptor / TcpServer
- EventLoopThread / EventLoopThreadPool，多 Reactor 基础可直接复用
- Protobuf Generic Service RPC
- 长度前缀 RPC framing，支持 TCP 半包/粘包
- `request_id` 请求响应关联
- Baseline 同步 RpcChannel：独立接收线程 + pending promise + timeout
- 服务端异步 `done` 生命周期保护：request/response 在 done 前保持存活
- Echo TCP 示例与最小 Protobuf RPC Echo 示例
- Debug / Release / ASan / UBSan CMake Presets

## 目录

```text
NebulaRPC/
├── nebula/
│   ├── base/
│   ├── net/
│   └── rpc/
├── examples/
│   ├── echo/
│   └── rpc_echo/
├── tests/
└── notes/
```

## 构建

依赖：Linux、CMake >= 3.22、Ninja、C++20 编译器、Protobuf C++ 开发包与 protoc。

```bash
cmake --preset linux-debug
cmake --build --preset build-linux-debug
ctest --preset test-linux-debug
```

ASan：

```bash
cmake --preset linux-asan
cmake --build --preset build-linux-asan
ctest --preset test-linux-asan
```

UBSan：

```bash
cmake --preset linux-ubsan
cmake --build --preset build-linux-ubsan
ctest --preset test-linux-ubsan
```

## 运行 TCP Echo

```bash
./build/linux-debug/bin/nebula_echo_server 9001
```

另一个终端可以直接使用 `nc`：

```bash
nc 127.0.0.1 9001
```

## 运行最小 RPC Echo

服务端：

```bash
./build/linux-debug/bin/nebula_rpc_echo_server 9000
```

客户端：

```bash
./build/linux-debug/bin/nebula_rpc_echo_client 127.0.0.1 9000
```

客户端连续发 3 次请求，用于最小验证 request_id / request-response 路径。

## 当前设计边界

Baseline 的 `RpcChannel` **故意保留同步调用语义**：调用线程等待 future，独立接收线程根据 `request_id` 唤醒对应请求。这是从旧 game_rpc_project 收敛出来的可工作基线，不是最终架构。

下一阶段会直接替换这一层，进入真正的新能力：

```text
EventLoop 驱动的 Async RPC
-> Deadline / Cancel / Exactly-Once Completion
-> C++20 Coroutine RPC
-> Backpressure
-> Client Runtime / LB / Retry
-> Observability
-> Benchmark / Profiling
-> bRPC / gRPC 对照验证
```

学习笔记只记录“项目已经实现的技术如何用于面试复习”，不再反过来驱动项目开发。
