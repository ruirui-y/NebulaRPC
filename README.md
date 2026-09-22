# NebulaRPC

NebulaRPC 是一个 Linux / C++20 RPC 学习与工程化项目。

项目不再围绕“学习路线软件”推进，而是直接以一个可运行、可验证、可扩展的 RPC 框架为主线持续迭代。

当前版本为 **Baseline**：将 MyMuduo 中已经验证的 Reactor/TCP 网络能力，以及 game_rpc_project 中已经验证的 Protobuf/RPC/request_id 能力整理为一个新的工程基础。

后续所有新能力都直接在该仓库演进。

---

## Baseline 已包含

- Linux epoll Reactor
  - EventLoop
  - Channel
  - Poller
  - EPollPoller
- eventfd 跨线程唤醒
- RunInLoop / QueueInLoop
- TCP Buffer 与 readv 读取扩展
- TcpConnection partial-write + EPOLLOUT 发送路径
- Acceptor / TcpServer
- EventLoopThread / EventLoopThreadPool
- Protobuf Generic Service RPC
- RPC framing，支持 TCP 半包/粘包
- request_id 请求响应关联
- RPC Server 基础调用链
- Echo TCP 示例
- Protobuf RPC Echo 示例
- TimerQueue / timerfd 基础定时能力
- Debug / Release / ASan / UBSan CMake Presets

---

## 第三方依赖

NebulaRPC 使用系统级第三方依赖，不直接 vendor 第三方源码。

Ubuntu 22.04：

```bash
sudo apt update

sudo apt install -y     cmake     ninja-build     g++     protobuf-compiler     libprotobuf-dev     libspdlog-dev
```

依赖说明：

| 依赖     | 用途                          |
| -------- | ----------------------------- |
| protobuf | RPC 消息序列化与 Service 定义 |
| spdlog   | 工程日志系统                  |
| cmake    | 构建系统                      |
| ninja    | 快速构建                      |
| g++      | C++20 编译                    |

---

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

---

## 构建

Debug：

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

---

## 运行 TCP Echo

服务端：

```bash
./build/linux-debug/bin/nebula_echo_server 9001
```

客户端：

```bash
nc 127.0.0.1 9001
```

---

## 运行最小 RPC Echo

服务端：

```bash
./build/linux-debug/bin/nebula_rpc_echo_server 9000
```

客户端：

```bash
./build/linux-debug/bin/nebula_rpc_echo_client 127.0.0.1 9000
```

用于验证：

- TCP 长连接
- Protobuf RPC
- request_id 请求响应匹配

---

## 当前开发阶段

当前 Baseline 已完成：

```text
Reactor TCP Runtime
        +
基础 Protobuf RPC
        +
Async RPC 初步改造
```

后续主要演进：

```text
Async RPC
    ↓
Timeout / Deadline / Cancel
    ↓
Exactly-Once Completion
    ↓
C++20 Coroutine RPC
    ↓
Backpressure
    ↓
Client Runtime / LB / Retry
    ↓
Observability
    ↓
Benchmark / Profiling
    ↓
bRPC / gRPC 对照验证
```

---

## 项目笔记原则

notes 目录只记录：

- 已实现技术
- 架构设计
- 面试复习内容
- 工程踩坑记录

不会再使用笔记反向驱动代码开发。

开发流程：

```text
设计功能
    ↓
实现代码
    ↓
测试验证
    ↓
整理技术笔记
```