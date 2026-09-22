# 02_Async_RPC

## 1. 这一阶段要解决什么

NebulaRPC Baseline 已经具备：

- Reactor 网络层
- TCP 长连接
- Protobuf RPC
- `request_id` 请求/响应关联
- 同步 RPC 调用

但 Baseline 的客户端 RPC 仍然带有同步等待模型：

```text
CallMethod()
    ↓
发送 request
    ↓
等待 response
    ↓
返回
```

旧 `game_rpc_project` 中，这类逻辑通常依赖：

```text
ReceiverThread / recv
        +
promise / future
        +
wait_for
```

这能完成 RPC，但不适合作为高并发异步 RPC Runtime 的最终模型。

这一阶段的目标是：

> 把 RPC Client 从“阻塞等待响应”改造成“由 Reactor 驱动的 Event-Driven Async RPC”。

最终调用链：

```text
业务代码
    ↓
RpcChannel::CallMethod()
    ↓
生成 request_id
    ↓
注册 PendingCall
    ↓
TcpConnection::Send()
    ↓
CallMethod 立即返回
    │
    │ 网络事件稍后到达
    ▼
EventLoop / EPOLLIN
    ↓
TcpConnection::HandleRead()
    ↓
RpcCodec::Decode()
    ↓
根据 request_id 找到 PendingCall
    ↓
反序列化 response
    ↓
完成调用
    ↓
done->Run()
```

核心变化只有一句话：

> 发起 RPC 和完成 RPC 不再发生在同一个同步调用栈中。

---

## 2. 为什么同步 `future.wait_for()` 不适合作为最终方案

同步等待模型最容易理解：

```cpp
SendRequest();
auto result = future.wait_for(timeout);
```

但是它有几个明显问题。

### 2.1 阻塞线程

每个正在等待 RPC 的调用都会占住一个线程。

高并发场景：

```text
1000 个并发 RPC
    ↓
大量线程阻塞等待
    ↓
线程调度 / 栈空间 / 上下文切换成本增加
```

RPC Runtime 的目标应该是：

```text
少量 Reactor 线程
    ↓
管理大量连接和大量 in-flight RPC
```

而不是：

```text
一个请求
    ↓
阻塞一个线程
```

### 2.2 Reactor 线程绝不能等待 RPC 响应

如果在 EventLoop 线程里执行：

```cpp
future.wait();
```

那么整个 Reactor 会停止处理：

```text
accept
read
write
timer
其他连接
其他 RPC response
```

这会形成非常严重的自锁式阻塞：

```text
EventLoop 等 response
       ↑
response 又必须由 EventLoop 读取
```

因此：

> Reactor Thread 只能发起异步操作，不能同步等待网络完成事件。

### 2.3 后续功能很难组合

后面 NebulaRPC 还要支持：

```text
Timeout
Cancel
Exactly-Once Completion
Coroutine
Retry
Backpressure
```

这些能力本质上都需要一个明确的“RPC 调用状态对象”。

同步 `future.wait_for()` 只是等待结果，并不能很好地表达：

```text
Pending
Completed
TimedOut
Cancelled
Failed
```

所以需要 `PendingCall`。

---

## 3. 为什么必须有 request_id

TCP 是字节流，不存在“这是第几个 RPC”的天然边界。

而且多个 RPC 可以同时在一个 TCP 连接上飞行：

```text
request #1001 ───────►
request #1002 ───────►
request #1003 ───────►

                  ◄──── response #1002
                  ◄──── response #1001
                  ◄──── response #1003
```

响应顺序不应该被假设为和请求顺序完全一致。

因此每个 RPC 必须有唯一标识：

```cpp
uint64_t request_id;
```

发请求时：

```text
request_id = NextRequestId()
```

并注册：

```text
pending_calls_[request_id] = PendingCall
```

收到响应时：

```text
response.request_id
        ↓
pending_calls_.find(request_id)
```

这样才能找到：

```text
这个 response 属于哪个 protobuf response 对象？
哪个 controller？
哪个 done callback？
```

所以：

> `request_id` 是多路复用 RPC 的关联键。

---

## 4. PendingCall 是什么

第一版可以理解为：

```cpp
struct PendingCall
{
    google::protobuf::Message* response{};
    google::protobuf::RpcController* controller{};
    google::protobuf::Closure* done{};
};
```

它不是“网络数据”。

它代表：

> 一次已经发出去、但还没有完成的 RPC 调用上下文。

典型状态：

```text
CallMethod()
    ↓
构造 PendingCall
    ↓
放入 pending_calls_
    ↓
发送请求
```

之后原调用栈可以结束。

等待网络事件到来后：

```text
OnMessage()
    ↓
找到 PendingCall
    ↓
写入 response
    ↓
done->Run()
    ↓
删除 PendingCall
```

因此 `PendingCall` 是同步调用栈和异步完成事件之间的桥梁。

---

## 5. 为什么先补 TcpClient / Connector

当前 NebulaRPC 服务端已经建立在 Reactor 上：

```text
Acceptor
    ↓
TcpServer
    ↓
TcpConnection
    ↓
EventLoop
```

如果客户端仍然使用：

```cpp
::socket()
::connect()
::send()
::recv()
```

那么 RPC Client 实际上仍然绕开了 NebulaRPC 自己的网络 Runtime。

真正的异步客户端应该是：

```text
Connector
    ↓
非阻塞 connect
    ↓
EPOLLOUT
    ↓
TcpClient
    ↓
TcpConnection
    ↓
EventLoop
```

所以 Async RPC 前必须把旧 MyMuduo 已经实现过的客户端 Reactor 能力迁移进 NebulaRPC。

这不是重新学习 Reactor，而是复用已经掌握的基础设施。

---

## 6. 非阻塞 connect 为什么关注 EPOLLOUT

非阻塞 socket 调用：

```cpp
connect(fd, ...)
```

通常可能返回：

```text
-1
errno = EINPROGRESS
```

这并不代表连接失败。

它表示：

> TCP 三次握手正在异步进行。

之后把 fd 注册到 epoll，关注可写事件：

```text
EPOLLOUT
```

当 socket 变为 writable 后，还不能直接认为连接成功。

必须：

```cpp
getsockopt(fd, SOL_SOCKET, SO_ERROR, ...)
```

判断最终结果。

因此：

```text
connect()
    ↓ EINPROGRESS
Channel EnableWriting
    ↓
EPOLLOUT
    ↓
getsockopt(SO_ERROR)
    ↓
0 -> connect success
非 0 -> connect failed
```

这是 `Connector` 的核心职责。

---

## 7. RpcChannel 改造后的职责

同步版 `RpcChannel` 可能承担：

```text
建 socket
connect
send
recv
等待 future
解析 response
```

职责过多。

异步版应该收敛为：

```text
RpcChannel
├── 管理 RPC request_id
├── 管理 PendingCall
├── 编码请求
├── 通过 TcpConnection 发送
├── 接收 RpcCodec 解码结果
└── 完成对应调用
```

TCP 生命周期交给：

```text
TcpClient
TcpConnection
Connector
EventLoop
```

RPC 层不再自己直接调用：

```cpp
::send()
::recv()
```

这就是网络层和 RPC 层真正解耦。

---

## 8. pending_calls_ 为什么第一版不加 mutex

一个直接方案是：

```cpp
std::mutex mutex_;
std::unordered_map<uint64_t, PendingCall> pending_calls_;
```

然后所有访问都加锁。

但 NebulaRPC 第一版更适合使用 Reactor 的线程归属原则：

> RpcChannel 的可变状态只在所属 EventLoop 线程访问。

也就是说：

```text
业务线程
    ↓
RunInLoop / QueueInLoop
    ↓
EventLoop Thread
    ↓
注册 PendingCall
    ↓
Send
```

响应：

```text
EPOLLIN
    ↓
EventLoop Thread
    ↓
OnMessage
    ↓
查 pending_calls_
```

两边都在同一个 EventLoop Thread：

```text
pending_calls_
```

自然不需要 mutex。

好处：

- 状态机更简单
- 避免锁竞争
- Timeout/Cancel 更容易统一
- Coroutine resume 线程语义更清晰
- 更符合 Reactor 的 owner-thread model

需要注意：

> “没有 mutex”不代表“线程安全问题不存在”，而是通过线程约束消灭共享并发访问。

---

## 9. CallMethod 为什么必须快速返回

异步版本中：

```cpp
stub.Echo(..., done);
```

调用后应该只完成：

```text
构造请求
注册 PendingCall
排队发送
```

然后立即返回。

不能：

```text
发送
↓
等 response
↓
返回
```

测试 Async RPC 时，必须验证“非阻塞”这一事实。

例如：

```cpp
stub.Echo(controller, &request, &response, done);

std::cout << "CallMethod returned" << std::endl;
```

如果真正异步，顺序应该表现为：

```text
CallMethod returned
...
RPC done callback
```

而不是调用 `Echo()` 时一直卡住。

---

## 10. Callback 在哪个线程执行

这是 Async RPC 非常关键的设计问题。

第一版 NebulaRPC 约定：

> `done->Run()` 在 RpcChannel 所属 EventLoop 线程执行。

原因是 response 本来就在：

```text
EventLoop
    ↓
TcpConnection::HandleRead
    ↓
RpcChannel::OnMessage
```

中被处理。

因此直接完成：

```text
Parse response
erase PendingCall
done->Run()
```

线程语义最简单。

但是这也意味着：

> done callback 不能执行长时间阻塞任务。

否则：

```text
done 阻塞
    ↓
EventLoop 被阻塞
    ↓
所有连接网络事件停止处理
```

以后如果业务需要，可以再增加：

```text
CompletionExecutor
WorkerPool
Coroutine Scheduler
```

把 completion 切换到其他执行器。

第一版先保持语义明确。

---

## 11. Response 生命周期为什么容易出问题

异步 RPC 最大的难点之一不是 socket，而是生命周期。

同步调用中：

```cpp
EchoResponse response;
stub.Echo(..., &response, ...);
```

如果 `CallMethod()` 在 response 到达前就返回，那么：

```text
response 必须活到异步完成时
```

否则：

```text
CallMethod 返回
    ↓
response 被析构
    ↓
网络响应到达
    ↓
PendingCall 持有悬空指针
```

就是 Use-After-Free。

同样：

```text
controller
done
request
TcpConnection
RpcChannel
```

都有生命周期问题。

因此后续实现时必须明确：

```text
谁拥有对象？
谁保证它活到 completion？
谁负责释放？
```

第一版可以先沿用 protobuf callback 模型的调用约定，但后续 Coroutine 版本必须进一步重构 ownership。

---

## 12. Async RPC 的基本状态机

最简单的状态：

```text
Created
   ↓
Pending
   ↓
Completed
```

后面加入 Timeout / Cancel 后会扩展成：

```text
                 ┌── Completed
Pending ─────────┼── TimedOut
                 ├── Cancelled
                 └── Failed
```

关键要求：

> 一次 RPC 只能完成一次。

例如 timeout 和 response 可能同时发生：

```text
Timer Thread/Event        Network Event
      │                       │
      ▼                       ▼
   Timeout                Response
      \                     /
       \                   /
        ── 谁先完成？ ────
```

后面的 `Exactly-Once Completion` 就是专门解决这个 Race。

Async RPC 阶段先建立 Pending -> Completed 基础模型。

---

## 13. 与 Coroutine 的关系

Async RPC 是 Coroutine RPC 的基础。

Callback 模型：

```cpp
stub.Echo(..., done);
```

未来 Coroutine 模型：

```cpp
auto response = co_await client.Echo(request);
```

底层其实仍然是：

```text
request_id
PendingCall
EventLoop
Response
Completion
```

区别只是完成方式：

```text
Callback:
    done->Run()

Coroutine:
    coroutine_handle.resume()
```

所以不能跳过 Async RPC 直接写协程。

协程只是异步状态机的一种更好用的上层表达方式。

---

## 14. 本阶段不做什么

为了控制复杂度，这一阶段暂时不同时加入：

- Timeout
- Cancel
- Retry
- Load Balance
- Connection Pool
- Coroutine
- Backpressure
- 自动重连
- Exactly-Once Race 处理

第一阶段只证明：

```text
一个 TCP 长连接
    +
多个 request_id
    +
多个 in-flight RPC
    +
没有同步 wait
    +
响应按 request_id 正确完成
```

完成后再逐项增加能力。

---

## 15. 代码验收标准

Async RPC 完成后至少验证：

### 15.1 CallMethod 不阻塞

```text
发起 RPC
↓
CallMethod 返回
↓
业务线程还能继续执行
↓
稍后 response 到达
↓
callback 执行
```

### 15.2 多个 in-flight RPC

连续快速发出多次 RPC：

```text
request 1
request 2
request 3
request 4
...
```

不能每次等待上一个完成。

### 15.3 request_id 正确关联

即使响应未来出现乱序，也必须：

```text
response N
    ↓
找到 request N 的 PendingCall
```

### 15.4 EventLoop 不被阻塞

RPC 等待期间 Reactor 必须仍能处理：

```text
其他连接
其他 request
其他 response
```

### 15.5 PendingCall 正确删除

RPC 完成后：

```text
pending_calls_
```

不能持续增长。

否则就是逻辑泄漏。

---

## 16. 面试需要记住的核心回答

### Q1：同步 RPC 和异步 RPC 最大区别是什么？

同步 RPC 在调用栈中等待结果；异步 RPC 将调用上下文保存为 PendingCall，发送后立即返回，网络响应到达后再通过 callback / coroutine 完成调用。

### Q2：为什么 RPC 需要 request_id？

因为一个 TCP 连接上可以存在多个并发 in-flight RPC，request_id 用于将异步到达的 response 与原始 request 及其调用上下文关联。

### Q3：为什么 Reactor 线程不能 wait future？

因为 Reactor 线程负责处理网络事件。如果它阻塞等待某个 response，而这个 response 又需要 Reactor 读取，就会导致整个网络 Runtime 停止推进。

### Q4：PendingCall 的作用是什么？

保存一次未完成 RPC 的异步调用上下文，包括 response、controller、completion callback 等，使原始调用栈退出后仍能在响应到达时恢复并完成该 RPC。

### Q5：为什么 pending map 可以不加锁？

不是因为 unordered_map 天生线程安全，而是通过线程归属约束，确保它只在一个 EventLoop owner thread 中访问。跨线程操作先投递到该 EventLoop。

### Q6：为什么 Async RPC 是 Coroutine RPC 的基础？

协程不会消除异步状态机。底层仍需要 request_id、PendingCall、网络事件和 completion。协程只是把 callback completion 转换成 `coroutine_handle.resume()`，让异步代码写起来更接近同步代码。

### Q7：异步 RPC 最大的工程难点是什么？

不是发送 socket，而是生命周期、状态管理和竞争条件，包括 response/controller/callback 的生命周期，以及 response、timeout、cancel 等多个完成源之间的 Exactly-Once Completion。

---

## 17. 下一阶段

Async RPC 完成后进入：

```text
Timeout / Deadline
    ↓
Cancel
    ↓
Response / Timeout / Cancel Race
    ↓
Exactly-Once Completion
```

目标是把：

```text
Pending -> Completed
```

扩展成真正可靠的 RPC Call State Machine。

之后再进入：

```text
C++20 Coroutine RPC
```

把已经稳定的 Async RPC completion 接到 coroutine resume 上。
