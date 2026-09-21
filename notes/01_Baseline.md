# 01 - Baseline：Reactor + TCP + Protobuf RPC

## 这一版解决什么

Baseline 的目的不是重新学习 MyMuduo 和 game_rpc_project，而是把两个旧项目已经验证过的能力收敛到 NebulaRPC，形成后续新能力的统一地基。

## 从 MyMuduo 迁入的能力

- `EventLoop -> Poller -> EPollPoller -> Channel` 可读/可写事件链
- `eventfd` 唤醒 EventLoop
- `RunInLoop / QueueInLoop` 跨线程投递
- Buffer + `readv`
- TcpConnection 输入/输出 Buffer
- partial write 后注册 `EPOLLOUT`，可写时继续发送
- Acceptor / TcpServer
- EventLoopThread / EventLoopThreadPool

迁移时没有照搬旧代码：例如 Buffer 的 `ReadFd` 错误码保存改为真正的 `errno`；跨线程 Send 不再捕获裸 `this`，而是持有 `shared_ptr`；线程局部 EventLoop 改用标准 C++ `thread_local`。

## 从 game_rpc_project 迁入的能力

- Protobuf Generic Service
- Service / Method Descriptor 动态分发
- request_id（旧项目 seq_id）
- pending request correlation
- TCP framing / 半包粘包意识
- RpcController

## Baseline RPC 协议

一帧数据：

```text
+------------------+ 4B network byte order
| frame_size       |
+------------------+ 4B network byte order
| meta_size        |
+------------------+
| RpcMeta protobuf |
+------------------+
| payload protobuf |
+------------------+
```

`RpcMeta` 包含：消息类型、request_id、service_name、method_name、payload_size、错误信息。

服务端用 Buffer 增量解析，因此一次 read 收到半帧时会保留数据，收到多帧时会循环解码。

## 当前 RpcChannel 为什么仍然是同步的

Baseline 先保留旧项目最容易验证的模型：

```text
CallMethod
  -> 分配 request_id
  -> pending_calls[request_id] = promise
  -> send
  -> future.wait_for(timeout)

ReceiverThread
  -> recv frame
  -> request_id
  -> 找 pending promise
  -> set_value
```

它的价值是先确认协议、请求关联、服务分发全部正确。

它的缺点也非常明确：

- 一个阻塞 ReceiverThread
- CallMethod 会阻塞调用线程
- timeout 与 response 到达存在竞态
- cancel 还没有真正下沉到 transport
- 连接异常恢复能力有限

这些缺点不是隐藏问题，而是下一阶段 **Async RPC** 要直接替换的对象。

## 面试复习关键词

项目完成后重点回顾：

- epoll LT/ET 区别，NebulaRPC 为什么当前使用这种事件处理方式
- eventfd 为什么适合跨线程唤醒 Reactor
- TCP 为什么没有消息边界
- readv 如何减少一次 Buffer 扩容/拷贝
- partial write 为什么必须监听 EPOLLOUT
- 为什么 TcpConnection 用 `enable_shared_from_this`
- request_id 如何解决多请求响应关联
- protobuf descriptor 如何完成动态 Service/Method 分发
- 为什么异步 Service 的 request/response 生命周期必须延长到 done
