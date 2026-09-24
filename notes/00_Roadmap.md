# NebulaRPC 技术路线总览（含各阶段实现细节）

> 每一节结构：来源 / 目标 / 状态 / 实现细节（涉及文件、具体步骤、关键代码、验收标准）。
> 已完成的阶段按实际实现记录"怎么做的"，未完成的阶段写清楚"具体怎么做"。

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

状态：已完成

### 实现细节

涉及文件：

    nebula/net/channel.h / channel.cpp
    nebula/net/poller.h / poller.cpp
    nebula/net/epoll_poller.h / epoll_poller.cpp
    nebula/net/event_loop.h / event_loop.cpp
    tests/net_smoke_test.cpp

实现步骤：

1. 先写 Channel。Channel 是 fd 的事件封装，不拥有 fd，只负责：
   - 保存 `events_`（关注事件）/ `revents_`（实际发生事件）/ `index_`（在 Poller 中的状态）；
   - `EnableReading / EnableWriting / DisableAll` 等修改 `events_` 后调用 `Update()`，
     转调 `loop_->UpdateChannel(this)`；
   - `HandleEvent()` 按 `revents_` 分发：`EPOLLIN` 走 read 回调，`EPOLLOUT` 走 write 回调，
     `EPOLLHUP/EPOLLERR` 走 close/error 回调；
   - `Tie(shared_ptr)`：保存 weak_ptr，`HandleEvent` 期间 lock 一次，
     防止回调执行中途属主对象被析构。
2. 写 Poller 基类。持有 `channels_`（fd -> Channel*），定义纯接口
   `Poll / UpdateChannel / RemoveChannel / HasChannel`，
   `NewDefaultPoller()` 返回 EPollPoller。
3. 写 EPollPoller：
   - 构造 `epoll_create1(EPOLL_CLOEXEC)`；
   - `Poll(timeout_ms)` 调 `epoll_wait`，`FillActiveChannels` 从
     `event.data.ptr` 还原 Channel* 并写入 `revents`；事件数组满了就扩容 2 倍；
   - `UpdateChannel` 按 Channel 的 `index_` 三态分发：
     `kNew -> EPOLL_CTL_ADD`（并登记进 channels_），
     `kAdded -> EPOLL_CTL_MOD`（若 IsNoneEvent 则 DEL 并转 kDeleted），
     `kDeleted -> EPOLL_CTL_ADD`（重新加回）；
   - `epoll_event.data.ptr = channel`，LT 模式。
4. 写 EventLoop：
   - `thread_local` 指针保证一个线程只能有一个 EventLoop；
   - 构造时创建 `eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)` 作为唤醒 fd，
     注册为 wakeup_channel_；
   - `Loop()` 主循环：`poller_->Poll() -> 遍历 active_channels HandleEvent
     -> DoPendingFunctors()`；
   - `RunInLoop`：owner 线程直接执行，否则 `QueueInLoop`（加锁入队 + `WakeUp`）；
   - `Quit()` 置 quit_ 并唤醒。

关键代码（EventLoop 主循环）：

```cpp
// 事件循环：等待事件 -> 分发 -> 执行跨线程任务
void EventLoop::Loop()
{
    AssertInLoopThread();

    while (!quit_.load())
    {
        active_channels_.clear();
        poller_->Poll(10000, &active_channels_);   // epoll_wait

        // 逐个分发就绪事件
        for (Channel* channel : active_channels_)
        {
            channel->HandleEvent();
        }

        // 执行其他线程投递进来的任务
        DoPendingFunctors();
    }
}
```

验收标准：

- `nebula_net_smoke_test` 通过；
- ASan preset 下无泄漏、无 UAF。

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

状态：已完成

### 实现细节

涉及文件：

    nebula/net/buffer.h
    nebula/net/tcp_connection.h / tcp_connection.cpp
    nebula/net/socket.h / socket.cpp

Buffer 设计（header-only）：

1. 内存布局：`vector<char>` + `read_index_` + `write_index_`，
   头部预留 `kCheapPrepend = 8` 字节，初始 `kInitialSize = 1024`。
2. 读：`ReadFd(fd)` 用 `readv` 散射读——主缓冲写满部分 +
   栈上 64KB 扩展缓冲，一次系统调用读尽数据，
   超出主缓冲的部分再 `Append` 回来。避免"先猜大小、不够再读"的两次系统调用。
3. 写/取：`Append / Peek / Retrieve / RetrieveAsString`；
   `AppendUInt32 / PeekUInt32` 内部做 `htonl/ntohl` 网络序转换（帧长度用）。
4. 空间管理：`MakeSpace` 优先挪动已有数据到头部，不够才 `resize`。

TcpConnection 发送路径（partial write + EPOLLOUT）：

1. `Send(data)`：owner 线程直接 `SendInLoop`；跨线程则
   `shared_from_this()` 投递，绝不捕获裸 this。
2. `SendInLoop`：
   - 当前没在写且输出缓冲为空 -> 直接 `::write`，一次写完就触发
     `WriteCompleteCallback`；
   - 写不完（或 EAGAIN）-> 剩余进 `output_buffer_`，`EnableWriting` 注册 EPOLLOUT。
3. `HandleWrite`：可写时继续 `::write` 输出缓冲；写空后 `DisableWriting`、
   触发 `WriteCompleteCallback`；若状态是 `kDisconnecting` 则执行半关闭。
4. `Shutdown`：CAS 到 `kDisconnecting`，等输出缓冲写空后 `shutdown(fd, SHUT_WR)`。

接收路径：

    HandleRead
        -> input_buffer_.ReadFd()
        -> n > 0: message_callback_(conn, &input_buffer_)   // 上层循环解码
        -> n == 0: HandleClose
        -> 错误(非 EAGAIN): HandleError

半包/粘包不在 Buffer 层解决，而是上层消息回调里"循环解码直到数据不够"（见第 4 节）。

验收标准：

- echo 示例 + `nc` 手工验证长消息、分片发送；
- `rpc_codec_test` 覆盖半包、粘包、坏帧。

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

状态：已完成（贯穿所有模块）

### 实现细节

1. 线程归属：
   - `EventLoop` 构造记录 `thread_id_`，所有操作入口 `AssertInLoopThread()`；
   - `thread_local EventLoop*` 保证一线程一 loop。
2. 跨线程投递：
   - `RunInLoop(cb)`：owner 线程直接执行，否则转 `QueueInLoop`；
   - `QueueInLoop(cb)`：mutex 下入队 `pending_functors_`，
     非 owner 线程或正在执行 functor 期间（`calling_pending_functors_`）都 `WakeUp`，
     保证新任务不会睡死在 epoll_wait 里；
   - `DoPendingFunctors` 整体 swap 出来再执行，缩短临界区。
3. 连接对象生命周期：
   - `TcpConnection` 继承 `enable_shared_from_this`，
     `ConnectEstablished` 时 `channel_->Tie(shared_from_this())`；
   - `Channel::HandleEventWithGuard` 把 weak_ptr lock 成 shared_ptr，
     保证一次事件分发期间连接对象不会析构。
4. Connector 的两个坑及解法：
   - 回调引用环：Channel 回调捕获 `weak_ptr<Connector>`，用时 lock；
   - 事件栈中销毁：`RemoveAndResetChannel` 可能仍在 `HandleEvent` 调用栈里，
     `channel_.reset()` 用 `QueueInLoop` 延迟到下一轮执行。
5. 析构顺序约定：先摘上层回调，再动底层 Channel。
   `TcpClient::~TcpClient` / `RpcChannel::~RpcChannel` 都遵守：
   清空 callback -> Stop/Reset 底层 -> 最后失败掉所有 pending。
6. 线程池：`EventLoopThread` 起线程跑 `EventLoop::Loop`，
   用 mutex + condition_variable 等 loop 构造完成再返回；
   `EventLoopThreadPool::GetNextLoop` round-robin 分配。

验收标准：

- ASan / UBSan preset 全部测试绿色；
- 高频建连/断连压测无 UAF、无泄漏。

------------------------------------------------------------------------

## 4. Protobuf / RPC Protocol

来源：

    game_rpc_project 能力迁入

目标：

-   protobuf service
-   RPC message framing
-   编解码流程
-   服务注册与调用分发

状态：已完成

### 实现细节

涉及文件：

    nebula/rpc/proto/rpc_meta.proto
    nebula/rpc/rpc_codec.h / rpc_codec.cpp
    nebula/rpc/rpc_server.h / rpc_server.cpp
    nebula/rpc/rpc_closure.h
    nebula/rpc/CMakeLists.txt
    tests/rpc_codec_test.cpp

步骤：

1. 定义元信息协议 `rpc_meta.proto`：

```proto
message RpcMeta {
  enum MessageType { REQUEST = 0; RESPONSE = 1; ERROR = 2; }
  MessageType type = 1;
  uint64 request_id = 2;
  string service_name = 3;
  string method_name = 4;
  uint32 payload_size = 5;
  int32 error_code = 6;
  string error_text = 7;
}
```

   CMake 用 `protobuf_generate_cpp` 生成代码，单独建 `nebula_rpc_proto` 静态库。

2. 定帧格式（解决 TCP 无边界）：

```text
+------------------+ 4B frame_size（网络序）= 4B meta_size 字段 + meta + payload
| frame_size       |
+------------------+ 4B meta_size（网络序）
| meta_size        |
+------------------+
| RpcMeta protobuf |
+------------------+
| payload protobuf |
+------------------+
```

3. 编码 `RpcCodec::Encode(meta, payload)`：
   - `meta.set_payload_size(payload.size())` 后序列化 meta；
   - 校验总长不超过 `kMaxFrameSize = 16MB`（防恶意长度打爆内存）；
   - 依次拼 `frame_size_be + meta_size_be + meta + payload`，长度全部网络序。

4. 解码 `RpcCodec::Decode(buffer, frame, error)`，三态返回：
   - 可读 < 4B -> `kNeedMore`（半包，Buffer 数据保留等下次）；
   - `frame_size` 非法（< 4B 或 > 16MB）-> `kError`；
   - 整帧未到齐 -> `kNeedMore`；
   - 到齐 -> 取出 body 交 `DecodeBody`：解析 meta_size -> 解析 meta ->
     校验 `payload_size` 与实际一致 -> `kOk`。

5. 消息回调里循环解码（粘包）：

```cpp
// 一次 read 可能带多帧，循环取完为止
while (true)
{
    const auto result = RpcCodec::Decode(buffer, &frame, &error);
    if (result == DecodeResult::kNeedMore)
    {
        return;                       // 半包，等下一次 EPOLLIN
    }
    if (result == DecodeResult::kError)
    {
        conn->Shutdown();             // 协议错误直接断连
        return;
    }
    HandleFrame(std::move(frame));
}
```

6. 服务端分发 `RpcServer`：
   - `RegisterService`：按 `descriptor->full_name()` 存入 `services_`；
   - `HandleRequest`：`FindMethodByName` 找方法 ->
     `GetRequestPrototype/GetResponsePrototype().New()` 动态创建消息对象 ->
     `ParseFromString(payload)` -> `service->CallMethod(...)`；
   - 找不到 service/method 回 404，解析失败回 400，序列化失败回 500，
     统一走 `RpcMeta::ERROR` 帧；
   - 异步生命周期：`ServerCall`（request/response 的 unique_ptr）包进
     `shared_ptr` 被 `RpcClosure` 的 lambda 捕获，活到 `done->Run()` 发完响应。

验收标准：

- `rpc_codec_test`：半包、粘包、坏 meta、payload 长度不匹配、超 16MB 全部正确判定；
- `rpc_echo` 端到端跑通。

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

状态：已完成

### 实现细节

1. request_id 生成：`RpcChannel` 里 `inline static std::atomic_uint64_t next_request_id_{1}`，
   `fetch_add(1)` 取值。全局单调，多个 channel 并存也不冲突。
2. PendingCall 结构（一次在途 RPC 的上下文）：

```cpp
struct PendingCall
{
    google::protobuf::Message* response{};          // 响应写入目标
    google::protobuf::RpcController* controller{};   // 错误写入目标
    google::protobuf::Closure* done{};              // 完成回调
    std::optional<TimePoint> deadline;              // 超时点（第 7 节）
    net::TimerId timeout_timer;                     // 超时定时器（第 7 节）
};
```

3. 注册：发送前 `pending_calls_.emplace(request_id, pending_call)`，
   request_id 同时写进 `RpcMeta` 随帧发出。
4. 匹配：收到响应帧后 `frame.meta.request_id()` ->
   `TakePendingCall(request_id)` 摘除并返回上下文 -> 完成调用。
5. 关键假设：**不假设响应顺序等于请求顺序**。一个连接上多请求并发飞行，
   乱序返回全靠 request_id 关联：

```text
request #1 ─►  request #2 ─►  request #3 ─►
                ◄─ response #2
                                ◄─ response #3
◄─ response #1
```

6. 线程约束：`pending_calls_` 只在属主 EventLoop 线程访问
   （注册、匹配、超时都在同一线程），所以不加锁。

验收标准：

- `rpc_echo_client` 连发 3 个请求，响应乱序也各自正确匹配；
- 全部完成后 `pending_calls_` 为空（无泄漏）。

------------------------------------------------------------------------

## 6. Async RPC

来源：

    NebulaRPC 新能力

目标：

-   移除阻塞等待模型
-   Reactor 驱动 RPC
-   callback completion
-   pending call 生命周期

状态：已完成

### 实现细节

涉及文件：

    nebula/net/connector.h / connector.cpp     （非阻塞 connect）
    nebula/net/tcp_client.h / tcp_client.cpp
    nebula/rpc/rpc_channel.h / rpc_channel.cpp
    examples/rpc_echo/rpc_echo_client.cpp

步骤：

1. 补客户端 Reactor 能力（先于 RPC 改造）：
   - `Connector`：非阻塞 `connect` 返回 `EINPROGRESS` -> 注册 `EPOLLOUT` ->
     可写后 `getsockopt(SO_ERROR)` 判定真正成败 -> 成功则把 fd 交给上层；
   - `TcpClient`：封装 Connector，成功回调里创建 `TcpConnection` 并
     `ConnectEstablished`，对外暴露 connection/message/connect-error 回调。
2. 改造 `RpcChannel::CallMethod`（可在任意线程调用）：

```text
CallMethod
    -> done == nullptr 直接拒绝（纯异步，无同步路径）
    -> request->SerializeToString
    -> request_id = next_request_id_.fetch_add(1)
    -> RpcCodec::Encode(REQUEST 帧)
    -> loop_->RunInLoop(RegisterAndSend)
    -> 立即返回，不等待
```

3. `RegisterAndSend`（属主 loop 线程）：
   - 已断连 -> 立即失败完成；
   - deadline 已过 -> 立即 "RPC timeout" 完成；
   - `pending_calls_.emplace(request_id, ...)`；
   - 已连接 -> `connection_->Send(bytes)`；
   - 未连接 -> 进 `pending_writes_` 排队。
4. 连接建立后冲刷：`OnConnection(connected)` 里遍历 `pending_writes_`，
   跳过已被超时/断连完成的 request_id，其余 `Send`。
5. 响应完成：`OnMessage -> Decode -> HandleFrame -> CompleteCallWithFrame`：
   - `TakePendingCall` 摘除（摘不到说明已超时/断连完成，迟到响应直接丢弃）；
   - 解析 response / 写错误到 controller；
   - `done->Run()`，**固定在属主 EventLoop 线程执行**（业务回调禁止阻塞）。
6. 失败兜底 `FailAllPending`：断连、连接错误、协议错误、析构四条路径
   都会把全部 pending 以失败完成，保证 done 一定执行、pending 不泄漏。
7. 生命周期约定：`response / controller / done` 必须活到 done 执行。
   示例用 `shared_ptr<CallContext>` 被 done 闭包捕获（见 `rpc_echo_client.cpp`）。

验收标准：

- `CallMethod` 调用后立即返回（先打印 "returned" 后才打印响应）；
- 多个 in-flight RPC 同时进行；
- RPC 等待期间 EventLoop 仍能处理其他事件；
- 完成后 `pending_calls_` 清空。

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

状态：**已完成** —— Timeout / Cancel / Race / Exactly-Once 四条完成路径全部落地，
Linux Debug 构建通过（13/13，0 error / 0 warning）。
未完成项：ASan / UBSan preset 验收未跑；代码与笔记未提交。
落地细节见 `06_Timeout_Cancel_ExactlyOnce_实现细节.md`。

### 实现细节

涉及文件：

    nebula/rpc/rpc_controller.h / rpc_controller.cpp
    nebula/rpc/rpc_call.h / rpc_call.cpp
    nebula/rpc/rpc_call_context.h                 （零引用，待删；原计划即删除）
    nebula/rpc/rpc_channel.h / rpc_channel.cpp
    nebula/net/timer_queue.h / timer_queue.cpp
    tests/rpc_timeout_test.cpp
    tests/rpc_call_test.cpp / rpc_cancel_test.cpp

#### 7.1 TimerQueue 基础（已完成）

- `timerfd_create(CLOCK_MONOTONIC)` 把时间事件变成 fd 事件，
  和 socket/eventfd 一起进 epoll，Reactor 不轮询时间；
- `multimap<TimePoint, timer_id>` 按到期排序 + `unordered_map<timer_id, Timer>`
  O(1) 找回调；新增/取消只在影响最早到期时间时才 `timerfd_settime`；
- 时钟用 `steady_clock`（单调，不受改系统时间影响）；
- 对外接口：`EventLoop::RunAt / RunAfter / CancelTimer`。

#### 7.2 Timeout（已完成）

1. `RpcController::SetTimeout(duration) / SetDeadline(time_point)`，
   两者互斥（设一个清另一个），mutex 保护。
2. `ResolveDeadline(controller)`：deadline 优先；否则 `now + timeout`；
   都没有则不注册定时器。
3. `RegisterAndSend` 注册 pending 后：

```cpp
// 有 deadline 就挂超时定时器
if (it->second.deadline.has_value())
{
    it->second.timeout_timer = loop_->RunAt(
        *it->second.deadline,
        [this, request_id]
            {
                OnTimeout(request_id);
            });
}
```

4. 到期：`OnTimeout -> CompleteCallWithFailure(request_id, "RPC timeout")`：
   `TakePendingCall` 摘除 -> `controller->SetFailed` -> `done->Run()`。
5. 互斥清理：
   - response 先到：`TakePendingCall` 时 `CancelTimer`，定时器作废；
   - timeout 先到：pending 已摘除，迟到的 response 找不到 request_id，丢弃。

#### 7.3 Exactly-Once 机制：CAS 状态机 + 单线程收口（已完成）

单线程摘除这一层仍然成立：response、timeout、disconnect 都在同一个
EventLoop 线程排队执行，谁先摘走 pending 谁完成，天然串行无竞争。

但 Cancel 打破了"所有完成源都在同一线程"这个前提（protobuf 约定
`StartCancel` 可在任意业务线程调用），因此 `rpc_call.h/.cpp` 的状态机
已接线，作为四条完成路径统一的完成权闸门：

```cpp
// 只有 Pending -> 目标状态 的第一次 CAS 成功，保证恰好一次完成
bool RpcCall::TryComplete(RpcCallState state) noexcept
{
    RpcCallState expected = RpcCallState::Pending;
    return state_.compare_exchange_strong(expected, state,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire);
}
```

接线步骤（已按此落地）：

1. `nebula/rpc/CMakeLists.txt` 的 `nebula_rpc` 源列表加入 `rpc_call.cpp`；
2. `PendingCall` 内嵌 `RpcCall`（**未采用**整体改用 `RpcCallContext`，该文件成死文件）；
3. 四条完成路径（response / timeout / disconnect / cancel）统一改为：
   先 `call.TryComplete(对应状态)`，返回 true 才允许 `TakePendingCall` + 执行回调；
4. `rpc_call.h/.cpp`、`tests/rpc_call_test.cpp` 提交进版本库（提交规划见 `05_工程路线_代码对齐版.md` 第 9 节）；
5. 补状态机单元测试（单线程顺序 + 多线程竞争）—— 已落 `tests/rpc_call_test.cpp`。

为什么现在就要接：单线程下 `TryComplete` 看似多余，但 7.4 的 Cancel
是第一个跨线程完成源，届时 CAS 是唯一正确解。先接线，Cancel 来了不用动骨架。

#### 7.4 Cancel（已完成）

已有能力（`RpcController`，mutex 保护，任意线程可调）：

    StartCancel()          置 cancel_requested_，swap 走所有 cancel 回调执行
    RegisterOnCancel(cb)   注册回调并返回 token；已取消则内联执行并返回 -1
    RemoveOnCancel(token)  按 token 摘除回调并释放
    IsCanceled()           取消**已生效**（终态，非"已请求"）

★ 实现与本段原方案的差异：原方案用 `NotifyOnCancel(cb)`（无返回值、事后无法摘除），
实际改为 `RegisterOnCancel` → token + `RemoveOnCancel(token)`。否则 RPC 正常完成后
回调会永久留在 controller 的 `cancel_callbacks_` 里（见下方第 3 条）。

接线步骤（已按此落地）：

1. `RegisterAndSend` 注册 pending 后，向 controller 注册回调：

```cpp
// 业务线程随时可能 StartCancel，回调里先 CAS 抢占完成权
cancel_token = controller->RegisterOnCancel(new RpcClosure([weak_guard, request_id, loop = loop_]
    {
        loop->RunInLoop([weak_guard, request_id]
            {
                channel->CompleteCallWithCancel(request_id);
            });
    }));
```

（完整形态含存活守卫校验，见 `nebula/rpc/rpc_channel.cpp` 的 `RegisterAndSend`）

2. `CompleteCallWithCancel`：`TryComplete(Cancelled)` 成功 ->
   `TakePendingCall` -> `done->Run()`（controller 不设 Failed，
   业务用 `IsCanceled()` 判断）；
3. 给 `RpcController` 增加取消回调摘除能力（如 `RemoveOnCancel` 或
   回调自失效），RPC 正常完成后必须摘除，防止 controller 复用时悬挂；
4. 后续增强：`RpcMeta` 增加 CANCEL 帧通知服务端、服务端释放 in-flight 资源。

#### 7.5 竞争测试（三组用例已补齐）

- Case 1 response 先到：正常完成，定时器被取消，done 恰好一次；
- Case 2 timeout 先到：失败完成，迟到 response 被丢弃，done 恰好一次；
- Case 3 并发竞争：一个线程循环 `StartCancel`，loop 线程立刻回 response，
  ASan 下断言 done 计数 == 1。

原已知测试缺陷（**已修复**）：`rpc_timeout_test.cpp` 的服务端 `Echo` 计算了
`delay`（slow = 250ms）但从未使用，响应用 `RunInLoop` 立即执行，
导致超时路径根本没被触发、测试恒过。修法已落地：服务端改用
`loop_->RunAfter(delay, ...)` 再回包，并补断言：slow 请求
`controller.Failed()` 为真、错误文本为 "RPC timeout"、done 恰好一次；
另加 `service.SlowTimerFiredCount() == 1` 证明迟到响应确实到达但被客户端丢弃。

验收标准：

- ✅ 三组竞争用例全部通过，done 计数恒为 1；
- ⬜ ASan / UBSan 绿色（preset 已就绪，**待跑**）；
- ✅ 修复后的超时测试能真实触发超时路径。

附带修复（超出本节原计划，见 `06` 的 5.5 与 9.3）：取消回调与超时定时器的存活守卫、
`~RpcController` 释放残留取消回调、`IsCanceled()` 改为终态语义、`AliveGuard` 改 atomic。

------------------------------------------------------------------------

## 8. C++20 Coroutine RPC

来源：

    NebulaRPC 新能力

目标：

-   callback 转 coroutine
-   co_await RPC
-   coroutine 生命周期
-   resume 调度线程

状态：未开始

### 实现细节

目标体验：

```cpp
// 现在：callback
stub.Echo(&controller, &request, &response, done);

// 之后：协程
auto response = co_await client.Echo(request);
```

实现步骤：

1. 写 `RpcAwaiter`（挂在现有完成路径上，不改网络层）：

```cpp
class RpcAwaiter
{
public:
    bool await_ready() const noexcept
    {
        return false;                              // 永远挂起等响应
    }

    // 挂起时把协程句柄登记进 PendingCall，然后发起异步调用
    void await_suspend(std::coroutine_handle<> handle);

    // 恢复后取出结果：成功返回 response，失败抛异常或返回错误
    EchoResponse await_resume();

private:
    RpcCallContext context_;                       // 复用第 7 节状态机
};
```

2. 写 `promise_type`：管理协程帧、异常传递（`unhandled_exception`）、
   `final_suspend` 决定协程结束后是否自动销毁。
3. 接入点：`CompleteCallWithFrame / CompleteCallWithFailure` 目前最后执行
   `done->Run()`——协程版改为恢复登记的 `coroutine_handle`：

```cpp
// 完成路径二选一：callback 或 coroutine
if (pending_call->done != nullptr)
{
    pending_call->done->Run();
}
else
{
    pending_call->handle.resume();                 // 恢复协程
}
```

4. resume 线程语义（必须写死）：默认在属主 EventLoop 线程恢复
   （与 done 回调语义一致，业务代码不得阻塞）；
   若业务要求回原线程，后续引入 executor：`loop->QueueInLoop(handle.resume)`。
5. 生命周期：协程帧、response、controller 必须活到 `await_resume`，
   沿用 `shared_ptr<CallContext>` 模式；超时/取消复用第 7 节状态机，
   `await_resume` 里检查 `TryComplete` 的最终状态。
6. 构建：C++20 已开（g++ 11+），CMake 无需改动。

验收标准：

- 协程版 echo 示例：`co_await` 拿到正确响应；
- 超时场景：`co_await` 抛出/返回超时错误，协程正常结束无泄漏；
- ASan 下协程帧无泄漏。

------------------------------------------------------------------------

## 9. Backpressure / Resource Limits / Overload Control

来源：

    NebulaRPC 新能力

目标：

-   输出缓冲限制
-   请求数量限制
-   过载保护
-   服务降级

状态：未开始

### 实现细节

三个无上限资源，逐个加限制：

1. 输出缓冲高水位（防慢客户端撑爆内存）：
   - `TcpConnection` 增加 `SetHighWatermarkCallback(cb, bytes)` 与
     `high_watermark_`；
   - `SendInLoop` 写入前检查：
     `output_buffer_.ReadableBytes() + data.size() > high_watermark_`
     且之前未超 -> 触发回调；
   - 上层（RpcChannel/业务）收到回调后选择：暂停发送新请求 / `Shutdown` 该连接。
2. pending 数量上限（防客户端无限堆积在途请求）：
   - `RpcChannel` 增加 `max_pending_calls`（如 10000）；
   - `RegisterAndSend` 里 `pending_calls_.size() >= max` 时直接
     `SetFailed("too many pending rpcs")` 拒绝，不进队。
3. 输入缓冲水位（服务端防恶意大流量）：
   - `TcpConnection::HandleRead` 后检查 `input_buffer_.ReadableBytes()`，
     超阈值直接 `Shutdown`（当前只有单帧 16MB 限制，总量无限制）。

过载策略三选一（按场景配置）：

    reject    直接拒绝，快速失败（默认）
    queue     有界排队，队满转 reject
    degrade   返回降级结果（框架只提供钩子，业务实现）

验收标准：

- 慢客户端压测（服务端故意不回包），进程内存曲线平稳不暴涨（heaptrack 验证）；
- pending 超限后新请求立即失败且 error 文本正确；
- 高水位回调触发次数可观测。

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

状态：未开始（当前是单连接单 loop 的 RpcChannel）

### 实现细节

目标架构：

```text
Application
    |
RpcClient（新增）
    |
    +-- ConnectionPool：N 条连接（每条 = 一个 RpcChannel）
    |
    +-- LoadBalancer：从池中选连接
    |
    +-- Retry / Reconnect 策略
```

实现步骤：

1. `RpcClient`：持有 endpoint 列表（ip:port），管理一组 `RpcChannel`，
   对外暴露与 `protobuf::RpcChannel` 相同的 `CallMethod` 接口，
   内部选连接后转发。
2. Connection Pool：
   - 每个 endpoint 维护 N 条连接（N 可配，默认 1）；
   - 连接空闲复用：RPC 多路复用天然支持（request_id），
     不必"借出/归还"，池只负责容量与健康；
   - 健康检查：心跳帧（新增 `RpcMeta::HEARTBEAT` 类型）或
     空闲超时探测，失败连接标记不可用。
3. Reconnect（先修 Connector）：
   - 当前 `Connector` 连接失败即永久停止（`connect_ = false`）；
   - 增加退避重连：失败后 `loop_->RunAfter(backoff, RetryConnect)`，
     backoff 指数增长封顶（如 100ms -> 200ms -> ... -> 5s）；
   - 重连成功后 `RpcChannel` 现有的 `pending_writes_` 冲刷机制直接复用。
4. Retry：
   - 只对幂等接口重试（`RpcMeta` 或方法选项标记）；
   - 次数上限 + 指数退避；
   - 硬约束：重试总耗时不得突破该次调用的 deadline（复用第 7 节）。
5. Load Balance 实现顺序：
   - 第一版 Round Robin（`next_++` 取模，`EventLoopThreadPool` 已有同款）；
   - 第二版 Least Connection（每连接维护 in-flight 计数，选最小）；
   - 第三版 Consistent Hash（按请求键路由，虚拟节点环）。
6. 多 loop 分布：连接分属不同 EventLoop（复用 `EventLoopThreadPool`），
   避免单 loop 成为瓶颈。

验收标准：

- 杀掉服务端进程 -> 客户端自动重连 -> 恢复后 RPC 成功；
- 断连期间在途请求全部以失败完成、无泄漏；
- RR 分布下各连接请求数均匀。

------------------------------------------------------------------------

## 11. Observability / Metrics / Trace / Structured Log / Graceful Shutdown

来源：

    NebulaRPC 新能力

目标：

-   spdlog 日志
-   指标采集
-   请求追踪
-   优雅关闭

状态：未开始（spdlog 已引入，RPC 层零日志调用）

### 实现细节

1. 结构化日志（第一步，成本最低）：
   - `logger.h` 的 `NLOG_*` 宏已就绪；
   - 埋点位置：`CallMethod` 发起、完成路径（成功/超时/取消/失败）、
     协议错误、断连、重连；
   - 每条日志固定字段：`request_id / service / method / latency_ms / error`，
     用 spdlog 的 fmt 风格输出，便于后续 grep/采集。
2. Metrics（进程内先行，不急着接 Prometheus）：
   - 每 method 维护：总次数、失败次数、超时次数（atomic 计数）；
   - latency 直方图：固定桶（1/5/10/50/100/500/1000ms+），
     完成时按耗时落桶；
   - 周期任务（`RunAfter` 定时）输出 QPS、P50/P95/P99
     （桶内累计反推分位数）。
3. Trace：
   - `RpcMeta` 增加 `trace_id` 字段，客户端生成（或透传上游），
     服务端日志原样带出，实现跨进程串联。
4. Graceful Shutdown：
   - `TcpServer` 增加 `Stop()`：停止 accept（关 listen fd）->
     等待 in-flight RPC 完成（设上限时间，如 10s）->
     逐个 `Shutdown` 连接 -> 退出；
   - `RpcServer` 需要 in-flight 表（可与服务端取消共用）；
   - 信号接入：`SIGTERM` -> `loop->QueueInLoop(server.Stop)`。

验收标准：

- 一次 RPC 的日志能串起 发起 -> 完成 全链路（同一 request_id）；
- 周期指标输出的 P99 与手工统计一致；
- 优雅关闭期间已收到的请求全部正常返回，不丢请求。

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

状态：未开始（Release/ASan/UBSan preset 已就绪）

### 实现细节

1. 压测工具（自建 benchmark 客户端）：
   - 参数：并发数、持续时间/总请求数、payload 大小、连接数；
   - 统计：QPS、latency 分布（P50/P95/P99/P999）、错误率；
   - 复用第 11 节的直方图组件。
2. 测试场景：

```text
echo 小 payload 高并发       -> 吞吐上限
大 payload（接近 16MB）      -> 拷贝/内存路径
单连接多路复用 vs 多连接     -> 多路复用收益
慢服务端 + 超时              -> 定时器规模影响
```

3. 分析工具链：

```text
perf record + flamegraph     CPU 热点
heaptrack                    分配与内存增长
linux-asan / linux-ubsan     内存与未定义行为（已有 preset）
```

4. 重点怀疑对象（基于当前实现的预判清单）：
   - `RpcCodec::Decode` 的 `RetrieveAsString` 一次整帧 string 拷贝；
   - `pending_calls_` 节点级堆分配频率；
   - `DoPendingFunctors` 的 mutex 在高投递频率下的竞争；
   - protobuf 序列化/反序列化占比（通常是大头，先量化再谈优化）。
5. 流程纪律：先测出基线 -> 找到热点 -> 只改热点 -> 复测对比，
   每一步数据进 notes。

验收标准：

- 产出一份性能报告（场景 x 指标矩阵 + 火焰图结论）；
- 每次优化都有 before/after 数据支撑。

------------------------------------------------------------------------

## 13. bRPC / gRPC 对照

来源：

    工程验证模块

目标：

-   同场景测试
-   架构对比
-   性能差异分析
-   设计取舍总结

状态：未开始

### 实现细节

1. 环境对齐：同一台机器、同版本编译器、同为 Release、
   同一份 echo.proto 生成三套代码。
2. 场景矩阵（与第 12 节一致，三个框架各跑一遍）：

```text
echo 小 payload x 并发梯度（1/8/32/128）
大 payload
高并发多路复用
超时场景
```

3. 对比维度：

```text
QPS / P99 / CPU 占用 / 内存 / 连接数 / 线程数
```

4. 架构对比要点（写进报告）：
   - 线程模型：NebulaRPC one-loop-per-thread vs bRPC bthread vs gRPC completion queue；
   - 序列化与帧格式差异；
   - 超时/取消机制差异。
5. 输出：对照报告进 notes，结论落到"设计取舍"——
   差距在哪、为什么、哪些值得借鉴。

验收标准：

- 报告包含完整数据矩阵与结论；
- 能回答"同一场景下 NebulaRPC 与工业框架差多少、差在哪"。

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

当前推进顺序：

    第 7 节收尾（状态机接线 + 超时测试修复 + Cancel）
        ↓
    第 8 节 Coroutine
        ↓
    第 9 节 Backpressure
        ↓
    第 10 节 Client Runtime
        ↓
    第 11 节 Observability
        ↓
    第 12 / 13 节 性能与对照
