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
   - ~~未连接 -> 进 `pending_writes_` 排队~~（**09-27 已改**：未连上直接失败，见第 10 节）。
4. ~~连接建立后冲刷：`OnConnection(connected)` 里遍历 `pending_writes_`，
   跳过已被超时/断连完成的 request_id，其余 `Send`。~~（**该机制 09-27 已删**，见第 10 节。）
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
    nebula/rpc/rpc_call_context.h                 （零引用死文件，2026-09-25 已删除）
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
2. `PendingCall` 内嵌 `RpcCall`（**未采用**整体改用 `RpcCallContext`，该文件成死文件，已于 2026-09-25 删除）；
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

状态：已落地（2026-09-24；实现方式与本节原计划有三处差异，见末尾「实际落地」）

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

### 实际落地（2026-09-24）

新增文件：

    nebula/rpc/rpc_task.h          Task<T> + promise_type（惰性 initial_suspend、final_suspend 对称转移）
    nebula/rpc/rpc_error.h         RpcError : std::runtime_error，携带 RpcCallState
    nebula/rpc/rpc_awaiter.h       RpcAwaiter<Req,Resp> + ResumeGuard + FindMethod（header-only）
    examples/rpc_echo/rpc_echo_coroutine_client.cpp
    tests/rpc_coroutine_test.cpp

改动文件：

    nebula/rpc/rpc_controller.h/.cpp   新增 CallState() / MarkCallState()，Reset 复位，MarkCanceled 顺带写 Cancelled
    nebula/rpc/rpc_channel.h           新增 Loop() 访问器
    nebula/rpc/rpc_channel.cpp         新增 SetCallState 助手，三条完成路径回填终态
    examples/rpc_echo/CMakeLists.txt、tests/CMakeLists.txt   各加一个 target

与本节原计划的**三处差异**：

1. **不改 `PendingCall`、不改完成路径**（原计划步骤 3 作废）。`done` 本来就是四条完成路径
   唯一的共同出口（`CompleteFailure` / `CompleteFrame` 都是「有 done 就 Run」），所以协程只要
   提供一个「跑起来就恢复协程」的 `RpcClosure`，channel 一行都不用改。代价是要处理
   「done 可能在 `await_suspend` 返回前就被内联调用」——因此恢复走 `QueueInLoop` 推迟。
   （收益：超时/取消/断连/析构四条出口自动覆盖，不会漏 resume 导致帧泄漏。）
2. **不采用 `RpcCallContext`**（原计划步骤 1、5 提到的 `RpcCallContext context_`）。
   第 7 节最终把状态机内嵌进了 `PendingCall`，该文件始终零引用、已于 2026-09-25 删除；
   协程层改用 `ResumeGuard`（持 `coroutine_handle` 的可失效盒）+ 复用 `RpcController` 的终态。
3. **终态可读**（原计划步骤 5「检查 TryComplete 的最终状态」原本拿不到）。补了
   `RpcController::MarkCallState()`，由抢到完成权的路径写入，`RpcError::State()` 因此能区分
   Timeout / Cancelled / Failed，不需要嗅探错误字符串。

验收方式（本项目 Linux-only，本机无法构建时用 MSVC 做语法级验证）：

- 协程机制可运行自检：`Task<T>` 的惰性启动 / 挂起 / 恢复 / 嵌套对称转移 / 返回值 / 异常 / move
  七组断言全部通过；
- `RpcAwaiter` 模板实例化与 `co_await` 语法检查通过（MSVC `/std:c++20 /W4`，0 warning）；
- 真实运行验收 = `ctest --preset test-linux-debug`（含 `nebula_rpc_coroutine_test`），
  **ASan 未跑**（`test-linux-asan`）。

设计细节与陷阱见 `07_Coroutine_RPC.md`；配套学习试卷见
`H:\YJJ\LearningCI\papers\NRPC-协程-V1.json`。

------------------------------------------------------------------------

## 9. Backpressure / Resource Limits / Overload Control

来源：

    NebulaRPC 新能力

目标：

-   输出缓冲限制
-   请求数量限制
-   过载保护
-   服务降级

状态：**已封版**（2026-09-26；Case 1~4 全部通过；实现方式与本节原计划有四处差异，见末尾「实际落地」）

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

### 实际落地（2026-09-26）

四个无上限点全部上锁，详见 `notes/08_Backpressure_资源上限.md`。

1. **输出水位**（`TcpConnection::SetHighWatermarkCallback`）：判据 `before < 水位 <= after`，
   只在跨线瞬间通知一次 —— 天然防抖，不需要低水位/迟滞状态。
2. **输出硬上限**（`SetMaxOutputBufferBytes`）：越线走 `ForceCloseInLoop()` 丢弃待发数据并立即
   关闭。**不能用 `Shutdown()`** —— 它是 graceful、要等 buffer 排空，而慢消费者永远不读，
   等于永远不关、内存永远不释放。
3. **输入水位**（`SetMaxInputBufferBytes`）：检查点在 `message_callback_` 之后，合法帧已被
   消费，残留才说明对端在灌无效流量。
4. **在途请求上限**（`RpcChannel::SetMaxPendingCalls`）：越线
   `SetFailed("too many pending rpcs")` 且不进队；检查排在 deadline 之前，系统级资源保护
   优先于单次调用的自身超时。
5. **未建连排队上限**（`SetMaxPendingWrites`，**草案漏掉的第四个资源点**）：越线
   `SetFailed("too many queued rpcs")`。
6. `TcpServer` / `TcpClient` / `RpcServer` 逐层透传水位配置，建连时应用到每条新建连接。
7. 过载动作：`SetDegradeHandler` 设了钩子即启用降级，钩子返回 `false` 回落为直接失败。
   **不做 `OverloadPolicy` 枚举** —— 草案的 reject/queue/degrade 不在同一个决策点上
   （queue 实际就是 `pending_writes_` 的存在本身），硬凑枚举是假的统一，还会引入
   「policy=kDegrade 但 handler 为空」这种非法状态。
8. 观测：`PendingOutputBytes()` / `HighWatermarkCount()` / `OverloadCloseCount()` /
   `PendingCallCount()` / `PendingWriteCount()` / `OverloadRejectCount()`。

新增 `tests/rpc_backpressure_test.cpp`（四个 case，**全部通过**）：在途闸门 / 输出水位+硬上限 /
输入水位 / 跨层闭环（连接被硬闸门踢掉后，该连接上的在途调用必须失败）。

未覆盖（诚实记账，按性质分类）：`max_pending_writes_` 闸门没有测试（「未建连」的窗口何时关闭由
Connector 内部行为决定，测试无法控制，稳定覆盖需要一个可注入延迟的 Connector 替身，否则测试会
flaky）；输入水位与单帧上限的**边界**未测（属参数纪律，机制已由 Case 3 验证）；heaptrack 内存
曲线未跑（唯一一条「机制在、数据没测」的）；P99 / 错误率属第 12 节。详见
`notes/08_Backpressure_资源上限.md` §5。

验收试卷：`NRPC-BP-V2`（V1 已归档 `papers/历史版本/`）。

**后续变更（2026-09-27）**：第 5 点与第 8 点的 `pending_writes_` 相关机制
（`SetMaxPendingWrites` / `PendingWriteCount` / `"too many queued rpcs"`）已在第 10 节随
「连接不可用即拒发」的设计一并删除 —— 未建连的请求不再排队，也就没有队列上限。
第 7 点的 `queue` 一半随之消失，`reject` 与 `degrade` 两半不受影响。
上面「未覆盖」那段里 `max_pending_writes_` 闸门那条欠账**因此作废**（机制不存在了，不是没测）；
`tests/rpc_pending_writes_test.cpp` 一并删除。`notes/08` 已同步。

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

状态：**已收口（2026-09-27）**。落地的内容与草案差距很大 —— 草案按「连接池 + LB + Retry + 重连」
四件套设计，实现时逐条核对代码后**砍掉三件**，只留下**连接自愈**与**失效拦截**。
判据见「职责边界」：库只负责把「连接能不能用」这件事说清楚；选路与重发在单 endpoint
架构下是空转，归业务。

### 为什么砍掉连接池 / LB / Retry

单 endpoint 部署（当前唯一形态）下，N 条连接指向**同一个 ip:port**，服务端死了就是全死：

```text
            +-- conn1 --+
业务 -- RR --+-- conn2 --+-- 同一个 127.0.0.1:9000
            +-- conn3 --+
                 ^ 服务端进程一死，三条一起掉，旋转换不到任何健康目标
```

于是三件事同时失效：

| 砍掉的 | 为什么 |
|---|---|
| 连接池 + Round Robin | 池里的成员全活全死，轮询是空转。多 loop 分布（池唯一站得住的理由）本阶段不做，见「本阶段不做」 |
| 幂等白名单 + Retry | 换连接重发 = 换到另一条同样断的 socket 上，拿不到不同结果 |
| `pending_writes_` 排队 + 建连冲刷 | 它存在的意义是「库自己扛过断线窗口」。这个职责归业务（落盘 + 连上后补发），库不该替业务扣住请求 |

顺带删掉 `RpcClient` 外壳（09-27 落地、同日删除）：剥掉 RR 与 Retry 后它只剩「继承
`protobuf::RpcChannel` 让 Stub 认得一个指针」，而 Stub 本来就能直接拿 `RpcChannel*`
（`examples/rpc_echo/rpc_echo_client.cpp:34-35`），一层转手没有内容。

### 职责边界（本节的落脚点）

```text
库（RpcChannel）   连接能不能用？不能用就当场失败 + 说清原因；连接自己回来（Connector 退避重连）
业务                收不收请求（拦截）、要不要落盘、连上后补发、给请求设 deadline
```

于是 `CallMethod` 的路由变成一条直线，不再有中途态：

```text
CallMethod
  +- 参数 / 序列化 / 编码 不合法  -> Invalid            （per-call，重发必然重现）
  +- 在途闸门越线                -> Failed + 降级钩子    （rpc_channel.cpp:247）
  +- 连接不可用                   -> Failed + connection_error_   （:243）
  +- 可用                         -> Send，进 pending_calls_ 等响应 / 超时 / 取消
```

`kConnecting` 与 `kDisconnected` 现在**都拒发**，区别只在原因能不能自愈：

| 状态 | 谁写入 | 语义 | 业务该怎么读 |
|---|---|---|---|
| `kConnected` | `OnConnection(conn)` 成功（`:365`） | 可用 | 正常发 |
| `kConnecting` | 初值；掉线 `OnConnection(∅)`（`:382`） | 暂时不可用，`Connector` 在退避重连 | 这次失败，稍后重发 |
| `kDisconnected` | 协议错误（唯一写点，`:406`） | 永久下线，重连治不好 | 别再试，先查协议 / 版本 |

业务唯一能拿到的线索是 `RpcController::ErrorText()`，它是**四个稳定串**（09-27 归位，见第六批）：

| `ErrorText()` | 谁写的 | 对应哪一批请求 | 业务动作 |
|---|---|---|---|
| `"RPC connection closed"` | 掉线分支（`:383`） | **掉线那一刻在途**的（发出去过，可能已被服务端执行） | 落盘，带幂等键补发 |
| `"RPC connection is not available"` | 闸门（`:239-241`，`kConnecting` 固定用它） | 空窗期 / 冷启动**新发**的（一次都没发出） | 直接补发即可 |
| `"RPC protocol error: ..."` | 协议错误（`:407`，落 `kDisconnected`） | 该连接上全部在途 | 永久下线，重试无用，先查协议 / 版本 |
| `"too many pending rpcs"` | 过载闸门（`:252`） | 被本端在途上限拒的 | 立刻重试只是加压，应退避 |

前两行刚好对应「**可能已执行**」与「**肯定没发出**」（闸门在 `Send` 之前就 return 了）——
但**不要拿字符串做幂等决策**，幂等键两边都要带。

**闸门按状态取原因，不按字符串取**（09-27 修正，此前是「`connection_error_` 非空就透传」）：

```cpp
const std::string& reason = connection_state_ == ConnectionState::kDisconnected
                                ? connection_error_                  // 永久态：协议错误原文必须给业务
                                : "RPC connection is not available"; // 暂时不可用：固定串
```

修正的动机是一条**测试跑出来的真缺陷**（详见第六批）：`Connector` 每次重连尝试失败都会把 net 层的
per-attempt 原文（`"connect failed: " + strerror(errno)`，`connector.cpp:177/227/266`）写进
`connection_error_`，**覆盖**掉线分支刚写的 `"RPC connection closed"`。后果是**同一个故障、同一段
代码，业务拿到什么字符串取决于它什么时候问、以及对端在哪儿**（本机 refused 瞬时覆盖；远程主机不通
时 connect 挂几秒，那几秒里又是另一个答案）。**业务没法写 `==`** —— 所以「透传」这个动作本身没错，
错的是槽里的字不是我们的。

### 连接失效的发现手段（本阶段补的最后一块）

前面几种断开（客户端主动 / 服务端优雅关 / 进程崩溃 / 慢消费者被踢 / 协议不兼容 / 中间设备丢表）
都能落到 `FIN` 或 `RST`，`read()` 立刻有结果。**唯独「服务端断电、拔网线、进程 hang」什么都不发** ——
客户端不发数据就永远发现不了，那条连接会一直占着 `kConnected`。

补法是两层：

```text
net 层    SO_KEEPALIVE + TCP_KEEPIDLE=10s / KEEPINTVL=3s / KEEPCNT=3      socket.cpp:92-104
          最快 10s、最坏 19s 内核判定对端不可达 -> read/write 报 ETIMEDOUT

错误出口   TcpConnection::HandleError() 原来只 getsockopt 读走 SO_ERROR 就丢掉、什么都不做，
          于是探测失败也不会关连接、更不会重连（删掉队列后才暴露出来的真缺口）。
          现在读走错误后 ForceCloseInLoop()                                  tcp_connection.cpp:251-263
```

链路因此闭合：

```text
keepalive 探测耗尽
  -> 内核把连接置错 -> epoll 报事件 -> read/write 返回 ETIMEDOUT
     -> HandleError -> ForceCloseInLoop -> HandleClose
        -> close_callback_ -> TcpClient::RemoveConnection
           -> connect_ 仍为真 -> Connector::Restart() -> 退避重连
```

**覆盖边界（诚实记账）**：keepalive 由对端**内核**应答，所以它证明的是「对端机器和网络还活着」，
不是「服务端进程还在干活」。进程 hang 但内核正常时探测会成功 —— 那种情况只能靠应用层心跳，
归第 11 节。

### 实施步骤（按实际落地顺序）

1. `Connector` 退避重连（自包含，可单独验收）—— **已落地**
   - 新增 `ReconnectPolicy{initial=100ms, multiplier=2, max=5s}` 与 `SetReconnectPolicy`；
   - `ReportError`（`connector.cpp:292-301`）末尾挂 `ScheduleReconnect()`（`:303-327`）：
     退避翻倍封顶、连上后复位（`:149` / `:239`）；
   - 新增 `Restart()`（`connector.cpp:64-71`）：把 `state_` 从 `kConnected` 带回起点再
     `StartInLoop()` ——「已建连接掉线后重启」的唯一入口。注意它的语义是**外部报丧 +
     内部重置起点**：`Connector` 建连成功即退役、状态天然冻结，它自己发现不了掉线，
     必须由第 2 步的 `TcpClient::RemoveConnection` 叫醒；
   - `Stop()`（`:83-99`）与析构（`:28`）里 `CancelReconnect()`，否则定时器会拖住
     `Connector` 不放。**取消必须排在 `StopInLoop` 的守卫之前** —— 等待重连期间
     `state_` 恰好是 `kDisconnected`，排在后面就取消不掉；
   - 新增 `ReconnectAttemptCount()` 供验收观测。
2. `TcpClient` 接上自愈 —— **已落地**
   - `HandleConnectError` 不再无条件 `connect_ = false`（`tcp_client.cpp:126-135`），
     否则连接器刚开始重试就被自己关掉，而且重连成功的新 fd 会被 `NewConnection` 的
     `if (!connect_)` 直接 `::close` —— 退避看着在跑，永远接不上；
   - `RemoveConnection` 补 `connector_->Restart()`（`tcp_client.cpp:115-119`）——
     **杀掉服务端进程后客户端能自己爬起来，缺的就是这条报丧线**；
   - ~~对外给 `bool Reconnecting() const`~~ **不做**：`TcpClient::Disconnect()/Stop()` 全项目
     零调用者，所以 `RpcChannel` 存活期 `connect_` 恒为真，`OnConnection(∅)` 无条件回
     `kConnecting` 就够了 —— 加了是零调用者的预置 API。**边界**：将来若真有第二条连接要
     走 `Disconnect()` 淘汰，必须补这个判据。
3. `RpcChannel` 改为「不可用即拒发」—— **已落地**
   - `RegisterAndSend` 的闸门判据从 `== kDisconnected` 改成 `!= kConnected`
     （`rpc_channel.cpp:284`），两种不可用状态一起拒发；
   - 删掉 `pending_writes_` 队列、建连冲刷循环、`FailSentPending` 差集判定、
     `max_pending_writes_` 闸门与 `SetMaxPendingWrites` / `PendingWriteCount` 两个 API；
     掉线分支改用 `FailAllPending`（`:384`）；
   - 第四步降级为纯防御检查（`:346-353`）：闸门与发送之间同 loop 无交错，走到这里必然可用；
   - 删掉 `OnConnectError`（函数 + 声明 + 构造里的注册，共 3 处，见第六批）—— 它原本只往
     `connection_error_` 里记 net 层每轮重连尝试的原文，属越权；闸门改为**按状态**取原因
     （`:239-241`）：`kDisconnected` 透传协议错误原文，`kConnecting` 固定
     `"RPC connection is not available"`；
   - 协议错误改走 `client_->Disconnect()`（`:411`）：既替掉原来的 `conn->Shutdown()`，
     又把 `connect_` 置假、断掉自愈 —— 否则会陷入「连上 -> 报错 -> 重连」死循环；
   - **不需要为超时写任何新代码**：deadline 定时器在入队前就挂好了（`:323`），
     请求生命周期内照常触发。
4. 连接失效发现（静默死亡）—— **已落地**
   - `Socket::SetKeepAlive`（`socket.cpp:92-104`）四个 `setsockopt`；
   - `TcpConnection` 构造里开启，参数 `10s / 3s / 3`（`tcp_connection.cpp:18-20`、`:49-52`）；
   - `TcpConnection::HandleError()` 补上 `ForceCloseInLoop()`（`tcp_connection.cpp:251-263`）。

### 本阶段不做

| 不做 | 理由 |
|---|---|
| 连接池 / LB / Retry | 单 endpoint 下全活全死，见「为什么砍掉」 |
| 一致性哈希 | 没有多 endpoint 时说不到选路 |
| 客户端多 loop 分布 | 验收标准都不依赖它；1/2/4/8 扩展性数据属第 12 节，现在做只能写「连接分散到不同 loop」一句，量不出东西 |
| 应用层心跳帧 / 新 `MessageType` | keepalive 已覆盖机器与网络不可达；心跳只为「进程 hang」服务，代价是动 `rpc_meta.proto` + `rpc_codec` + `rpc_channel::OnMessage` + `rpc_server::OnMessage` 四处。**归第 11 节** |
| 库层「连接不可用」的主动通知 API | 当前由单请求失败携带原因，业务据此拦截（见「职责边界」的 reason 表）；要不要再加一个连接级回调，等有真实业务再说 |

### 验收标准

- 杀掉服务端进程 -> 客户端自动重连 -> 服务端恢复后 RPC 成功；—— **已覆盖**
  （`tests/rpc_reconnect_test.cpp`：掐掉 `RpcServer` -> 窗口期请求当场被拒 -> 重启 -> 新请求成功）
- **连接不可用期间请求一次也不发、当场失败并带原因**；—— **已覆盖**
  （同上：窗口期 3 发的 `failed_after_issue == 3`，且 `ok` 只该有首尾两发）
- 断连期间在途请求全部以失败完成、无泄漏；—— **已覆盖**（第 9 节
  `rpc_backpressure_test.cpp` Case 4，reason `"RPC connection closed"`）
- **退避可观测**：重连尝试间隔按 100/200/400/800ms… 递增并封顶 5s，服务端恢复后计数停止增长；
  —— **已覆盖**（`playground/02_connector_backoff.cpp` 打时间戳、`03_client_reconnect.cpp` 打
  掉线 -> 重连的回调序列）
- **静默死亡的发现**：keepalive 生效 —— 代码可读（`socket.cpp:92`、`tcp_connection.cpp:49`），
  但**没有用例**：造「对端断电」得靠 iptables DROP 或拔网线，纯软件的测试里做不出假半开连接。
  **诚实记账：机制在，无自动化验证。**
- **对外原因是稳定串**：`ErrorText()` 只会是上面表里的四个常量之一，不含 `strerror` 原文；
  —— **已覆盖**（`rpc_reconnect_test` 的 `[final] first_error` 应打印
  `"RPC connection is not available"`；第六批之前它打印的是 `"connect failed: Connection refused"`）。
- ~~RR 分布 / 重试白名单~~ —— **已随 `RpcClient` 一并删除**（对应目标已从本节移出）。

### 实际落地（2026-09-27）

**第一批 · net 层自愈**，改 5 个文件（一路用探针看回调序列、一路用集成测试看 RPC 成功）：

| 文件 | 改了什么 |
|---|---|
| `nebula/net/connector.h` | `ReconnectPolicy` + `SetReconnectPolicy` / `Restart()` / `ReconnectAttemptCount()`；私有 `ScheduleReconnect` / `CancelReconnect` / `RestartInLoop` + `TimerId retry_timer_` + `next_backoff_` |
| `nebula/net/connector.cpp` | `ReportError` 末尾 `ScheduleReconnect()`；成功路径两处 `next_backoff_ = {}`；`StopInLoop`/析构 `CancelReconnect()`；新增 `Restart` / `RestartInLoop` |
| `nebula/net/tcp_client.cpp` | `HandleConnectError` 去掉 `connect_ = false`；`RemoveConnection` 补报丧线 |
| `tests/rpc_reconnect_test.cpp` | 跨层闭环：正常一发 -> 掐服务端 -> 窗口期 3 发被拒 -> 重启 -> 新请求成功 |

第一批的一条判断：**协议错误永久下线**要靠 `client_->Disconnect()`（挡「重新连上」）加掉线分支的
`kDisconnected` 守卫（挡「状态被改写」），两条缺一不可。

**第二批 · 失败出口统一**（09-27，读代码时发现，2 个文件 10 个点）

`CompleteFailure` 是**全部失败出口**的共同落点，可它只 `SetFailed`、**不写 `CallState`** ——
于是同一批失败里，走 `CompleteCallWithFailure` 的写了终态、走 `CompleteFailure` 的停在 `Pending`。
症状：`Failed()` 报 true 而 `CallState()` 报 `Pending`，与「完成终态；未完成时是 Pending」
自相矛盾（done 明明已经跑过了）。根因不是漏写某一句，是**出口不唯一** —— 所以修法是把终态
收到一个口，而不是补 6 句。

| 行（当前） | 场景 | 状态 |
|---|---|---|
| `:150` | `done == nullptr` | `Invalid` |
| `:161` / `:177` / `:199` | 参数不合法 / 序列化 / 编码 | `Invalid` |
| `:242` | 连接不可用快失败 | `Failed` |
| `:259` | deadline 已过 | `Timeout` |
| `:533` | `CompleteCallWithFailure` 内 | `state` |
| `:619` | `FailAllPendingNow` 内 | `Failed` |
| `:668` | `RejectOverloaded` 内 | `Failed` |
| `:659` | 降级钩子接管分支 | `Completed` |

改动：`rpc_call.h` 加 `Invalid`；`rpc_channel.cpp:97-109` 的 `CompleteFailure` 加 `RpcCallState state`
（**不给默认值**）并在内部 `SetCallState`。

1. **必须新增 `Invalid`，不能复用 `Failed`。** 三个 per-call 错误若标 `Failed`，重试逻辑
   就会对**参数不合法**开火。不取 `NotSent`：协议错误与在途闸门满**同样"未发出"**，
   却要归 `Failed` —— 判据是**故障归属**（per-connection 换连接有意义 / per-call 换也没用），
   不是「字节出没出去」。
2. **`state` 不给默认值**是手段不是洁癖：让编译器在**每个调用点**拦住漏写 —— 这比补 6 句
   `SetCallState` 可靠，而且下一次新增失败出口时自动生效。
3. **降级钩子接管写 `Completed`**，与 `CompleteFrame` 那条路**同构**：钩子注释
   （`rpc_channel.h:57`）写着「返回 true 表示钩子已自行完成该次调用」，即往返被收口（`Completed`）、
   业务是否失败由 `Failed()` 表达，`rpc_awaiter.h` 的兜底 throw 接住。

**第三批 · 砍掉 `RpcClient`，改为失效拦截**（09-27）

见「为什么砍掉连接池 / LB / Retry」与「实施步骤 3」。删除
`nebula/rpc/rpc_client.{h,cpp}`、`tests/rpc_client_test.cpp`、`tests/rpc_pending_writes_test.cpp`；
`ResolveDeadline` 从 `rpc_controller.{h,cpp}` 退回 `rpc_channel.cpp` 的匿名命名空间（只剩一个使用者）。

删除暴露出的两个真缺口（都不是本次新引入的，是原有代码里的）：

1. **`TcpConnection::HandleError()` 是空壳** —— 只 `getsockopt` 读走 `SO_ERROR` 就返回，
   不关连接。于是 **RST 与写失败都不会让连接进入 `HandleClose`**，也谈不上重连。
   第 9 节之所以没发现，是因为当时的断开用例（`ForceClose` / 服务端析构）走的都是
   `read() == 0` 那条路。本批补上 `ForceCloseInLoop()`。
2. **静默死亡无任何兜底** —— 全库无 `SO_KEEPALIVE`。本批补上，见「连接失效的发现手段」。

**第四批 · 静默死亡发现**（09-27）

`socket.h/.cpp` 加 `SetKeepAlive`；`tcp_connection.cpp` 构造里开启（`10s / 3s / 3`）；
`HandleError` 补 `ForceCloseInLoop()`。

**第五批 · SIGPIPE 崩溃修复**（09-27，跑 `rpc_reconnect_test` 时暴露）

`tcp_connection.cpp` 两处裸 `::write`（`:202` 补写 / `:279` 直写）换成 `::send(..., MSG_NOSIGNAL)`。

这是本批第三个真缺口：**对端 RST 之后任何一次写都会让内核产生 `SIGPIPE`，默认动作是终止进程** ——
`SendInLoop` 里那个 `written < 0 → HandleError()` 分支（`:292-300`）写了等于没写，
进程在 `write` 返回之前就被信号杀掉了。

触发窗口**无法在应用层消除**：服务端进程刚死、FIN 还没到的那几十毫秒里，通道仍认为自己是
`kConnected`，闸门放行 → 请求真写到 socket 上。所以只能保证 write 不崩、让它返回 `EPIPE`
走 `HandleError`。`notes/knowledge/02` §9.1 早就记录了这个隐患（当时写的是「未处理」），
本批按修法 A 落地。`TcpConnection` 服务端也在用，所以两侧一起修好。

**第六批 · 对外原因归位**（09-27，跑通 `rpc_reconnect_test` 后按实测修正）

用例全绿，但 `first_error` 打印出来的是 `"connect failed: Connection refused"` ——
**源码里搜不到这四个单词**。它来自 `connector.cpp:266` 的
`"connect failed: " + std::strerror(ECONNREFUSED)`：字面量是我们的，`Connection refused`
是 libc 把 `errno = 111` 翻出来的。链路是：

```text
Connector 每轮重连尝试失败
  -> ReportError("connect failed: " + strerror(errno))        connector.cpp:177/227/266
     -> TcpClient::HandleConnectError                          tcp_client.cpp:133
        -> RpcChannel::OnConnectError                          （本批已删）
             connection_error_ = reason   <- 【覆盖】掉线分支刚写的 "RPC connection closed"
                -> 闸门透传 -> 业务看到的是 libc 的句子
```

两个后果，一个坏一个好：

- **时序不确定**（坏）：`reset()` 之后 10ms 发请求拿到 `"RPC connection closed"`，
  120ms 发就变成 refused；远程主机不通时 connect 会挂几秒，那几秒里又是另一个答案。
  同一故障、同一段代码，答案随「什么时候问」和「对端在哪儿」变。
- **层次越权**（坏）：`connection_error_` 是 rpc 层「这条路为什么不可用」的语义槽，
  被写入了 net 层单次尝试的细节。libc / kernel 的词顺着回调漏进了业务可见字段。
- **顺手捞回的区分度**（好）：掉线那刻在途的（`FailAllPending`，`:383`）与空窗期新发的
  （闸门，`:239-241`）本来就是「可能已执行」与「一次没发出」两批 —— 归位后它们各自
  对应一个**原子串**，不再依赖 `strerror`。

修法三处：删 `OnConnectError` 函数、删 `rpc_channel.h` 里的声明、删构造里的
`SetConnectErrorCallback` 注册；闸门从「按字符串取」改成「按状态取」。
`TcpClient::SetConnectErrorCallback` **保留** —— `playground/03_client_reconnect.cpp:46`
正在用它看重连节奏，那是 net 层该有的能力，只是 rpc 层不该把它的输出当自己的原因。
`connection_error_` 从此只在 `kDisconnected` 时被读，语义收缩为「这条路永久废掉的原因」。

**删除带来的行为变更（要记住的三条）**

1. **掉了线就当场失败，不再排队等冲刷。** 服务端重启的那一两百毫秒里，业务发出的请求全部失败，
   由业务自己退避重发。这是本批最大的语义变化，也是 `tests/rpc_reconnect_test.cpp` 断言
   与之前完全相反的原因。
2. **冷启动的首个请求必失败。** `connection_state_` 初值就是 `kConnecting`
   （`rpc_channel.h:88`），进程刚起、`Connector` 还在建连的那几十毫秒里任何请求都被拒。
   业务需要「启动后先探活再放量」。
3. **「肯定没执行」这个集合没了。** 队列天然把掉线时刻的请求分成「已 Send（服务端可能执行过）」
   与「只在队列（肯定没执行）」两类；取消队列后**全部**归为「可能执行过」，
   业务补发时每一条都要走幂等判断，且整体是 at-least-once。

### 与草案的差异

1. **草案漏了「连接建立后再掉线」这条线。** 草案只提「连接失败即永久停止」，且把
   `connect_ = false` 归给 `Connector` —— 实际那行在 `TcpClient::HandleConnectError`
   （改动前位于 `tcp_client.cpp:122`，09-27 已删除）。更关键的是「连接建立后再掉线」这条线
   **根本没人接**：`Connector` 建连成功即**退役** —— fd 交出去、channel 摘掉、
   `socket_fd_ = -1`（`connector.cpp:220-221` / `:242`），它不再持有那条连接的任何句柄，
   所以 `state_` 也没有从 `kConnected` 出发的转换出口（全部 `state_ =` 赋值都发生在建连之前）；
   而掉线走的是
   `TcpConnection::HandleClose -> close_callback_ -> TcpClient::RemoveConnection`
   （`tcp_client.cpp:103-124`），**不经过 Connector**。两件事叠加，掉线后就没有任何人叫它重新连；
   `StartInLoop` 的守卫又要求 `state_ == kDisconnected`（`connector.cpp:76`），即使有人调 `Start()`
   也是空转。而进程被杀走的正是这条「先连上、后掉线」的路 —— 只修连接失败的重试，验收第 1 条
   依然过不了。**两条缺口已于 09-27 补上**（报丧线 + `RestartInLoop`）。
2. **草案的四件套砍成一件。** 连接池 / LB / Retry 在单 endpoint 下全是空转，
   见「为什么砍掉连接池 / LB / Retry」；本节的实质内容收缩为**连接自愈 + 失效拦截**。
3. **草案把「断开」当终态。** 重连窗口内 `kConnecting` 就是「暂时不可用」，
   理由见「职责边界」的状态表。
4. **草案漏了 `HandleError` 空壳、keepalive 与 SIGPIPE；也没料到对外原因会被 net 层覆盖。**
   见第三、四、五、六批。第四、五批是前者的兜底，第六批是「rpc 层的原因槽被 net 层的
   per-attempt 字符串污染」—— 草案只考虑了「状态机怎么走」，没考虑「业务读到的字符串谁写的」。
5. **草案给 Retry 配了独立退避定时器，随 Retry 一起没了。** 退避在 `Connector` 那层本来就有。
6. 顺带修正草案一处措辞：`pending_writes_` 的上限第 9 节已经落地过
   （`max_pending_writes_`，reason `"too many queued rpcs"`）—— 但该队列本批已删，
   第 9 节那条「测不到」的欠账随之作废（机制不存在了，不是没测）。

------------------------------------------------------------------------

## 11. Observability / Metrics / Trace / Structured Log / Graceful Shutdown

来源：

    NebulaRPC 新能力

目标：

-   spdlog 日志
-   指标采集
-   请求追踪
-   优雅关闭

状态：**已落地（2026-09-28，待编译验证）**。五块全部实现；下面「实际落地」一节记录与草案的四处偏差。

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
5. 应用层心跳（**第 10 节移入的欠账**）：
   - 背景：第 10 节用 `SO_KEEPALIVE`（10s / 3s / 3）补了「静默死亡」的发现手段，但它由对端
     **内核**应答，只能证明「对端机器和网络还活着」—— **服务端进程 hang 住而内核正常时，
     探测照样成功**，客户端仍以为连接可用；
   - 做法：RPC 层加一个心跳方法（`rpc_meta.proto` 增 `HEARTBEAT` 类型，或复用一个预留
     method），客户端定时发、服务端原样回；连续 N 次无响应 -> 判连接失效 ->
     `ForceClose` 走报丧线 -> 重连；
   - 代价：动 `rpc_meta.proto` + `rpc_codec` + `rpc_channel::OnMessage` + `rpc_server::OnMessage`
     四处，还要定「间隔 / 判定次数」两个常量；
   - 与 keepalive 的分工：keepalive 管「机器 / 网络层没了」（内核探测比 TCP 重传快得多），
     心跳管「进程不干活了」。两者互补，**keepalive 仍要保留**。

验收标准：

- 一次 RPC 的日志能串起 发起 -> 完成 全链路（同一 request_id）；
- 周期指标输出的 P99 与手工统计一致；
- 优雅关闭期间已收到的请求全部正常返回，不丢请求。

### 实际落地（2026-09-28）

五块都落了。**与草案的四处偏差**（草案是按想象写的，实现被现实改了）：

1. **④ 的 `SIGTERM -> loop->QueueInLoop(server.Stop)` 不合法，已改**。
   `QueueInLoop` 要取 `mutex_` 并构造 `std::function`，两者都不是 async-signal-safe 的 ——
   在信号上下文里调用是未定义行为（最坏是与被中断线程自己的取锁死锁）。
   实际做法：`EventLoop::NotifyFromSignal()`（`event_loop.cpp:188`）只做**原子置位 + `::write(wakeup_fd_)`**
   两件信号安全的事；`Loop()` 每轮迭代末尾检查 `signal_pending_`（`event_loop.cpp:93`）后
   才回 loop 线程执行 `signal_callback_`（`event_loop.h:39/72/76`）。
2. **④ 的 `TcpServer::Stop()` 拆成了两个方法**。草案把「停 accept -> 等 in-flight -> 逐个 Shutdown」
   写成一个方法，但 `TcpServer` 不知道什么叫「in-flight」（那是 RPC 层的概念）。实际拆为
   `Stop()`（`tcp_server.cpp:55`，停 accept）、`CloseAllConnections(on_all_closed)`（`:78`，
   逐个 Shutdown 并在连接全部退场后回调）；**等 in-flight 由 `RpcServer` 负责**。
3. **③⑤ 都不用动 `rpc_codec`**。`RpcCodec::Encode/Decode` 对 `type` 完全无感知（meta 整体序列化），
   加 `trace_id` 与 `HEARTBEAT` 都不需要改 codec。草案里「改 4 处」实际是 3 处：
   `rpc_meta.proto` + 客户端 `HandleFrame`/心跳定时器 + 服务端 `OnMessage` 分流。
4. **① 的 service/method 用描述符指针存，不存字符串**。
   `PendingCall` 里放 `const MethodDescriptor*`（`rpc_channel.h:79`，8 字节、指向生成代码的静态对象），
   完成日志直接从它取 `service()->full_name()` / `name()`；只有指标表才拼 `"service.method"` 串做 key。
   这样不给 §12 的分配热点清单凭空加一条 per-call 字符串。

**关键锚点**：

| 块 | 落点 |
|---|---|
| ① 结构化日志 | `rpc_channel.cpp:246`（发起日志 `rpc call start`）、`:891` `LogCompletion`（打点 `:906`，固定字段 request_id/outcome/service/method/latency_ms/error）、`:455` 断连日志、`:484` 协议错误、`rpc_server.cpp:228` dispatch |
| ② Metrics | 新增 `nebula/rpc/rpc_metrics.h` / `.cpp`；`LatencyHistogram` 桶界 `kBoundsUs` = 1/5/10/50/100/500/1000ms + 溢出桶；`RpcChannel::StartMetricsReport`（`rpc_channel.cpp:607`）、`ReportMetricsInLoop`（`:656`）；`RpcMetrics::TakeCycleCount()` 由 channel 计时算 QPS，`Dump()` 只出计数与 P50/P95/P99 |
| ③ Trace | `rpc_meta.proto:20` `string trace_id = 8`；`GenerateTraceId()`（`rpc_channel.cpp:28`，进程盐 + 计数 + 时钟 → 16 位 hex）；服务端 `SendResponse`/`SendError` 原样带回 |
| ④ 优雅关闭 | `TcpServer::Stop`（`tcp_server.cpp:55`）/ `CloseAllConnections`（`:78`）/ `CheckAllClosedInLoop`（`:107`，由 `RemoveConnectionInLoop:174` 驱动）；`Acceptor::Stop`（`acceptor.cpp:33`，摘读事件，**listen fd 留给对象析构**）；`RpcServer::Stop/StopInLoop`（`rpc_server.cpp:62/70`）、`CloseConnectionsInLoop`（`:97`）、`FinishShutdownInLoop`（`:121`）、`OnCallFinished`（`:145`）、`in_flight_` 计数（`rpc_server.h:81`）；总上限 `kGracefulTimeout = 10s`（`rpc_server.cpp:21`） |
| ⑤ 应用层心跳 | `rpc_meta.proto:10` `HEARTBEAT = 3`；常量 `kHeartbeatInterval = 3s` / `kHeartbeatMaxMissed = 3`（`rpc_channel.cpp:24-25`）；客户端 `ScheduleHeartbeatInLoop`（`:518`）/ `OnHeartbeatTimer`（`:557`，连丢 3 次 → `ForceClose` 走报丧线）；服务端 `SendHeartbeatEcho`（`rpc_server.cpp:297`），`OnMessage` 在 REQUEST 判定**之前**分流（`:175`） |
| 演示 | `examples/rpc_echo/rpc_echo_server.cpp:43/76/80/84`（SIGTERM → 优雅关闭）；新增 `rpc_echo_observability_client.cpp`（持续打流 + 周期指标） |

**两处口径选择（不是疏漏，是刻意）**：

- **没进 `pending_calls_` 的调用也计入指标**（连接不可用 / 过载拒绝 / 入队前已超时，`rpc_channel.cpp:291/301/318`）。
  否则「被闸门拦下」这一整类请求在统计里不存在，`failed` 会被系统性低估。
- **对端回 `ERROR` 帧计入 `failed`**（按对端结果记，`rpc_channel.cpp:765`），但完成权仍是 `Completed` ——
  `RpcCallState` 表达的是「谁抢到完成权」，不是成败，这条语义没动。

**本阶段不做 / 未验证（诚实记账）**：

- **代码未编译未运行**（按护栏由用户自行验证）；
- **心跳的「进程僵死」场景没有自动化验证** —— 要造一个「内核活、进程不干活」的对端，测试不可控；
  与 §10 keepalive 同性质，不写偶发红的测试；
- 验收标准第 1 条（日志串链）与第 3 条（优雅关闭不丢请求）目前只有示例可人工观察，**没有断言用例**；
- **服务端无 metrics**：`超时` 本身就是客户端概念，本轮指标只做客户端侧；服务端 QPS 由 §12 从外部测；
- `Stop()` 不立即 `close` listen fd（交给 `~Acceptor` → `~Socket`），与 §10 的 FIN 是同一条纪律：
  「逻辑置死」与「释放 fd」分两处；
- `trace_id` 只支持本端生成，**没有「透传上游」的入口**（当前没有上游可透传，等真有 service→service 再加 setter）。

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

阶段推进方式：

    Roadmap 只记录「这一阶段要达成什么」
        ↓
    上一阶段落地后，按当时的代码现状重写下一阶段的实现方案
        ↓
    实施 -> 测试验证 -> 笔记 -> 进入下一阶段

Roadmap 里的实现步骤都写于开工之前，看不到上一阶段最终长成什么样。
所以每进入新阶段，先核一遍现有家底的 file:line，再改本节方案 —— 沿用草案
等于按「上一阶段之前」的代码假设施工。第 10 节就是按这条规则重写的第一例。

当前推进顺序：

    第 1 ~ 9 节  已完成（第 9 节 2026-09-26 封版，Case 1~4 全部通过）
        ↓
    第 10 节 Client Runtime      <- 已收口（09-27）：连接自愈 + 失效拦截 + keepalive；
                                      连接池 / LB / Retry / RpcClient 已按单 endpoint 现状砍掉
        ↓
    第 11 节 Observability       <- 含第 10 节移入的「应用层心跳」欠账
        ↓
    第 12 / 13 节 性能与对照
