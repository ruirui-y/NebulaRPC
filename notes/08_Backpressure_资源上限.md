# 08 · Backpressure / 资源上限 / 过载控制

状态：**已封版（2026-09-26）**。Case 1~4 全部通过（Linux / Debug / `ctest -R backpressure`；直跑末行为 `RPC backpressure test passed`）。配套验收试卷：`NRPC-BP-V1`（已归档）、**`NRPC-BP-V2`（现行）**。

一句话：给四个「无上限资源」各装一道闸门 —— 两个水位（输出/输入）走 `TcpConnection`，两个计数上限（在途/排队）走 `RpcChannel`；闸门一律默认关闭（0 = 不限制），所以存量行为零变化。

---

## 1. 四个无上限点（Roadmap 只列了三个）

| # | 资源 | 位置 | 怎么爆 | 闸门 |
|---|---|---|---|---|
| 1 | `output_buffer_` | `nebula/net/tcp_connection.h:103` | 服务端回包速度 > 客户端读速度（慢消费者） | 软水位 + 硬上限 |
| 2 | `input_buffer_` | `nebula/net/tcp_connection.h:102` | 对端灌无效流量，上层认不出帧、也不排空 | 输入水位 |
| 3 | `pending_calls_` | `nebula/rpc/rpc_channel.h:132` | 客户端无限堆在途请求 | `SetMaxPendingCalls` |
| 4 | **`pending_writes_`** | `nebula/rpc/rpc_channel.h:133` | **未建连时的排队完全无界** | `SetMaxPendingWrites` |

第 4 个是草案漏掉的。它原来只受「连接是否很快建立」影响 —— 连接一直建不起来，队列就能一直涨。`RegisterAndSend` 的路由是：先进 `pending_calls_`，**再**决定「直发」还是「进 `pending_writes_`」，所以两个上限是叠加的，不是二选一。

---

## 2. 改动清单

```
nebula/net/callbacks.h
  + HighWatermarkCallback = function<void(const TcpConnectionPtr&, size_t pending_bytes)>

nebula/net/tcp_connection.h/.cpp
  + SetHighWatermarkCallback(cb, bytes)   软水位
  + SetMaxOutputBufferBytes(limit)        硬上限，越线断连
  + SetMaxInputBufferBytes(limit)         输入水位，越线断连
  + ForceClose() / ForceCloseInLoop()     立即关闭，不等 output_buffer 排空
  + NotifyHighWatermark(before, after)    越线判定 + 通知
  + PendingOutputBytes() / HighWatermarkCount() / OverloadCloseCount()   观测

nebula/net/tcp_server.h/.cpp
nebula/net/tcp_client.h/.cpp
  + 同名的三个透传配置，在 NewConnection 里逐条应用到新建连接

nebula/rpc/rpc_channel.h/.cpp
  + SetMaxPendingCalls / SetMaxPendingWrites / SetDegradeHandler
  + RejectOverloaded()                    过载收口：先问降级钩子，再回落直接失败
  + PendingCallCount() / PendingWriteCount() / OverloadRejectCount()   观测

nebula/rpc/rpc_server.h
  + SetHighWatermarkCallback / SetMaxOutputBufferBytes / SetMaxInputBufferBytes 转发给内部 TcpServer

examples/rpc_echo/rpc_echo_server.cpp
  + 软水位 4MB 只打日志；硬上限 16MB 兜底断连

tests/rpc_backpressure_test.cpp（新增，4 个 case，全部通过）+ tests/CMakeLists.txt
```

---

## 3. 五个关键决策

### 3.1 软水位不需要低水位，也不需要迟滞状态

判据只有一行（`tcp_connection.cpp` `NotifyHighWatermark`）：

```
before < 水位 <= after        // before = 追加前待发量，after = 追加后待发量
```

这条**天然防抖**，不需要额外的 bool 或 resume 水位：

```
缓冲一直高于水位   → before >= 水位      → 不触发          ✔ 不重复通知
降到水位下再涨上来 → before < 水位 <= after → 触发一次     ✔ 这是新的一次超载事件，应该通知
```

对比 BOSS3 的 write_queue 三水位迟滞（soft/hard/resume）：那里的 resume 水位解决的是「业务暂停之后何时恢复发送」。这里恢复信号用**已存在的** `WriteCompleteCallback`（`tcp_connection.cpp:193-200`，缓冲排空时触发）。少一个状态、少一处可能不同步的配置。

代价：恢复粒度粗 —— 必须等到缓冲**全空**才恢复，而不是「降到低水位以下」。对 echo 这种每请求一份响应的场景够用；要精确恢复再加 low watermark。

### 3.2 硬上限必须 `ForceClose`，不能用 `Shutdown`

`Shutdown()` 是 graceful，它会等 buffer 发完：

```
ShutdownInLoop()                                  tcp_connection.cpp
  if (!channel_->IsWriting()) { ShutdownWrite(); }
  ↑ IsWriting 为真时什么都不做，等 HandleWrite 排空后再关
```

慢消费者永远不读 → socket 永远不可写 → epoll 永远不报 `EPOLLOUT` → buffer 永远排不空 → **连接永远不关，内存永远不释放**。

所以硬超限走 `ForceCloseInLoop()`：`RetrieveAll()` 丢弃待发数据 + `DisableWriting()` + `HandleClose()`。这是**唯一**能让内存立刻回落的路。

### 3.3 不做统一的 `OverloadPolicy` 枚举

草案写「三策略按场景配置：reject / queue / degrade」。但这三个**不在同一个决策点上**：

| 草案策略 | 实际落点 |
|---|---|
| reject | `max_pending_calls_` / `max_pending_writes_` 越线 → 直接失败 |
| queue | **就是 `pending_writes_` 的存在本身**（未建连时排队）＋ 一个上限；「队满转 reject」= `max_pending_writes_` 闸门 |
| degrade | 只给钩子，业务实现 |

硬凑 `enum { kReject, kQueue, kDegrade }` 是**假的统一** —— 三个值作用在不同地方、不能互换，而且会产生「policy=kDegrade 但 handler 为空」这种非法状态。

实际做法：
- 两个上限 = reject（默认）
- `SetDegradeHandler` **设了钩子即启用降级**，钩子返回 `false` 说明它不接管、回落为直接失败
- **没有 enum** —— 钩子存在与否就是策略开关，非法状态在类型层面就不存在

### 3.4 水位回调走 `QueueInLoop`，不内联

与 `WriteCompleteCallback`（`tcp_connection.cpp:196`）保持一致。原因：回调里业务大概率要调 `Shutdown` / `ForceClose`，不能在**写路径中间**改连接状态 —— 那会在 `SendInLoop` 尚未 append 完时触发 `HandleClose`。

### 3.5 硬上限先判、且不入队

`SendInLoop` 里顺序是：**硬上限 → 软水位通知 → 真正 append**。

硬超限时直接 `return`，不 append —— 反正马上要丢整个 buffer，先 append 再 `RetrieveAll` 是白干一次拷贝（大 payload 下这是实打实的开销）。

### 3.6 输入水位的检查点在 `message_callback_` 之后

```
HandleRead:  readv → message_callback_（消费合法帧）→ 检查残留
```

放在回调**之后**：合法帧会被 codec 取走，残留才说明对端在灌无效流量。放在之前会把「一次读进来一整个大帧」误判成攻击。

水位值必须显著大于单帧上限（RPC codec 的 16MB），否则合法大帧会被误杀。

- 测试用 64KB 这种小值，只为验证机制。
- **示例 `examples/rpc_echo/rpc_echo_server.cpp` 当前没有启用输入水位** —— 只装了输出侧两道闸门（软 4MB / 硬 16MB）。输入侧在示例里仍是完全无上限。

---

## 4. 与 Roadmap 草案的四处偏离

| # | 草案 | 实现 | 为什么 |
|---|---|---|---|
| 1 | 判据 `ReadableBytes() + size > 水位` 且「之前未超」 | `before < 水位 <= after` | 一趟判完，不需要维护「之前未超」这个 bool |
| 2 | 只有软水位回调 | 软水位 + **硬上限** | 只靠软回调 + 业务自觉，慢客户端压测过不了 —— 业务不 Shutdown 就还是涨 |
| 3 | 三个资源点 | **四个** | 漏了 `pending_writes_` |
| 4 | reject / queue / degrade 三策略 | 两处闸门 + 一个钩子 | 见 3.3 |

---

## 5. 验收状态

| 项 | 状态 |
|---|---|
| 代码接线 | 已落地（Linux / Debug 编译通过） |
| 在途请求闸门 | Case 1 **通过** |
| 输出水位 + 硬上限 | Case 2 **通过** |
| 输入水位 | Case 3 **通过** |
| **跨层闭环**（被踢连接上的在途调用必须失败） | Case 4 **通过** |
| 高水位回调次数可观测 | `HighWatermarkCount()` / `OverloadCloseCount()` / `OverloadRejectCount()` 三个计数均在实测中生效 |
| 慢客户端内存曲线平稳 | **未做** —— 需要 heaptrack 验证，属遗留项 |

实测输出（2026-09-26，用户本机，四个 case 全文照抄）：

```
---------- backpressure pending limit detail ----------
[gate] server_call_count=3, pending_calls=3, pending_writes=0, overload_reject=3
[calls] total=6, rejected_failed=3, error_text_ok=3, rejected_done=3, hanging_done=0
-------------------------------------------------------
---------- backpressure output watermark detail ----------
[gate] established=1, high_watermark_hits=1, conn_watermark_count=1, overload_closes=1, peak_pending_bytes=4137600
----------------------------------------------------------
---------- backpressure input watermark detail ----------
[gate] connection_closed=1, overload_closes=1
---------------------------------------------------------
---------- backpressure forced close detail ----------
[gate] server_requests=1, overload_closes=1
[calls] done_count=1, failed=1, error_text="RPC connection closed"
-----------------------------------------------------
RPC backpressure test passed
```

四点值得记：

- `high_watermark_hits=1` —— 软水位**只报了一次**，「事件」语义成立：判据 `before < 水位 <= after` 在 buffer 持续高于水位时不会反复触发，没有退化成每 append 一次刷一次。
- `peak_pending_bytes=4137600 < 4MB`（=4194304）—— 硬上限是**入队前判的**（`SendInLoop:287` 用预计值 `ReadableBytes()+remaining` 比较），最后一个成功入队的值就是峰值，永远越不过上限。
- `overload_closes=1` 在 Case 2 / Case 3 各出现一次 —— 两条**不同的路径**（输出超限 / 输入超限）汇到同一处 `ForceCloseInLoop`。
- Case 1 的 `pending_writes=0` —— 排队闸门没参与：`RpcChannel` 构造后到 `CallMethod` 之间连接已就绪，走的全是在途闸门那条路。这与遗留项第 1 条（`max_pending_writes_` 测不到）互为印证。

Case 4 的输出即上面最后一段：`server_requests=1`（服务端 `message_callback` 被调用次数）/ `overload_closes=1`（这条连接上 `OverloadCloseCount()` 累计值）/ `done_count=1`（exactly-once）/ `failed=1` + `error_text="RPC connection closed"`（跨层失败链打通）。

**Case 4 的完整链路**（两端，逐行核实过）：

```
客户端（RpcChannel，真栈）              服务端（裸 TcpServer，哑端）
  t≈0    connect 成功
         └─ 服务端此刻【一个字节都不发】—— message_callback 只在有数据到达时触发
  t=200ms stub.Echo(...)
     RegisterAndSend -> pending_calls_ -> Send 请求帧
                                        HandleRead -> message_callback
                                            RetrieveAll()        不解码、不回包
                                            连发 512 x 64KB 合法帧
                                               SendInLoop 累积
                                               超 4MB -> ForceCloseInLoop
                                                    RetrieveAll()   丢用户态缓冲
                                                    DisableWriting()
                                                    HandleClose()   置死 + DisableAll
                                                      -> close_callback_
                                                           -> RemoveConnection
                                                                -> QueueInLoop(erase)
  读到一批合法帧（未知 id 丢弃）+ 半截帧（Decode 走 kNeedMore）
  erase 执行 -> 对象析构 -> ~Socket -> close(fd) -> 内核发 FIN
  read 返回 0 -> HandleRead -> HandleClose:213
        :222 SetState(kDisconnected)        <- 先置死
        :225 connection_callback_(guard)    <- 后回调
              -> RpcChannel::OnConnection:378   此时 Connected() 已为 false
                   connection_.reset()          释放 RpcChannel 那份引用
                   :383 connection_state_ = kDisconnected
                   :384 connection_error_ = "RPC connection closed"
                   :385 FailAllPending(...)
                         -> 逐个 CompleteCallWithFailure
                              -> QueueInLoop(done->Run())    <- 晚一拍
  controller.Failed() == true, ErrorText() == "RPC connection closed"
```

三个容易混淆的点：

- **服务端发数据的触发点是「收到请求」，不是「连接建立」。** `message_callback` 只在有数据到达时触发 —— 建连到 200ms 之间服务端静默。这一点是 Case 4 确定性的来源：请求到达与灌爆发生在**同一个函数里前后脚**，不靠时间赌。
- **`HandleClose` 的顺序不能反**（`:222` 在 `:225` 之前）：先 `SetState(kDisconnected)`、再调 `connection_callback_`。反过来的话 `OnConnection` 里 `conn->Connected()` 会是 true，走成 `:356-376` 的「新连接」分支 —— 重新赋 `connection_`、重放 `pending_writes_`，整条链跑偏。
- 服务端这边**不是 `ShutdownWrite`**。效果上确实是「写方向结束」（最终会发 FIN），但机制是 `ForceCloseInLoop`：丢缓冲 + 摘事件 + 置死，**连 fd 都不关**，fd 要等 `RemoveConnection` 的 erase lambda 跑完 → 对象析构 → `~Socket` 才 `close()`。

**Case 4 的一条硬约束（踩过才知道）：服务端灌进去的填充数据必须是【合法 RPC 帧】，不能塞裸字节。**

```
塞裸字节 'x'（0x78）：首 4 字节 = 0x78787878 = 2021161080 > kMaxFrameSize(16MB)
  └─ rpc_codec.cpp:61-66        kError("invalid RPC frame size")
       └─ rpc_channel.cpp:404-411
            connection_error_ = "RPC protocol error: " + error     ← 另一个 reason
            FailAllPending(...)                                    ← 在途调用被【错误的 reason】失败
              → 断言 ErrorText() == "RPC connection closed" 挂
              （或客户端自己 Shutdown，连接在服务端超限之前就死了）
```

正确做法：用 `RpcCodec::Encode` 造一个合法帧（`RESPONSE` + 一个绝不撞车的 `request_id`），连发 512 个。客户端能正常解码，未知 `request_id` 的帧被**直接丢弃、零副作用**（`rpc_channel.cpp:481-485`）。

**`request_id` 的选择是关键**：必须是一个绝不会等于在途调用 id 的值。若撞上，`CompleteCallWithFrame` 会**在连接被踢之前**就把那次调用成功完成，`Failed()` 变成 false、`done_count` 提前到 1 —— 断言同样失真。

### 遗留项（诚实记账）

**已闭环**：~~跨层闭环未测~~ → **Case 4 已覆盖并通过**。补齐时顺带**纠正了一次我的错误判断**：连接的断开路径不是 `FailAllPendingNow`（那只在 `RpcChannel::~RpcChannel` 里，`rpc_channel.cpp:141`，reason `"RPC channel closed"`），而是 `OnConnection` 的断开分支 —— 真实链路是：

```
服务端 SendInLoop:287 超限
  └─ ForceCloseInLoop:314      RetrieveAll + DisableWriting + HandleClose
       └─ HandleClose:213      置死 + DisableAll + 两个回调   （不关 fd）
            └─ close_callback_  → TcpServer::RemoveConnection（tcp_server.cpp:69-72）
                 └─ connections_.erase + QueueInLoop(RemoveConnectionInLoop)
                      └─ ConnectDestroyed:134 → channel_->Remove()
                           └─ 最后一个持有者释放 → ~TcpConnection → ~Socket → ::close(fd) → FIN

客户端侧（收到 FIN 之后）
  read 返回 0 → HandleRead:168 → HandleClose:170
     └─ connection_callback_(guard)       ← 踢连接时 Connected() 已为 false
          └─ RpcChannel::OnConnection:378  （rpc_channel.cpp）
               connection_error_ = "RPC connection closed"    :384   ← 不是 "RPC channel closed"
               FailAllPending(connection_error_)              :385
                    └─ CompleteCallWithFailure:511 → QueueInLoop:537
                         └─ SetCallState + CompleteFailure(done) → done->Run()
```

业务侧要区分的三个 reason：**`"RPC connection closed"`**（连接没了 → 换连接重试是合理语义）、`"too many pending rpcs"`（被自己的在途闸门限流拒了 → 立刻重试只是加压，应退避）、`"RPC protocol error: ..."`（解不出合法帧 → 重试无用，先查协议/版本）。Case 4 断言的是第一个。

剩余四条，**按性质分类**（不要笼统都叫「没测」，混在一起会让人误以为机制有洞）：

1. **`max_pending_writes_` 闸门没有测试 —— 测试无法稳定控制窗口。** 原归因是「未建连的时间窗口太窄」—— **这个说法不完整**。源码里有两处相关事实：`rpc_channel.h:135` 的 `connection_state_` 初值就是 `kConnecting`，而 `CallMethod` 的投递在 loop 线程内是内联的 —— 所以**构造 channel 之后同步发请求，其实稳定落在窗口内**；窗口真正关闭的条件是 `connector.cpp:115` 的 `::connect` 同步返回 0（当场建连）+ 跑过一轮 loop。也就是说，测不了的原因不是「窄」，而是**窗口何时关闭由 Connector 的内部行为决定，测试无法控制**。要稳定覆盖，需要一个可注入延迟的 Connector 替身；没有它就只能靠时间赌，测试会 flaky —— 宁可不写，也不写一个偶尔红的测试。
2. **输入水位与单帧上限的边界未测 —— 参数问题，不是机制缺口。** §3.6 说水位值必须显著大于单帧上限，否则合法大帧会被误杀 —— 但 Case 3 用的是 64KB 水位 + 64KB 垃圾，只证了「机制工作」，没证「配置边界安全」。
3. **heaptrack 内存曲线没跑 —— 唯一一条「机制在、只是数据没测」。** 草案验收第 1 条因此只完成一半。
4. **慢客户端压测的 P99 / 错误率未测 —— 属第 12 节（Benchmark）的工作**，不在本节验收范围内。

### Case 4 的设计（当日补充）

要造出「连接被踢时仍有在途调用」的场面，需要两件事同时成立：调用在途、连接被踢。做法是让**服务端的 `message_callback` 自己动手**：

```
客户端 200ms 发一个 Echo 请求
   └─ 服务端 message_callback：
        buffer->RetrieveAll()     吃掉请求、不回包 → 客户端的调用留在 pending_calls_
        if (!conn->Connected()) return;
        for (512 次) conn->Send(64KB)   顶到硬上限 → ForceCloseInLoop
```

时序上完全确定（请求到达 → 踢连接），不依赖任何时间赌。断言四条：服务端 `server_requests >= 1`、`overload_closes >= 1`、客户端 `done_count == 1`（exactly-once）、`Failed() && ErrorText() == "RPC connection closed"`。

一个安全前提（核过源码）：`TcpConnection::Send` 在 loop 线程内**直接内联** `SendInLoop`（`tcp_connection.cpp:96-100`），不走 `RunInLoop`、不捕获 `this`；而 `HandleRead:157` 调用 `message_callback_` 时传的 `shared_from_this()` 临时对象 + `Channel::Tie` 的 guard（`tcp_connection.cpp:126`）全程持有连接，所以「连发 512 次、中途被 `ForceClose`」不会 use-after-free。

---

## 6. 与下一节的关系

Roadmap 第 9 节有个隐含前置：**Multi-Reactor 与跨线程所有权**（客户端侧多 loop、1/2/4/8 扩展性）。

本节**没有碰它** —— 本节只做「资源上限」，所有闸门都在单 loop 语义内即可验证。跨线程部分（`EventLoopThreadPool` 已在 `nebula/net/event_loop_thread_pool.h:15`、`tcp_server.h` 已持有）留作独立一块推进。
