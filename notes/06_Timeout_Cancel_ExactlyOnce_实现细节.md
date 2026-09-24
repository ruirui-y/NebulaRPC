# Phase 7 实现细节：Timeout / Cancel / Race / Exactly-Once Completion

> 本文是 `00_Roadmap.md` 第 7 节的落地展开，只讲这一阶段具体做什么、怎么做、怎么验收。
>
> **当前状态（2026-09-24 已落地）**：本文三个任务全部完成 —— `RpcCall` 状态机已接线、
> 超时测试已修复并真实触发超时路径、Cancel 已在 `RpcChannel` 接线并反注册。
> Linux Debug 构建 13/13 通过，0 error / 0 warning。
> **未完成**：ASan / UBSan preset 验收未跑（第 8 节总清单最后两项）；第 7 节的提交未做。
> 另有一批超出本文计划的修复记录在第 10 节。

---

# 1. 概念澄清

## 1.1 Exactly-Once Completion 是什么

一次 RPC 有四个完成来源：

```
Response     响应到达
Timeout      超时定时器触发
Cancel       业务主动取消
Disconnect   连接断开 / 协议错误
```

它们可能先后到达，甚至"看起来同时"到达。Exactly-Once 的要求是：

```
无论多少个完成来源到达，done->Run() 恰好执行一次
```

违反的后果只有两种，都是事故：

- **执行两次**：第一次完成后业务往往已经释放/复用 response、controller，
  第二次回调就是 use-after-free 或业务逻辑错乱；
- **执行零次**：pending 永远留在 map 里，内存泄漏，业务永远等不到结果。

## 1.2 为什么当前"单线程摘除"没有问题

当前实现靠 `TakePendingCall(request_id)`：谁先把 PendingCall 从
`pending_calls_` 里摘走，谁负责完成；后来的来源找不到 request_id，直接丢弃。

这个方案现在正确，原因是**线程模型**，不是 map 本身安全：

```
response 事件   -> EPOLLIN      -> 属主 EventLoop 线程
timeout 事件    -> timerfd      -> 同一个 EventLoop 线程
disconnect 事件 -> 连接回调      -> 同一个 EventLoop 线程
```

三个来源在同一线程串行排队执行，不存在真正的并发，
"摘除"这个非原子操作因此天然原子。

结论：**当前阶段单线程摘除完全够用，不需要改。**
问题只在于——这个保证是"隐式的"，依赖"所有完成来源都在同一线程"这条假设。

## 1.3 CAS 状态机是什么

CAS = Compare-And-Swap，对应 CPU 的 `CMPXCHG` 原子指令。
`rpc_call.h` 里已经写好的实现：

```cpp
// 只有"当前还是 Pending"的第一次尝试能成功，之后所有尝试都失败
bool RpcCall::TryComplete(RpcCallState state) noexcept
{
    RpcCallState expected = RpcCallState::Pending;

    return state_.compare_exchange_strong(
        expected,
        state,
        std::memory_order_acq_rel,
        std::memory_order_acquire);
}
```

语义：

```
线程 A: TryComplete(Timeout)    -> 状态 Pending == Pending，改成 Timeout，返回 true
线程 B: TryComplete(Completed)  -> 状态已是 Timeout != Pending，返回 false，放弃
```

它什么时候才**必须**出现：

- `StartCancel` 按 protobuf 约定可以在**任意业务线程**调用，
  是第一个不经过 EventLoop 的完成来源。一旦取消的完成动作不投递回
  loop 线程，就是两个线程同时抢完成权，map 摘除不再安全，只有 CAS 能仲裁。

它什么时候是"保险"：

- 即使 Cancel 选择投递回 loop 线程（见 4.1 的决策），`TryComplete` 作为
  统一闸门也有价值：所有完成路径变成"先仲裁、后完成"的显式结构，
  不再依赖"大家都记得在同一线程"这条口头约定；
  后续 Coroutine / CompletionExecutor 改变完成线程时，骨架不用动。

## 1.4 两套机制的关系

```
TryComplete     = 状态仲裁：谁先改状态，谁获得完成权（跨线程安全）
TakePendingCall = 资源清理：摘 pending、取消定时器、摘写排队（只在 loop 线程）
```

接线后不是二选一，而是串联：

```
完成来源到达
    |
    v
TryComplete(对应状态)
    |
    +-- false -> 别人已完成，直接丢弃
    |
    +-- true  -> TakePendingCall 清理资源 -> 写结果 -> done->Run()
```

---

# 2. 现状盘点

| 项 | 状态 | 位置 |
|---|---|---|
| TimerQueue / RunAt / CancelTimer | 完成 | `nebula/net/timer_queue.cpp` |
| Timeout 链路（SetTimeout -> RunAt -> OnTimeout） | 完成 | `rpc_channel.cpp` |
| response 先到取消定时器 / 迟到响应丢弃 | 完成 | `TakePendingCall` / `CompleteCallWithFrame` |
| `RpcCall` + `TryComplete` | **完成** | `rpc_call.h/.cpp`，已进 CMake 并接入四条完成路径 |
| `RpcCallContext` | **零引用，待删** | `rpc_call_context.h`，与 `PendingCall` 字段重复 |
| `RpcController` cancel 四件套 | **完成** | `StartCancel / RegisterOnCancel / RemoveOnCancel / IsCanceled` |
| Channel 侧 Cancel 接线 | **完成** | `RegisterAndSend` 已注册 cancel 回调 |
| cancel 回调反注册 | **完成** | `TakePendingCall` 内 `RemoveOnCancel`，四条完成路径唯一收口 |
| 超时测试 | **已修复** | `rpc_timeout_test.cpp`，见任务 2 |
| 竞争测试 Case 3 | **完成** | `tests/rpc_cancel_test.cpp`，1000 轮 |
| 状态机单测 | **完成** | `tests/rpc_call_test.cpp` |

---

# 3. 任务 1：接线 RpcCall 状态机（已完成）

## 3.1 涉及文件

```
nebula/rpc/CMakeLists.txt        源列表加 rpc_call.cpp
nebula/rpc/rpc_channel.h         PendingCall 内嵌 RpcCall
nebula/rpc/rpc_channel.cpp       四条完成路径加 TryComplete 闸门
nebula/rpc/rpc_call_context.h    删除（字段与 PendingCall 完全重复，保留会造成两套并行结构）
```

## 3.2 步骤

第一步：`nebula_rpc` 源文件列表加入 `rpc_call.cpp`。

第二步：`PendingCall` 内嵌状态机，删除 `rpc_call_context.h`：

```cpp
struct PendingCall
{
    google::protobuf::Message* response{};              // 响应写入目标
    google::protobuf::RpcController* controller{};      // 错误写入目标
    google::protobuf::Closure* done{};                  // 完成回调
    std::optional<TimePoint> deadline;                  // 超时点
    net::TimerId timeout_timer;                         // 超时定时器
    RpcCall call;                                       // 状态机：恰好一次仲裁
};
```

第三步：`CompleteCallWithFailure` 增加状态参数，区分超时与断连：

```cpp
// reason 之外带上目标状态，供状态机记录最终死因
void CompleteCallWithFailure(std::uint64_t request_id,
                             const std::string& reason,
                             RpcCallState state);
```

调用点对应修改：

```
OnTimeout        -> RpcCallState::Timeout
OnConnection 断连 -> RpcCallState::Failed
协议错误          -> RpcCallState::Failed
析构 FailAllPendingNow -> RpcCallState::Failed
```

第四步：四条完成路径统一改为"先仲裁、后清理"。以响应路径为例：

```cpp
// 响应到达：先过状态机闸门，再做资源清理和回调
void RpcChannel::CompleteCallWithFrame(std::uint64_t request_id, RpcFrame frame)
{
    // ---- 第一步：找到在途调用，仲裁完成权 ----
    auto it = pending_calls_.find(request_id);
    if (it == pending_calls_.end())
    {
        return;                                         // 已被其他来源完成
    }

    if (!it->second.call.TryComplete(RpcCallState::Completed))
    {
        return;                                         // timeout/cancel 抢先
    }

    // ---- 第二步：仲裁成功，摘除并清理资源 ----
    auto pending_call = TakePendingCall(request_id);
    if (!pending_call.has_value())
    {
        return;
    }

    // ---- 第三步：写结果并执行回调 ----
    loop_->RunInLoop([response = pending_call->response,
                        controller = pending_call->controller,
                        done = pending_call->done,
                        frame = std::move(frame)]() mutable
        {
            CompleteFrame(response, controller, done, std::move(frame));
        });
}
```

其余三条路径（timeout / disconnect / cancel）同构：
`find -> TryComplete(对应状态) -> TakePendingCall -> 回调`。

第五步：`rpc_call.cpp` 进 CMake、接线完成（提交见第 7 节）。

## 3.3 验收（已满足）

- ✅ 编译通过，`rpc_call.cpp` 真正进入 `nebula_rpc` 库；
- ✅ 现有 `rpc_echo_client`、`rpc_timeout_test` 行为不变（接线不改变单线程语义）；
- ✅ 状态机单测：并发 N 个线程对同一个 `RpcCall` 调 `TryComplete` 不同状态，
  断言恰好一个返回 true（`tests/rpc_call_test.cpp`）。

---

# 4. 任务 2：修复超时测试（Case 1 / Case 2）（已完成）

## 4.1 修复前测试的两个错误（历史记录）

`tests/rpc_timeout_test.cpp` 目前恒过，但什么都没测到：

错误一：服务端 `Echo` 计算了 `delay`（slow = 250ms）但**从未使用**，
响应用的是 `loop_->RunInLoop`（立即执行）。超时路径从未触发。

错误二：断言方向写反了。测试给 slow 设置 100ms 超时，却断言
`!slow->controller.Failed() && text == "slow"`——这是"超时没发生"的断言。
超时真正生效后，slow 必须是 `Failed() == true`。

## 4.2 修法

服务端：真正延迟回包。

```cpp
// 用 RunAfter 代替 RunInLoop，让 slow 请求在 250ms 后才响应
loop_->RunAfter(delay, [this, text, response, done]
    {
        response->set_text(text);
        response->set_server_sequence(next_sequence_.fetch_add(1));

        if (done != nullptr)
        {
            done->Run();
        }
    });
```

客户端断言改为：

```
fast（timeout 500ms，服务端 10ms 回包）：
    done_count == 1
    Failed() == false
    response.text() == "fast"

slow（timeout 100ms，服务端 250ms 回包）：
    done_count == 1                       // 恰好一次，不能是 0 也不能是 2
    Failed() == true
    ErrorText() == "RPC timeout"
```

迟到响应验证（Case 2 的后半段）：

```
slow 超时完成后，250ms 时服务端响应才到达。
等 5s 总判定时刻再检查：
    slow_done_count 仍然 == 1             // 迟到响应没有触发第二次回调
    service.SlowTimerFiredCount() == 1    // 服务端确实回包了，只是被客户端丢弃
```

这两个断言同时成立，才证明"超时先完成 + 迟到响应被丢弃"。

## 4.3 验收（已满足）

- ✅ 修复前已确认测试是"恒过"的（`delay` 未使用 → 超时路径根本没跑起来）；
- ✅ 修复后测试通过，且 `SlowTimerFiredCount() == 1` 证明迟到响应真实到达、被客户端丢弃；
- ⬜ ASan preset 下通过（**待跑**）。

---

# 5. 任务 3：接线 Cancel（已完成）

## 5.1 完成线程模型决策

`StartCancel` 可从任意业务线程调用，完成动作有两个选择：

```
方案 A：cancel 回调 RunInLoop 投递回属主 loop 线程完成
方案 B：cancel 直接在业务线程完成（需要给 pending_calls_ 加锁）
```

选择方案 A。

> 技术决策说明：为什么 Cancel 走方案 A
>
> 为什么是这个方案：
>   方案 A 保持 owner-thread 模型，`pending_calls_` 继续无锁，
>   与 response/timeout 路径完全同构；代价是取消生效多一次事件循环投递，
>   对取消这种低频操作可以忽略。方案 B 的即时性收益很小，
>   却要引入 map 加锁和"锁内执行回调"的死锁风险。
>
> 底层是什么：
>   `RunInLoop` 经 eventfd 唤醒目标线程，任务在下一轮循环执行，
>   延迟通常微秒级。
>
> 如果是你你会怎么学：
>   对比 brpc 的 bthread 内完成模型——它能方案 B 是因为有用户态线程池兜底，
>   NebulaRPC 没有这层设施，先保持单线程模型是正确取舍。

`TryComplete` 在方案 A 下暂时是"保险闸门"（见 1.3），
接线是为了让 Cancel 语义显式化，并为将来切换方案 B / Coroutine 留好骨架。

## 5.2 RpcController 增加反注册

问题：channel 注册进 controller 的 cancel 回调捕获了 channel 的 `this`。
controller 归业务所有、可能比 channel 活得久，若业务之后调 `StartCancel`，
回调里的 `this` 已悬空。所以**每次完成都必须把回调摘掉**。

`NotifyOnCancel` 是 protobuf 虚函数签名，不动它；新增带 token 的一对方法：

```cpp
int RegisterOnCancel(google::protobuf::Closure* callback);    // 返回 token，-1 表示已取消立即执行
void RemoveOnCancel(int token);                               // 按 token 摘除
```

实现要点（都在已有 `mutex_` 保护下）：

```
cancel_callbacks_ 从 vector<Closure*> 改为 vector<pair<int, Closure*>>
RegisterOnCancel：
    已取消 -> 立即 callback->Run()，返回 -1
    未取消 -> 分配 token 存入列表
RemoveOnCancel：
    按 token 找到并从列表移除，移除方负责 delete 该 closure
```

所有权规则（避免 double free）：

```
closure 只有一个持有者：
    要么在 controller 列表里（未触发）
    要么被 StartCancel swap 出去正在执行（执行完 RpcClosure::Run 自删除）
    要么被 RemoveOnCancel 摘走（摘除方 delete）
StartCancel 先整体 swap 再执行，RemoveOnCancel 与它互斥加锁，
因此同一个 closure 不可能同时落入两方手里。
```

## 5.3 RpcChannel 接线

第一步：`RegisterAndSend` 注册 pending 后登记取消回调：

```cpp
// 业务线程随时可能 StartCancel -> 投递回 loop 线程走取消完成路径
auto* rpc_controller = dynamic_cast<RpcController*>(it->second.controller);
if (rpc_controller != nullptr)
{
    it->second.cancel_token = rpc_controller->RegisterOnCancel(
        new RpcClosure([this, request_id]
            {
                loop_->RunInLoop([this, request_id]
                    {
                        CompleteCallWithCancel(request_id);
                    });
            }));
}
```

`PendingCall` 增加 `int cancel_token{-1};`。

第二步：新增取消完成路径，与其他三条同构：

```cpp
// 取消完成：状态机仲裁 -> 清理 -> 回调
void RpcChannel::CompleteCallWithCancel(std::uint64_t request_id)
{
    auto it = pending_calls_.find(request_id);
    if (it == pending_calls_.end())
    {
        return;
    }

    if (!it->second.call.TryComplete(RpcCallState::Cancelled))
    {
        return;                                         // response/timeout 抢先
    }

    auto pending_call = TakePendingCall(request_id);
    if (!pending_call.has_value())
    {
        return;
    }

    // 取消不算失败：不 SetFailed，业务用 controller->IsCanceled() 判断
    loop_->QueueInLoop([done = pending_call->done]
        {
            if (done != nullptr)
            {
                done->Run();
            }
        });
}
```

第三步：`TakePendingCall` 集中反注册（四条完成路径都经过它，一处收口）：

```cpp
// 摘除 pending 的同时把 cancel 回调从 controller 上摘掉，防止悬空
if (pending_call.cancel_token >= 0)
{
    auto* rpc_controller = dynamic_cast<RpcController*>(pending_call.controller);
    if (rpc_controller != nullptr)
    {
        rpc_controller->RemoveOnCancel(pending_call.cancel_token);
    }
}
```

注意一个边界：取消路径自己就是被 cancel 回调触发的，
此时回调已被 `StartCancel` swap 走自删除，`RemoveOnCancel` 找不到 token
是正常情况（列表里已没有它），实现要容忍"摘不到"。

## 5.4 验收

- 发起 RPC 后立即 `controller.StartCancel()`：done 恰好一次，
  `IsCanceled() == true`，`Failed() == false`。该条是**确定性**的：
  `StartCancel` 是 `CallMethod` 返回后的下一条同步语句，而 response
  至少要一个网络往返，取消必然抢到完成权；
- 响应已完成后才 `StartCancel()`：无任何效果，不崩溃，
  `IsCanceled()` 保持 `false`（取消输给了 response）；
- channel 先析构、业务后调 `StartCancel()`：不崩溃（回调已反注册）；
- Case 3 竞测里 `IsCanceled()` 与每轮赢家一致（见 6.1）；
- ASan 下全部通过。

## 5.5 接线之外的四处加固

接线过程中发现四个与 Cancel 生命周期相关的缺陷，一并修掉（均超出原计划）：

### ① `IsCanceled()` 改为终态语义

原实现 `canceled_` 一个 bool 身兼两职：既作 `IsCanceled()` 的返回值，
又作 `RegisterOnCancel` 判定「已受理 → 内联执行」的互锁。取消请求
一受理就置 true，于是**输给 response 的迟到取消也会让 `IsCanceled()`
返回 true**，与「response 已被填充」互相矛盾 —— 1.4 那条
「要么 IsCanceled，要么拿到 response」的验收根本不成立。

⚠️ 不能只把置位点往后挪：`StartCancel` 已经 swap 走列表、标志还没
置位的那个窗口里注册进来的回调，会永远留在列表里没人执行 ——
取消被整个吞掉。所以拆成两个标志：

```cpp
bool cancel_requested_{false};   // 取消已受理：StartCancel 入口置位，兼作注册互锁
bool canceled_{false};           // 取消已生效：抢到完成权后置位，IsCanceled() 读它
void MarkCanceled();             // 仅由取消完成路径调用
```

`MarkCanceled()` 落在 CAS **之后**（`CompleteCallWithCancel` 内）、
`QueueInLoop(done)` **之前**，不是「回调跑完」之后 —— 取消回调跑完
≠ 取消赢了，输给 response 时回调照样跑，落在那里仍会把终态置错。
放在 CAS 之后还有个连带好处：主线程在 done 之后读 `IsCanceled()`
必然读到终值（由 `mutex_` + done 的 cv 建立 happens-before）。

### ② `~RpcController()` 释放残留回调

`cancel_callbacks_` 是裸指针 vector，而 `~RpcController()` 原是
`= default` → 不 delete。正常路径每次 `TakePendingCall` 都摘干净、
vector 恒为空，所以不漏；一旦哪条路径漏摘且 controller 析构
（controller 常是短命的调用方局部对象），那块 `RpcClosure` 就永久
留在堆上。而闭包只在别人调 `StartCancel()` 时才会被 Run 自删除 ——
请求正常完成对它而言根本不是事件。

新增 `ReleaseCancelCallbacks()`，析构与 `Reset()` 共用同一份释放逻辑
（锁外 delete，避免闭包析构回调业务代码时持有 `mutex_`）。

### ③ 超时定时器不再捕裸 `this`

定时器活在 `EventLoop` 的 TimerQueue 里，可能比 channel 长寿；
`RunAt` 登记的 lambda 捕获裸 `this`，channel 析构后到点触发即 UAF。
改为捕获 `weak_guard`，执行时先 `Acquire()` 校验。

判据：**定时器是「一定会响的闹钟」，取消回调是「只有人按才响的门铃」**。
前者必然触发 + 捕裸指针 → 四项清理里唯一必然 UAF 的一项。

### ④ 取消回调守卫提前 + `AliveGuard` 改 atomic

原结构的顺序是「先解引用裸 `loop`，再 `lock` 校验 channel」——
守卫站得比它该站的位置靠里，`loop->RunInLoop(...)` 是全流程唯一
没有任何守卫兜底的裸解引用。改为投递前先校验 channel 存活。

`loop` 仍然值捕获、**不改成 `channel->loop_`**：外层只需要「channel
还在吗」这个门禁，读 channel 成员反而多一次 TOCTOU。有效性是**传递**
过来的 —— `~RpcChannel()` 首行 `assert(loop_->IsInLoopThread())`
意味着「channel 活着 ⇒ loop 必须活着」，守卫确认 channel 存活
就足够给 `loop` 背书。

⚠️ 这一改带来一个必须先解决的前提：守卫字段的读，从「只在 loop 线程」
变成「业务线程读、loop 线程写」，裸指针不再成立。因此
`AliveGuard::channel` 改为 `std::atomic<RpcChannel*>`，统一经
`Acquire()`（acquire 语义）读，构造/析构用 release 写 ——
把「必须按 acquire 读」固化在类型上，而不是每处手写 `load(...)`。

**诚实边界**：`Acquire()` 返回非空到真正解引用之间仍有 TOCTOU 窗口。
AliveGuard 是「降低概率」的弱保证，不是强一致 —— 它没有把 `loop`
本身变成受守卫对象，只是让 `loop` 的解引用由 channel 的存活背书。
要彻底消除需改用 `shared_ptr` 持有 channel 本体或引入引用计数握手，
那是另一个量级的改动。

---

# 6. 任务 4：竞争测试（Case 3）（已完成）

Case 1（response 先到）/ Case 2（timeout 先到）由任务 2 覆盖。
Case 3 是真正的并发竞争，依赖任务 3 的 Cancel。

测试结构（`tests/rpc_cancel_test.cpp`）：

```
服务端 ImmediateEchoService：收到请求立即回包，让响应尽快到达
warmup 一轮：先等连接建立，避免把连接竞争混入取消竞争

主循环 1000 轮：
    主线程发起 RPC（不设超时）
    紧接着 controller.StartCancel()   ← 可能早于注册、早于响应、晚于响应

    断言：
        done_count == 1（无论谁赢，恰好一次）
        终态自洽：要么拿到 response，要么 response 为空（取消赢），二者不并存
        IsCanceled() 与赢家一致（见 6.1）
        任何一轮 Failed() 为真都算路径错误
```

它同时是状态机接线（任务 1）正确性的最终裁判：如果 `TryComplete`
没接好，这里会出现 done 执行两次或零次。

## 6.1 终态自洽断言（6.2 之外的补充）

初版只按 `response.text()` 空/非空判赢家，**完全没用 `IsCanceled()`**
—— 而 5.5 ① 恰好改了它的语义。语义改了却零覆盖，等于没验。补上：

```cpp
// IsCanceled() 是终态：取消赢 ⟺ true；MarkCanceled 排在 done 之前，读到的是终值
if (context->controller.IsCanceled() != got_cancel)
{
    std::cerr << "round " << round << ": IsCanceled="
              << context->controller.IsCanceled()
              << " but cancel_won=" << got_cancel << "\n";
    return 1;
}
```

读到终值是安全的：`MarkCanceled()` 在 loop 线程、`QueueInLoop(done)`
之前执行，主线程在 done 之后才读 → 由 controller 的 `mutex_` 与
done 的 cv 建立 happens-before。

⚠️ 别把这条误当作「取消必然赢」：这 1000 轮里 response 赢的轮次
`IsCanceled()` 必须为 **false**，两侧都要成立才叫终态语义。

---

# 7. 任务 5：提交与笔记（待执行）

按 scope 分四个提交（不混合）：

```
提交 1：feat(rpc): 接入 RpcCall 状态机
    - 新增 nebula/rpc/rpc_call.h / rpc_call.cpp，进 CMake
    - rpc_channel.h 的 PendingCall 内嵌 RpcCall
    - 四条完成路径全部先 TryComplete 后回调
    - 新增 tests/rpc_call_test.cpp

提交 2：fix(test): 超时测试真正覆盖超时路径
    - 服务端改用 RunAfter(delay) 再回包
    - slow 断言改为失败完成 + 迟到响应丢弃验证

提交 3：feat(rpc): Cancel 支持
    - RpcController 增加 RegisterOnCancel / RemoveOnCancel
    - RpcChannel 取消完成路径 + TakePendingCall 集中反注册
    - Case 3 竞争测试（含 IsCanceled 终态断言）
    - 5.5 的四处加固（终态语义 / 析构释放 / 定时器守卫 / 守卫提前）

提交 4：docs(notes): 笔记对齐代码现状
    - 00_Roadmap.md 第 7 节、05、06 三份文档
    - notes/README.md 索引

（chore 提交：.gitignore 忽略 .workbuddy/ 与 *.zip）
```

⚠️ `nebula/rpc/rpc_call_context.h` 是**死文件**（全仓零引用，仅自身定义），
计划里写着删除 —— 目前保留在工作区未入库，删或留待定。

完成后更新 `notes/README.md` 索引，并把本文标记为已实现。

---

# 8. 总验收清单

```
[x] rpc_call.cpp 进入构建
[x] 四条完成路径全部先 TryComplete 后回调
[x] 超时测试修复：RunAfter 真延迟 + slow 断言，超时路径真实触发
[x] Case 1/2/3 全部实现，done 计数恒为 1；Case 3 含 IsCanceled 终态断言
[x] Cancel 三个边界（先取消 / 后取消 / channel 先死）全部安全
[ ] ASan + UBSan preset 全绿（未跑，属提交前最后一道）
[ ] 四个提交入库，笔记索引更新
```

诊断观察与最终结论要分开记：上面 `[x]` 表示**代码已实现且经 Debug 构建通过**，
不等于 sanitizer 下的最终通过。

---

# 9. 面试要点（完成后回顾）

## 9.1 Exactly-Once 与 CAS

- 什么是 Exactly-Once Completion？为什么 RPC 必须有它？
- "单线程摘除"为什么等价于 Exactly-Once？它的前提是什么？
- 哪个完成来源会打破这个前提？（Cancel，任意线程可调）
- CAS 如何保证恰好一次？`compare_exchange_strong` 的内存序为什么选 acq_rel？
- ★ **exactly-once 是双保险**：CAS 管「完成权」（跨线程），`erase` 管
  「资源寿命」（loop 线程）。只删掉 `erase` 时，迟到响应也会被
  `TryComplete` 挡住（`find` 成功但 CAS 失败）→ 不会二次 `done->Run()`。
  **毁掉的是资源，不是恰好一次。**

## 9.2 cancel 回调的所有权

- cancel 回调的所有权规则：谁持有、谁删除、为什么不会 double free？
- 为什么取消完成选择投递回 loop 线程而不是原地完成？
- ★ `PendingCall` 里没有 request、也不「持有」controller / response ——
  三个都是裸指针（借用）。request 连借都不借：序列化成 bytes 后原对象
  再未被引用。所以「清理这次请求」= `erase` 掉一个 optional + TimerId +
  int + atomic，**controller / response 一个字节都没动，也不该动**
  （它们必须活过 `done->Run()`）。
- ★ 四项清理对应**四类不同故障**，不能合并成「反正有 weak_ptr」：

```
清理项        不做的后果                                    持有者
─────────────────────────────────────────────────────────────────────
① map 条目    FailAllPendingNow 的 while(!empty()) 死循环   channel 自己
② 定时器      channel 析构后到点触发 → UAF                  EventLoop 队列
③ 写队列      残留字节（有 contains 兜，最轻）               channel 的 deque
④ 取消回调    泄漏 + 白跑（不崩）                          controller 的 vector
```

- ★ **判「漏摘某项」的后果只看两件事**：残留物自己的触发条件 +
  它捕获的指针有没有守卫。定时器两样都最差（**一定会响的闹钟** + 捕裸
  `this`）→ 四项里唯一必然 UAF；取消回调两样都最好（**只有人按才响的
  门铃** + weak_ptr）→ 降级成泄漏/白跑。**这个不对称就是「不能拿
  weak_ptr 一句话带过四项清理」的答案。**

## 9.3 本轮修出来的四个坑（都是真实缺陷）

- ★ **`IsCanceled()` 的语义选择**：一个 bool 身兼「取消已受理」和
  「取消已生效」两职时，`IsCanceled()` 报的是哪一个？为什么不能简单
  把置位点后移？（会吞掉窗口内注册的回调）→ 必须拆成
  `cancel_requested_`（互锁）+ `canceled_`（终态）。
- ★ **对象无法报告自己已死**：存活标记若长在对象自己身上，对象一死
  标记跟着消失，读它就是 UAF。这正是 `AliveGuard` 必须**独立分配 +
  `shared_ptr` 持有**、而不是做成 `RpcChannel` 成员的原因。
- ★ **守卫的「位置」比守卫的「有无」更重要**：`lock()` 排在
  `loop->RunInLoop()` 之后 = 守卫站得比它该站的位置靠里。凡是
  「执行时机不受调用方控制」的路径（取消回调任意线程触发、定时器
  到点必响），守卫都必须站在第一次指针解引用**之前**。
- ★ **把不变式固化在类型上**：守卫被提到最外层后，字段的读跨了线程
  → 裸指针不再成立。改用 `std::atomic<T*>` + 具名 `Acquire()`，
  比在 N 处手写 `load(std::memory_order_acquire)` 更不容易被后人写坏。
- ★ **诚实边界**：`Acquire()` 非空到真正解引用之间仍有 TOCTOU 窗口。
  AliveGuard 是「降低概率」的弱保证，不是强一致 —— 别把它说成
  「彻底解决」。
- **连带坑（编译器帮你抓）**：嵌套 lambda 各自 `lock` 同一个
  `weak_ptr` 做存活校验时，内外同名变量在 `-Wshadow` 下必报。
  命名分层：外层 `guard`（投递前门禁）/ 内层 `inner_guard`（线程内二次校验）。
- **同模式扩散排查**：全仓 grep `RunAt(` 只有一处使用点（超时定时器）；
  其余捕获 `this` 的 lambda（如 `CallMethod` 里的 `RunInLoop`）属
  「调用方主动发起」，由调用方保证对象存活，**不算同病，别乱改**。
