# NebulaRPC Coding & Architecture Rules

本文件只定义 NebulaRPC 的代码编写规范、架构约束和新增代码方向。
它不是学习笔记，不记录“如何阅读源码”“从哪里开始看”“知识点解释”等内容。

---

## 1. 总体原则

- 新增或修改的 `.h` / `.cpp` 一律照 §10.5 的 `timer_queue.h` / `timer_queue.cpp` 模板写：
  缩进、空行、include 分组、注释位置全部照抄，不要另起风格。
- 新代码优先保持简单、可验证、可组合，不为了“框架感”提前引入复杂抽象。
- 不基于未验证的性能假设修改架构。性能优化必须有可复现实验或 profiling 证据支撑。
- 网络层、RPC 层、协程层职责必须清晰，禁止把上层协议语义塞进底层网络组件。
- 同一对象的线程归属优先使用 EventLoop 串行化保证，不优先用 mutex 修补并发问题。
- 保留必要的兼容路径，但禁止 Legacy 风格继续扩散到新代码。

---

## 2. Coroutine-First 异步编程规范

从现在开始，NebulaRPC 的新异步代码统一采用 C++20 Coroutine。

### 2.1 新增异步函数

优先返回：

```cpp
nebula::coro::Task<T>
nebula::coro::Task<void>
```

需要 fire-and-forget 生命周期时才使用：

```cpp
nebula::coro::DetachedTask
```

禁止新写多层 completion callback / nested lambda 异步链。

### 2.2 新增 RPC Client

新代码优先使用：

```cpp
CoroutineStub
CoCall
RpcResult<T>
```

推荐形式：

```cpp
auto result = co_await stub.Call<Request, Response>(
    "Method",
    std::move(request));
```

禁止新代码继续直接构造：

```cpp
RpcClosure
```

禁止新增：

```cpp
CallMethod(..., done)
```

式业务 completion chain。

旧 tests / examples / legacy API 可以继续保留，不要求为了统一风格强制重写。

### 2.3 新增 RPC Service

新 Service 优先使用：

```cpp
CoroutineService
```

Handler 返回：

```cpp
Task<Response>
```

推荐形式：

```cpp
service.Register<Request, Response>(
    "Method",
    [](RpcServerContext& context,
       const Request& request) -> Task<Response>
    {
        Response response;
        co_return response;
    });
```

新业务 Service 禁止继续扩散：

```cpp
done->Run();
google::protobuf::Closure*
```

式异步业务流。

### 2.4 Timer / 延迟 / Yield

业务层等待统一优先使用：

```cpp
co_await SleepFor(...);
co_await Yield(...);
```

不要在新业务代码中继续构造：

```cpp
RunAfter(..., lambda)
```

形成异步状态机。

---

## 3. Callback 允许存在的边界

Callback 不要求从整个项目中彻底消失。

允许存在：

- epoll / Reactor 内部事件分发；
- Channel read/write/error callback；
- TcpConnection / TcpServer 等底层网络事件接口；
- protobuf legacy compatibility；
- 尚未迁移的旧 tests / examples；
- 与第三方库接口兼容所必需的 callback。

不允许在新业务代码中继续扩散：

- RpcClosure completion chain；
- nested lambda 异步流程；
- done->Run() 驱动业务状态机；
- callback 套 callback 的 RPC 编排。

原则：

> Reactor 内部可以 callback；业务异步流程统一 coroutine。

---

## 4. Coroutine 调度与线程归属

NebulaRPC 当前以 `EventLoop` 作为 coroutine 的主要执行上下文。

### 4.1 恢复线程

任何 awaiter 在挂起后恢复 coroutine 时，必须保证恢复到正确的 EventLoop。

不要直接在任意线程：

```cpp
handle.resume();
```

除非该 coroutine 明确允许在当前线程恢复。

### 4.2 跨线程操作

对象属于某个 EventLoop 时：

```text
当前线程 == 所属 EventLoop
    -> 直接执行

当前线程 != 所属 EventLoop
    -> QueueInLoop / RunInLoop 投递
```

优先使用线程归属解决同步问题，不因为调用可能跨线程就直接增加 mutex。

### 4.3 Exactly-Once Completion

RPC completion 必须保证：

```text
response
timeout
cancel
disconnect
```

最多只有一个路径能够完成调用并恢复 coroutine。

禁止出现：

- double resume；
- double completion；
- timeout 和 response 同时完成同一个 PendingCall；
- coroutine frame 已销毁后继续 resume。

---

## 5. Task 生命周期规范

`Task<T>` / `Task<void>` 必须满足：

- coroutine frame 所有权明确；
- 不允许悬空 continuation；
- 父 Task `co_await` 子 Task 时，子 Task 完成后恢复父 Task；
- Task 析构时不能留下仍会被异步路径访问的无效 handle；
- DetachedTask 必须有明确的自销毁规则；
- 不允许依赖未定义的隐式生命周期。

新代码如果保存 coroutine handle，必须明确：

```text
谁拥有
谁恢复
谁销毁
何时失效
```

---

## 6. RPC Client 结果模型

新 Coroutine RPC 不应只暴露 `Response`。

优先返回：

```cpp
RpcResult<Response>
```

结果对象可以承载：

- Response；
- RpcCallState；
- ErrorText；
- Cancelled；
- RequestId；
- TraceId；
- Latency；
- RemoteErrorCode。

业务代码应通过结果对象判断 RPC 是否成功，而不是依赖 callback 中零散状态。

---

## 7. TcpConnection Write / Flush 规范

发送模型固定为：

```text
Write
    -> 只追加到 output_buffer_

Flush
    -> 上层明确提交当前批次

FlushInLoop
    -> 尝试写 socket

partial write / EAGAIN
    -> EnableWriting

EPOLLOUT
    -> HandleWrite
    -> 继续排空
```

### 7.1 Write

`Write()`：

- 不代表立即发生 syscall；
- 不应该偷偷 Flush；
- 只负责把数据放入连接输出缓冲；
- 跨线程时投递回连接所属 EventLoop。

### 7.2 Flush

`Flush()`：

- 由上层决定 batch boundary；
- 不由 TcpConnection 猜测“什么时候这一批结束”；
- 可以尝试一次写出当前 output_buffer；
- 写不完时转入 EPOLLOUT 驱动。

### 7.3 HandleWrite

`HandleWrite()` 只负责：

> 已经 Flush 但没有完全写完的数据续写。

不要把业务 batch 策略放入 `HandleWrite()`。

---

## 8. RPC 层与 Transport 层职责

### TcpConnection / net

负责：

- socket；
- input/output buffer；
- Write / Flush；
- partial write；
- EAGAIN；
- EPOLLIN / EPOLLOUT；
- connection lifecycle。

不负责：

- RPC request / response 语义；
- protobuf method；
- request_id；
- RPC batch policy。

### RpcChannel

负责：

- Client RPC 生命周期；
- request_id；
- pending call；
- timeout；
- cancel；
- response completion；
- coroutine resume；
- RPC Client Flush 边界。

### RpcServer

负责：

- frame decode；
- service / method dispatch；
- server-side call lifecycle；
- coroutine service execution；
- response encode；
- server-side Flush 边界。

---

## 9. 新 tests / examples 的异步风格

已有旧 tests / examples 不要求为了风格强制重写。

从现在开始，新写的 tests / examples：

- 有异步等待时优先 coroutine；
- RPC Client 优先 `co_await`；
- Server 新异步示例优先 CoroutineService；
- Timer 优先 `SleepFor`；
- 不新增 RpcClosure callback chain；
- 不新增 nested lambda 状态机。

同步示例可以继续保持同步形式，不要求为了使用 coroutine 而强行 coroutine 化。

---

## 10. C++ 代码风格

### 10.1 大括号

统一 Allman 风格：

```cpp
if (condition)
{
    DoSomething();
}
```

函数：

```cpp
void Foo()
{
}
```

Lambda：

```cpp
loop.QueueInLoop([this]
    {
        DoSomething();
    });
```

### 10.2 命名

类型 / 类：

```text
PascalCase
TcpConnection
RpcChannel
CoroutineService
```

函数：

```text
PascalCase
HandleRead
StartCoroutineCall
FlushInLoop
```

局部变量 / 参数：

```text
snake_case
request_id
pending_call
total_inflight
```

成员变量：

```text
snake_case_
loop_
connection_
pending_calls_
```

枚举值 / 常量：

```text
kPrefix
kConnected
kHeartbeatInterval
```

Namespace：

```text
lowercase
nebula::net
nebula::rpc
nebula::coro
```

### 10.3 空行

- 逻辑块之间允许一行空行；
- 不连续堆大量空行；
- 不为了“视觉整齐”把相关的短逻辑拆得过散；
- return 前是否空行按当前函数可读性决定，保持同一文件一致。

### 10.4 行宽与换行

默认优先写成一行；只有一行明显写不下，或者参数较多、按语义分组后更清晰时才换行。

保持单行：

```cpp
const std::size_t body_size = sizeof(std::uint32_t) + meta_size + payload_size;
```

参数较多时按语义换行：

```cpp
auto result = co_await stub.Call<Request, Response>(
    "Method",
    std::move(request));
```

复杂条件只有在单行过长或分行后明显更清晰时才拆：

```cpp
if (state == State::kConnected &&
    pending_calls_.find(request_id) != pending_calls_.end())
{
}
```

函数声明同样遵守这一原则：短签名单行，参数较多或单行过长才分行。不要为了视觉对齐主动拆行，也不要为了压成一行牺牲可读性。

### 10.5 文件模板

后续新增和修改文件一律照 `nebula/net/timer_queue.h` / `nebula/net/timer_queue.cpp` 写，
包括缩进、空行、注释位置和 include 分组。这两份文件是标准模板。

头文件模板：

```cpp
#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"
#include "nebula/net/timer_notify.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <unordered_map>
#include <variant>
#include <vector>

namespace nebula::net
{

class Channel;
class EventLoop;

// TimerQueue 管一组定时器：Linux timerfd 每 1ms 提醒一次 EventLoop，时间轮据此判断哪些 timer 到期。
// TimerSlot 用完后不会释放，而是放回空闲链表复用，避免 Add/Cancel 时反复创建和销毁节点。
class TimerQueue final : private base::Noncopyable
{
public:
    using Clock = std::chrono::steady_clock;                                                            // 只会向前走的时钟；系统改时间不会影响 timeout
    using TimePoint = Clock::time_point;                                                                // Clock 时间轴上的一个具体时刻，例如 Clock::now() + 100ms；不是“100ms”这种时长
    using Functor = std::function<void()>;                                                              // timer 到期后要执行的回调

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerId AddTimer(TimePoint deadline, Functor callback);                                             // 普通回调版本；std::function 可能发生内存分配
    TimerId AddTimer(TimePoint deadline, TimerNotify notify);                                           // RPC 热路径版本；固定大小通知，避免 std::function 分配
    void Cancel(TimerId timer_id);                                                                      // 取消 timer；在所属 EventLoop 线程里可直接从时间轮摘掉

private:
    using TimerAction = std::variant<Functor, TimerNotify>;                                             // timer 到期后，要么执行普通回调，要么发送 TimerNotify

    // TimerId 同时保存“是否跨线程”“slot 在哪”“这个 slot 是第几次被复用”三类信息。
    // generation 每复用一次都会变化，所以旧 TimerId 不会误指向后来占用同一 slot 的新 timer。
    static constexpr std::uint32_t kInvalidSlot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::uint64_t kExternalTimerMask = std::uint64_t{1} << 63;
    static constexpr std::uint64_t kSlotIndexMask = (std::uint64_t{1} << 31) - 1;
    static constexpr std::uint64_t kGenerationMask = 0xffffffffULL;

    static constexpr std::uint8_t kWheelLevelCount = 4;
    static constexpr std::uint8_t kWheelSize = 10;
    static constexpr std::uint8_t kInvalidWheelLevel = std::numeric_limits<std::uint8_t>::max();
    static constexpr std::uint8_t kInvalidBucket = std::numeric_limits<std::uint8_t>::max();
    static constexpr std::array<std::uint64_t, kWheelLevelCount> kWheelQuantumTicks{1, 10, 100, 1000};  // L0~L3 每个格子分别代表 1/10/100/1000ms

    struct TimerBucket
    {
        std::uint32_t head{kInvalidSlot};                                                               // 这个 bucket 里第一个 timer 的 slot 下标
        std::uint32_t tail{kInvalidSlot};                                                               // 这个 bucket 里最后一个 timer 的 slot 下标
    };

    // slot 正在使用时，wheel_prev/wheel_next 把它串在 bucket 里；空闲时只用 next_free 串进复用链表。
    struct TimerSlot
    {
        TimePoint deadline{};                                                                          // 这个 timer 真正应该到期的时间点
        TimerAction action{};                                                                           // 到期后要执行的回调或通知
        std::uint64_t external_timer_id{0};                                                             // 跨线程 AddTimer 预先返回的 id；普通路径为 0
        std::uint64_t wheel_rounds{0};                                                                  // timer 放在最粗的 1s 层（L3）时还要多等几整圈；用于超过 10s 的 timeout
        std::uint32_t wheel_prev{kInvalidSlot};                                                         // 同一个 bucket 里前一个 timer 的 slot 下标
        std::uint32_t wheel_next{kInvalidSlot};                                                         // 同一个 bucket 里后一个 timer 的 slot 下标
        std::uint32_t next_free{kInvalidSlot};                                                          // slot 空闲时，下一个可复用 slot 的下标
        std::uint32_t generation{0};                                                                    // slot 每复用一次就 +1，让旧 TimerId 自动失效
        std::uint8_t wheel_level{kInvalidWheelLevel};                                                   // 当前在哪一层：0=1ms，1=10ms，2=100ms，3=1s
        std::uint8_t bucket_index{kInvalidBucket};                                                      // 当前在这一层的第几个格子，范围 0~9
        bool active{false};                                                                             // 这个 slot 当前是否正被一个 timer 使用
    };

    TimerId AddTimerInLoop(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id = 0);
    void CancelInLoop(TimerId timer_id);
    void HandleRead();
    void ArmTimerFd();                                                                                  // 启动 1ms“时钟”，让时间轮继续往前走
    void DisarmTimerFd();                                                                               // 没有 timer 时停掉 1ms“时钟”，避免 EventLoop 空转
    void SyncCurrentTick();                                                                             // timerfd 停着时，用真实经过的毫秒数把 current_tick_ 追到现在

    // 这三个函数管 slot 的一生：拿一个空位给新 timer 用、用完放回空闲链表、删除 timer 时顺便回收。
    std::uint32_t AllocateSlot(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id); // 拿一个可复用 slot，并填入新 timer 的数据
    void ReleaseSlot(std::uint32_t slot_index);                                                         // 清空这个 slot，并放回复用链表
    void RemoveTimerSlot(std::uint32_t slot_index);                                                     // 把 timer 从映射和时间轮中移除，再回收 slot

    // 下面这一组负责把 timer 放进或移出时间轮；时间往前走时，再把远处的 timer 一层层搬近并执行到期 timer。
    void ScheduleTimerSlot(std::uint32_t slot_index);                                                   // 根据 timer 的到期时间，把它放进时间轮
    void PlaceTimerSlot(std::uint32_t slot_index, std::uint64_t expiry_tick);                           // 看 timer 还剩多久，决定放哪一级、哪一个格子
    void LinkTimerSlot(std::uint32_t slot_index, std::uint8_t wheel_level, std::uint8_t bucket_index);  // 把 timer 接到指定 bucket 链表末尾
    void UnlinkTimerSlot(std::uint32_t slot_index);                                                     // 把 timer 从当前 bucket 链表里摘下来
    void AdvanceWheel(std::uint64_t expirations);                                                       // 过去了多少毫秒，时间轮就往前走多少格
    void CascadeLevel(std::uint8_t wheel_level);                                                        // 上层格子走到当前时，把里面的 timer 重新放到更细的层级
    void ExpireCurrentBucket();                                                                         // 检查当前 1ms 格子：到期就执行，没到期就重新安排

    [[nodiscard]] std::uint64_t DeadlineTick(TimePoint deadline) const;                                 // 算出 deadline 是时间轮开始后的第几毫秒；不足 1ms 向后取整
    [[nodiscard]] TimerId MakeSlotTimerId(std::uint32_t slot_index, std::uint32_t generation) const;   // 把 slot 下标和 generation 编成 TimerId
    [[nodiscard]] TimerId MakeExternalTimerId();                                                        // 跨线程 AddTimer 先生成这个 id，真正入轮后仍使用它
    [[nodiscard]] bool DecodeSlotTimerId(TimerId timer_id, std::uint32_t* slot_index, std::uint32_t* generation) const; // 从普通 TimerId 拆出 slot 下标和 generation
    [[nodiscard]] static bool IsExternalTimerId(TimerId timer_id) noexcept;                            // 判断是不是跨线程路径生成的 TimerId

    static void InvokeAction(TimerAction action);                                                       // 执行普通 callback，或发送 TimerNotify

    EventLoop* loop_;                                                                                   // 这个 TimerQueue 属于哪个 EventLoop；时间轮状态只在它的线程里修改
    int timer_fd_;                                                                                       // Linux timerfd，负责每 1ms 提醒一次 EventLoop
    std::unique_ptr<Channel> timer_channel_;                                                            // 把 timer_fd_ 的可读事件接入 EventLoop

    // 时间轮本身只保存 slot 下标；TimerSlot 用完后继续留在 slots_ 里复用。
    std::vector<TimerSlot> slots_;                                                                      // 所有 timer 的槽位，空闲槽也保留在这里等待复用
    std::array<std::array<TimerBucket, kWheelSize>, kWheelLevelCount> wheel_{};                         // 四层、每层 10 个格子；每个格子串着一组 timer
    std::uint32_t free_slot_head_{kInvalidSlot};                                                        // 当前第一个可复用的空闲 slot；没有时为 kInvalidSlot
    std::uint64_t active_timer_count_{0};                                                               // 当前还有多少个正在等待触发或取消的 timer
    TimePoint wheel_epoch_{Clock::now()};                                                               // 时间轮开始计时的时间点，后面的 tick 都从这里往后数
    std::uint64_t current_tick_{0};                                                                     // 时间轮现在已经走过了多少毫秒，1 tick = 1ms
    bool timer_fd_armed_{false};                                                                        // 1ms 周期 timerfd 现在是否正在运行

    // 别的线程调用 AddTimer 时，必须先把 TimerId 返回给调用者，再由所属 EventLoop 线程真正把 timer 放进时间轮。
    // RPC timeout 正常就在所属 EventLoop 线程里执行，不走这套额外映射。
    std::atomic_uint64_t next_external_timer_id_{1};                                                    // 下一个跨线程 TimerId 使用的自增序号
    std::unordered_map<std::uint64_t, std::uint32_t> external_timer_slots_;                             // 跨线程 TimerId -> 实际 slot 下标
};

}                                                                               // namespace nebula::net
```

源文件模板（节选，见 `timer_queue.cpp` 全文）：

```cpp
void TimerQueue::HandleRead()
{
    loop_->AssertInLoopThread();

    std::uint64_t expirations = 0;
    // EventLoop 忙的时候可能一次错过多个 1ms 提醒；timerfd 会把次数累加起来，读到几次，时间轮就补走几毫秒。
    const ssize_t n = ::read(timer_fd_, &expirations, sizeof(expirations));

    // 没有新的计时提醒时 read 会返回 EAGAIN，被信号打断时返回 EINTR；这两种情况都说明这次不用往前走时间轮。
    if (n < 0)
    {
        if (errno == EAGAIN || errno == EINTR)
        {
            return;
        }

        throw std::runtime_error("timerfd read failed: " + std::string(std::strerror(errno)));
    }

    AdvanceWheel(expirations);

    // timer 回调里可能顺手取消最后一个 timer，所以这一轮处理完后再统一判断要不要停掉 timerfd。
    if (active_timer_count_ == 0)
    {
        DisarmTimerFd();
    }
}

void TimerQueue::ArmTimerFd()
{
    if (timer_fd_armed_)
    {
        return;
    }

    const TimePoint next_tick = wheel_epoch_ + std::chrono::milliseconds(static_cast<std::int64_t>(current_tick_ + 1));
    auto delay = std::chrono::duration_cast<std::chrono::nanoseconds>(next_tick - Clock::now());

    if (delay.count() <= 0)
    {
        // timerfd 里 it_value=0 表示“关闭”。如果下一毫秒边界已经错过，就设成 1ns，让它几乎立刻补一次提醒。
        delay = std::chrono::nanoseconds(1);
    }

    itimerspec value{};
    constexpr std::int64_t kNanosecondsPerSecond = 1000000000LL;
    const std::int64_t total_nanoseconds = delay.count();
    value.it_value.tv_sec = static_cast<time_t>(total_nanoseconds / kNanosecondsPerSecond);
    value.it_value.tv_nsec = static_cast<long>(total_nanoseconds % kNanosecondsPerSecond);
    value.it_interval.tv_sec = 0;
    value.it_interval.tv_nsec = 1000000L;

    // 第一次提醒对齐到下一毫秒，之后每 1ms 提醒一次；EventLoop 忙时错过的次数会在下一次 read 时一起拿回来。
    if (::timerfd_settime(timer_fd_, 0, &value, nullptr) < 0)
    {
        throw std::runtime_error("timerfd_settime failed: " + std::string(std::strerror(errno)));
    }

    timer_fd_armed_ = true;
}

void TimerQueue::DisarmTimerFd()
{
    if (!timer_fd_armed_)
    {
        return;
    }

    itimerspec value{};

    if (::timerfd_settime(timer_fd_, 0, &value, nullptr) < 0)
    {
        throw std::runtime_error("timerfd_settime failed: " + std::string(std::strerror(errno)));
    }

    timer_fd_armed_ = false;
}
```

模板固定下来的具体规则：

- 缩进固定 4 空格，禁止 tab；
- 构造函数初始化列表每个成员一行，续行与 `:` 之后的第一项对齐；
- 匿名 namespace 写法固定为 `namespace` / `{` / 内容 / `}  // namespace`；
- 注释位置：`.h` 行尾右对齐，`.cpp` 头顶单行；内容先说人话，再补必要技术细节，规则见 §11；
- include 分组与成员位置见 §12；
- 本节头文件代码块必须与真实头文件逐字一致；源文件节选必须与对应实现区段逐字一致，改模板文件时同步本节。

---

## 11. 注释规范

### 11.1 什么时候写

注释是写给“过一段时间再回来读代码的人”看的，不是写给刚写完代码、脑子里还记得全部上下文的人看的。

优先在这些地方写：

- 类 / 结构体：先用一句人话说明它整体负责什么、在哪个线程工作；
- 不直观的成员变量：直接说明“这个值代表什么”，必要时补单位、范围、生命周期；
- 不直观的 helper 函数：直接说明“调用它以后发生什么”，以及它在整条流程里负责哪一步；
- 难懂或反直觉的逻辑：再解释为什么这样设计、不能怎么用、有哪些隐藏契约。

签名和名字已经把意思说清的可以不写，例如 `void Cancel(TimerId timer_id);`。

但只要参数、返回值或类型别名本身容易让人误解，就算函数名看起来简单，也必须把真实语义写出来。尤其是：

- `TimePoint` / `deadline`：表示“时间轴上的某一个具体时刻”，不是“还要等多少毫秒”；
- `duration` / `delay` / `milliseconds`：表示“一段时间有多长”；
- `tick`：必须说明 1 tick 等于多少时间，以及从哪个起点开始数；
- `slot_index` / `bucket_index`：必须说明它是哪一个容器里的下标；
- `TimerId` / token / handle：必须说明它是给谁用来标识什么对象的。

例如 `RunAt` 和 `RunAfter` 必须让读者一眼看出差别：

```cpp
TimerId RunAt(TimePoint deadline, Functor cb);                 // 到 deadline 这个具体时刻时执行 cb
TimerId RunAfter(std::chrono::milliseconds delay, Functor cb); // 从现在开始再等 delay 这么久后执行 cb
```

也不能因为函数名里有一个英文动词就默认“读者肯定懂”。像 `Schedule`、`Place`、`Cascade`、`Advance`、`Sync` 这类依赖上下文的词，
如果单看名字不能知道它在 NebulaRPC 里具体做什么，就必须补一句人话。

例如不要只写：

```cpp
std::uint64_t current_tick_{0}; // 已处理完成的 1ms tick 序号
```

应该直接写：

```cpp
std::uint64_t current_tick_{0}; // 时间轮现在已经走过了多少毫秒，1 tick = 1ms
```

判断标准不是“只写为什么、绝不写做什么”，而是：

> 先让以后回来看代码的人立刻知道“这是什么 / 这一步干什么”，再在真正需要的地方补“为什么这么设计”。

仍然禁止机械逐行翻译，例如 `SetReadCallback` 不必写“注册读回调”，`return value` 不必写“返回 value”。

### 11.2 写在哪

`.h`：

- 类声明：允许头顶单独一行注释；
- 成员变量 / 成员函数：需要说明时优先用行尾注释，同一段声明内 `//` 起始列右对齐；
- 协议二进制布局、ownership、生命周期等跨多个声明的隐藏契约，可以在相关声明前使用块注释；
- 没有需要说明的成员就空着，不为了对齐凑注释。

`.cpp`：

- 统一头顶单独一行注释，不用行尾注释；
- 用于说明这一段在整条流程里做什么，或为什么必须这样写；不机械逐行翻译。

完整示例见 §10.5 的 `timer_queue.h` / `timer_queue.cpp` 模板。

示例：

```cpp
class RpcCall final
{
public:
    [[nodiscard]] RpcCallState State() const noexcept;
    bool TryComplete(RpcCallState state) noexcept;                         // 尝试抢到这次 RPC 的完成权；只有第一个调用者会成功

private:
    std::atomic<RpcCallState> state_{RpcCallState::Pending};               // 这次 RPC 当前处于什么状态；多个完成路径靠它保证只完成一次
};
```

### 11.3 写什么

注释统一采用：

> **先说人话，再补必要的英文技术名词。**

第一句话优先回答下面四类问题之一：

- 类型别名：**这个类型在业务里到底代表什么？是“一个时刻”、一段时长、ID、下标还是句柄？**
- 变量：**这个值代表什么？单位是什么？从哪里开始算？**
- 函数：**调用它以后发生什么？参数表示“什么时候”还是“等多久”？**
- 复杂逻辑：**为什么必须这样做？**

时间相关类型必须把“时刻”和“时长”分开写清楚。例如：

```cpp
using Clock = std::chrono::steady_clock;     // 只会向前走的时钟；系统改时间不会影响 timeout
using TimePoint = Clock::time_point;         // Clock 时间轴上的一个具体时刻，例如 Clock::now() + 100ms；不是“100ms”这种时长
using Functor = std::function<void()>;        // timer 到期后要执行的回调
```

看到 `TimePoint deadline` 时，读者应该立刻知道它表示“具体什么时候到期”；看到 `milliseconds delay` 时，应该立刻知道它表示“从现在开始还要等多久”。

技术名词可以用，但不能拿技术名词继续解释技术名词。`tick`、`bucket`、`generation`、`owner loop`、`intrusive list` 等词，
应该建立在已经说清人话含义的基础上。

不推荐：

```cpp
std::uint64_t current_tick_{0}; // 已处理完成的 1ms tick 序号
void ScheduleTimerSlot(...);    // deadline 转 tick 后安排到合适的时间轮 bucket
void CascadeLevel(...);         // 上层 bucket 轮到时把 timer 下放到更细粒度层
```

推荐：

```cpp
std::uint64_t current_tick_{0}; // 时间轮现在已经走过了多少毫秒，1 tick = 1ms
void ScheduleTimerSlot(...);    // 根据 timer 的到期时间，把它放进时间轮
void CascadeLevel(...);         // 上层格子走到当前时，把里面的 timer 重新放到更细的层级
```

如果还需要解释实现细节，再在 `.cpp` 里补第二层说明。例如：

```cpp
// 如果这个 deadline 已经过去，最早只能放到下一毫秒；已经走过的格子不能倒回去。
if (expiry_tick <= current_tick_)
{
    expiry_tick = current_tick_ + 1;
}
```

注释重点可以包括：

- 类型别名在业务里真正代表什么，特别是“时刻 / 时长 / ID / 下标 / 句柄”；
- 一个状态变量在现实逻辑里代表什么，以及它的单位、范围、计数起点；
- 一个 helper 在整条调用链里负责哪一步；
- 生命周期、线程归属、ownership；
- timeout / cancel 竞争关系；
- 协议字段边界、长度语义和二进制布局；
- 性能代码里为什么必须避免某个中间对象 / copy / allocation；
- 看起来反直觉但必须保持的行为。

协议格式属于隐藏契约，允许用紧凑的布局图直接写清楚，不要求压成一句话：

```cpp
// Wire Format：
//
// +-------------------+
// | frame_size   4B   |  后续 body 总长度，不包含自身 4B
// +-------------------+
// | meta_size    4B   |  RpcMeta 序列化后的长度
// +-------------------+
// | RpcMeta           |  meta_size 字节
// +-------------------+
// | payload           |  业务 protobuf 数据
// +-------------------+
//
// frame_size = 4B(meta_size) + meta_size + payload_size
```

性能热路径也先说“这一步为什么存在”，不逐行翻译实现。例如：

```cpp
// 先算出最终大小，只分配一次完整 frame，避免中间字符串反复扩容。
const std::size_t payload_size = payload.ByteSizeLong();

// Protobuf 直接写到最终 output，不再创建 meta_bytes / payload 中间字符串。
if (!meta.AppendToString(&output) || !payload.AppendToString(&output))
{
    return {};
}
```

像 `kOk`、`payload`、`SetReadCallback` 这类名字已经说明含义的内容不补注释。

推荐：

```cpp
// Write 只放进待发送缓冲；真正什么时候发，由上层 Flush 决定。
connection_->Write(data);
```

推荐：

```cpp
// tasks_ 必须比 channel_ 先销毁，否则仍在等待的 coroutine 可能访问已经失效的 RpcChannel。
std::vector<Task<int>> tasks_;
```

不推荐：

```cpp
// 调用 Write。
connection_->Write(data);
```

不推荐写大段教程式注释；目标是让代码重新变得容易读，而不是把源码变成教材。

### 11.4 CMakeLists

**版式**

- `add_executable` / `add_library` 的源文件列表一个一行；
- `target_include_directories` / `target_link_libraries` **一律单行**，不换行、不把 `PRIVATE` 单独拆出去：

```cmake
target_include_directories(nebula_rpc_timeout_test PRIVATE ${PROJECT_BINARY_DIR}/examples/rpc_echo)
target_link_libraries(nebula_rpc_timeout_test PRIVATE nebula_rpc nebula_rpc_echo_proto)
```

不要写成：

```cmake
target_link_libraries(nebula_rpc_timeout_test
    PRIVATE
        nebula_rpc
        nebula_rpc_echo_proto
)
```

**注释**

- 每个 target 组上方一行 `# 一句话。`，中文，句号结尾；
- 只描述这个 target 是什么 / 测什么，不描述它怎么构建；
- 复杂的 target 允许续行，最多两三行，冒号引出细节；
- `target_link_libraries`、`add_test`、`set_tests_properties` 不再单独注释。

**测试 target 模板**

```cmake
# 取消最早到期的 timer 不能影响后续 timer 的触发。
add_executable(nebula_timer_queue_cancel_earliest_test timer_queue_cancel_earliest_test.cpp)
target_link_libraries(nebula_timer_queue_cancel_earliest_test PRIVATE nebula_net)
add_test(NAME nebula_timer_queue_cancel_earliest_test COMMAND nebula_timer_queue_cancel_earliest_test)
set_tests_properties(nebula_timer_queue_cancel_earliest_test PROPERTIES RUN_SERIAL TRUE)
```

**示例程序模板**

```cmake
# Fixed-InFlight 协程压测客户端：
# 每个 Worker 一个线程 / EventLoop / RpcChannel / TCP 长连接，
# 每条连接同时维持 window 个协程 RPC。
add_executable(nebula_rpc_echo_fixed_inflight_client rpc_echo_fixed_inflight_client.cpp)
target_include_directories(nebula_rpc_echo_fixed_inflight_client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(nebula_rpc_echo_fixed_inflight_client PRIVATE nebula_rpc nebula_rpc_echo_proto)
```

参照 `tests/CMakeLists.txt` 与 `examples/rpc_echo/CMakeLists.txt`。

---

## 12. Header / Source 组织

头文件尽量：

- 前置声明能解决时优先前置声明；
- 不无意义扩大 include；
- public API 在前；
- private implementation 在后；
- 成员变量集中放在类尾部；
- 生命周期相关成员顺序必须有明确理由时，用短注释说明。

`.cpp`：

- include 自己的 header 优先；
- 项目 header；
- 第三方 header；
- STL header；
- implementation。

---

## 13. 错误处理

RPC coroutine：

- 业务可恢复错误优先通过 `RpcResult` 返回；
- 框架内部 invariant 失败可以 assert / fail fast；
- 不要用 exception 代替普通状态分支；
- exception 用于无法正常继续的异常情况；
- timeout / cancel / disconnect 必须统一映射到明确的 RpcCallState。

禁止吞掉错误：

```cpp
catch (...)
{
}
```

---

## 14. 性能与内存规范

新代码不要因为“可能更快”就提前优化。

性能改动必须优先验证：

- benchmark；
- perf；
- syscall count；
- flame graph；
- latency；
- allocation；
- batch size；
- correctness。

Hot path 中避免无意义：

- heap allocation；
- string copy；
- mutex；
- shared_ptr；
- type erasure；
- repeated descriptor lookup。

但没有实验数据时，不为了理论收益引入复杂度。

---

## 15. API 兼容策略

Legacy API 可以保留：

```text
RpcClosure
CallMethod(..., done)
protobuf Service / Closure
```

但定位为：

> Compatibility API

新功能默认不继续围绕 Legacy API 扩展。

如果新 Coroutine API 和 Legacy API 同时存在：

- 核心状态机尽量共用；
- 不维护两套独立 RPC 生命周期实现；
- Legacy 层尽量作为兼容适配层；
- Coroutine 路径优先成为未来主路径。

---

## 16. 新增代码禁止事项

除非有明确理由，新代码禁止：

- 新增 RpcClosure 异步业务链；
- 新增 done->Run() 驱动业务状态；
- nested lambda callback hell；
- 在 TcpConnection 中猜测业务 Flush 时机；
- 为 EventLoop-owned 对象无条件增加 mutex；
- 跨线程直接操作属于其他 EventLoop 的状态；
- 无 exactly-once 保护直接 resume coroutine；
- coroutine frame 生命周期不明确；
- 将性能推测写成既定事实；
- 为了减少代码行数破坏分层。

---

## 17. 修改现有代码时的原则

- 优先做最小、边界清晰的修改；
- 不顺手大规模格式化无关文件；
- 功能修改与纯格式化尽量分开；
- 不为了新架构强行重写全部旧 tests/examples；
- 如果修改公共 API，必须同步搜索所有调用点；
- 编译通过后再进入 benchmark / profiling；
- 性能测试条件必须保持可复现。

---

## 18. 提交前基本检查

至少确认：

```text
1. Release / Debug 按当前任务要求能够编译；
2. 没有遗留旧 API 调用导致编译错误；
3. Coroutine 不存在明显 double resume；
4. timeout / cancel / disconnect completion 仍保持 exactly once；
5. Write / Flush 边界没有被无意改变；
6. 新异步业务没有重新引入 callback chain；
7. 新 tests/examples 遵守 coroutine-first 原则；
8. 没有夹带无关格式化；
9. 注释先用人话说明类型、单位、变量和 helper 到底代表什么；TimePoint/Duration、ID/下标、tick/单位等不能靠读者猜，再补必要的设计原因；不要机械逐行翻译代码；
10. 性能结论没有超过实际实验能够证明的范围。
```