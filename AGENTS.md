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

长函数调用按参数语义换行：

```cpp
auto result = co_await stub.Call<Request, Response>(
    "Method",
    std::move(request));
```

复杂条件：

```cpp
if (state == State::kConnected &&
    pending_calls_.find(request_id) != pending_calls_.end())
{
}
```

不要为了压成一行牺牲可读性。

### 10.5 文件模板

后续新增和修改文件一律照 `nebula/net/timer_queue.h` / `nebula/net/timer_queue.cpp` 写，
包括缩进、空行、注释位置和 include 分组。这两份文件是标准模板。

头文件模板：

```cpp
#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <unordered_map>

namespace nebula::net
{

class Channel;
class EventLoop;

// 定时器队列：timerfd 接入 EventLoop 的 epoll，定时回调统一在本 loop 线程执行。
class TimerQueue final : private base::Noncopyable
{
public:
    using Clock = std::chrono::steady_clock;                        // 单调时钟，不受系统时间跳变影响
    using TimePoint = Clock::time_point;
    using Functor = std::function<void()>;

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerId AddTimer(TimePoint deadline, Functor callback);
    void Cancel(TimerId timer_id);

private:
    using DeadlineMap = std::multimap<TimePoint, std::uint64_t>;    // 同一 deadline 可挂多个定时器

    struct Timer
    {
        Functor callback;
        DeadlineMap::iterator deadline_it;                          // 指向 deadlines_ 中位置，取消时 O(1) 摘除
    };

    void AddTimerInLoop(TimerId timer_id,
                        TimePoint deadline,
                        Functor callback);
    void CancelInLoop(TimerId timer_id);
    void HandleRead();
    void ResetTimerFd();

    EventLoop* loop_;
    int timer_fd_;
    std::unique_ptr<Channel> timer_channel_;

    std::atomic_uint64_t next_timer_id_{1};                         // AddTimer 可能来自非 loop 线程
    DeadlineMap deadlines_;                                         // 只存到期时间与 id
    std::unordered_map<std::uint64_t, Timer> timers_;               // 只保留存活且未触发的定时器
};

}  // namespace nebula::net
```

源文件模板（节选，见 `timer_queue.cpp` 全文）：

```cpp
#include "nebula/net/timer_queue.h"

#include "nebula/net/channel.h"
#include "nebula/net/event_loop.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/timerfd.h>
#include <unistd.h>
#include <utility>

namespace nebula::net
{
namespace
{

int CreateTimerFd()
{
    // CLOCK_MONOTONIC 与 TimerQueue::Clock 对应；NONBLOCK 保证 HandleRead 不会被 read 挂住。
    const int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);

    if (fd < 0)
    {
        throw std::runtime_error("timerfd_create failed: " + std::string(std::strerror(errno)));
    }

    return fd;
}

}  // namespace

void TimerQueue::HandleRead()
{
    loop_->AssertInLoopThread();

    std::uint64_t expirations = 0;
    // timerfd 的 read 返回累计到期次数，必须一次读满 8 字节；读完计数清零。
    const ssize_t n = ::read(timer_fd_, &expirations, sizeof(expirations));

    // 非阻塞 fd 无到期数据时是 EAGAIN，被信号打断是 EINTR，两者都不算错误。
    if (n < 0 && errno != EAGAIN && errno != EINTR)
    {
        throw std::runtime_error("timerfd read failed: " + std::string(std::strerror(errno)));
    }

    const TimePoint now = Clock::now();

    // 先摘除再执行回调：回调里 Cancel 或重新 AddTimer 不会碰到正在遍历的容器。
    while (!deadlines_.empty() && deadlines_.begin()->first <= now)
    {
        // ...
    }

    ResetTimerFd();
}

}  // namespace nebula::net
```

模板固定下来的具体规则：

- 缩进固定 4 空格，禁止 tab；
- 构造函数初始化列表每个成员一行，续行与 `:` 之后的第一项对齐；
- 匿名 namespace 写法固定为 `namespace` / `{` / 内容 / `}  // namespace`；
- 注释位置：`.h` 行尾右对齐，`.cpp` 头顶单行，规则见 §11.2；
- include 分组与成员位置见 §12。

---

## 11. 注释规范

### 11.1 什么时候写

只在这三类地方写：

- 类 / 结构体声明：说明它负责什么、线程归属；
- 难懂或反直觉的函数：生命周期、所有权、竞争关系、时间窗口；
- 有隐藏契约的成员：为什么是这个类型、为什么不能换别的。

签名本身已经把用途说清的不写。例如：

```cpp
TimerId AddTimer(TimePoint deadline, Functor callback);
void Cancel(TimerId timer_id);
```

两个名字已经讲完“做什么”，再补 `// 添加定时器` 只是噪音。

注释在复述代码字面同样是噪音：`SetReadCallback` 不必写“注册读回调”，
`loop_->RunInLoop(...)` 不必写“投递到 loop 线程”。

判断标准：注释回答“为什么这么写 / 不能怎么用”，不回答“这行在做什么”。

### 11.2 写在哪

`.h`：

- 类声明：允许头顶单独一行注释；
- 成员变量 / 成员函数：用行尾注释，同一段声明内 `//` 起始列右对齐；
- 没有需要说明的成员就空着，不为了对齐凑注释。

`.cpp`：

- 统一头顶单独一行注释，不用行尾注释；
- 用于解释这一段实现为什么这么写。

完整示例见 §10.5 的 `timer_queue.h` / `timer_queue.cpp` 模板。

示例：

```cpp
class RpcCall final
{
public:
    [[nodiscard]] RpcCallState State() const noexcept;                     // 读取当前状态
    bool TryComplete(RpcCallState state) noexcept;                         // CAS 抢占完成权，只有第一次成功

private:
    std::atomic<RpcCallState> state_{RpcCallState::Pending};               // 初始为在途
};
```

### 11.3 写什么

注释使用：

> 简短中文 + 必要英文技术名词。

注释重点解释：

- 为什么这样设计；
- 生命周期；
- 线程归属；
- ownership；
- batch boundary；
- timeout / cancel 竞争关系；
- 看起来反直觉但必须保持的行为。

推荐：

```cpp
// Write 只入待发缓冲；Flush 由上层决定批次边界。
connection_->Write(data);
```

推荐：

```cpp
// Task 必须先于 channel_ 析构，避免仍在途 Awaiter 访问失效 RpcChannel。
std::vector<Task<int>> tasks_;
```

不推荐：

```cpp
// 调用 Write。
connection_->Write(data);
```

不推荐写大段教程式注释。

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
# 取消队首后的重新武装：T1 取消不能让 T2 丢失，
# 覆盖 stale wakeup 之后 timerfd 没有被重设的场景。
add_executable(nebula_timer_queue_lazy_cancel_test timer_queue_lazy_cancel_test.cpp)
target_link_libraries(nebula_timer_queue_lazy_cancel_test PRIVATE nebula_net)
add_test(NAME nebula_timer_queue_lazy_cancel_test COMMAND nebula_timer_queue_lazy_cancel_test)
set_tests_properties(nebula_timer_queue_lazy_cancel_test PROPERTIES RUN_SERIAL TRUE)
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
9. 注释描述的是设计原因，不是逐行翻译代码；
10. 性能结论没有超过实际实验能够证明的范围。
```