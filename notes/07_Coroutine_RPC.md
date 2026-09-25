# 07. C++20 Coroutine RPC 实现记录

前置：第 7 节（Timeout / Cancel / Exactly-Once）已完成并提交（`7c6da7a`）。
本轮目标：把「callback 收尾」变成「`co_await` 收尾」，且**不改动已经验证过的完成路径**。

------------------------------------------------------------------------

# 1. 结论先行

```
callback 版：stub.Echo(&controller, &request, &response, done);
协程版    ：EchoResponse r = co_await RpcAwaiter<Req, Resp>(&channel, method, req, timeout);
```

协程层是**纯增量**：`rpc_channel.cpp` 的完成路径一行未改，只加了终态回填。
整个「协程怎么被唤醒」复用了一个既有事实——**`done` 是四条完成路径唯一的共同出口**。

------------------------------------------------------------------------

# 2. 核心取舍：为什么复用 done 而不是给 PendingCall 加句柄

原计划（Roadmap 第 8 节步骤 3）是给 `PendingCall` 加 `std::coroutine_handle<>`，
把完成路径改成「`done->Run()` 或 `handle.resume()` 二选一」。实际没有这么做，原因：

```
完成路径现状（rpc_channel.cpp:31-65）
    CompleteFailure(controller, done, reason)  { ... if (done != nullptr) done->Run(); }
    CompleteFrame(response, controller, done, f){ ... if (done != nullptr) done->Run(); }
    CompleteCallWithCancel                        loop_->QueueInLoop([done]{ done->Run(); });
    FailAllPendingNow（析构路径）                  CompleteFailure(...) 同上

→ done 已经覆盖：response / timeout / cancel / 断连 / 析构 五条出口
```

于是「协程」只需要提供一个**特殊的 done**：跑起来就把协程恢复。

| | 复用 done（采用） | PendingCall 加句柄（原计划） |
|---|---|---|
| 改动面 | channel 零改动 | 四条路径 + 析构路径 + 字段互斥 |
| 漏 resume 的风险 | 无（出口唯一） | 断连/析构两条最易漏 → 协程永久挂起 = 帧泄漏 |
| 新问题 | done 可能**早于挂起**被调用（见 §4） | 无 |
| 恢复线程 | = done 执行线程 | 需要自己定义 |

代价集中在 §4 那一个点上，用 `QueueInLoop` 解决，比"改五条路径且可能漏"划算。

------------------------------------------------------------------------

# 3. 四个关键机制

## 3.1 Task\<T\>：句柄与帧的所有权

```
Task<T>           持有 coroutine_handle<promise_type>，唯一的销毁者
promise_type      continuation（上层协程）/ value（optional<T>）/ exception（exception_ptr）
initial_suspend   suspend_always → 惰性：创建后不跑，Start() 或被 co_await 才跑
final_suspend     FinalAwaiter：有 continuation 就对称转移回上层，否则 noop 停住
~Task()           handle_.destroy()
```

**为什么 final_suspend 返回 noop 而不是 destroy 自己**：顶层用法是
`task.Start(); loop.Loop(); return task.Result();` —— 结果要从 promise 里读，
帧必须停在 final suspend point 由 `~Task` 销毁。在 `await_suspend` 内部销毁自己所在的帧
既难讲清也易踩坑，把销毁权交给持有者是更干净的模型。

**只做 `Task<T>`、不做 `Task<void>`**：promise 不能同时声明 `return_value` 与 `return_void`
（标准明文禁止），所以 `Task<void>` 需要特化 promise；本轮用 `Task<int>` 返回退出码规避，
代价是语义上借用 int。要做的话正路是 `TaskPromiseBase<T>` + 两个派生。

## 3.2 ResumeGuard：两个持有者

```
        RpcAwaiter::guard_  ──┐
                              ├──→  shared_ptr<ResumeGuard>{ atomic<coroutine_handle<>> handle }
        done 闭包捕获的 guard ─┘

完成路径到来   exchange(nullptr) 拿到非空 → resume()
awaiter 析构   exchange(nullptr) 拿到非空 → 说明还在飞 → StartCancel()
```

`exchange`（认领）而不是 `load`（观察）是关键：只有 RMW 能保证两个持有者**不会都认为自己该 resume**。
`load` 会导致双重恢复（协程可能已跑完或挂在下一个 await 上，再推一把 = UB）。

`ResumeGuard` 是 `shared_ptr`，**比协程帧长寿**，专门用来表达「帧已经不在」——
和 channel 侧 `AliveGuard` 是同一个思路：小对象比被保护对象活得久。

## 3.3 析构即取消：`~RpcAwaiter`

帧被销毁（Task 中途丢弃）时，`PendingCall` 里仍然留着 `&response_` / `&controller_`。
不取消 → 响应回来时 `ParseFromString` 写已释放内存 + resume 已销毁帧 = 两处 UAF。

所以 `~RpcAwaiter` 判「是否还挂着」，是就 `StartCancel()`，让调用走**已有的**取消完成路径
做全部清理（摘 pending、取消定时器、摘取消回调、丢排队 bytes），不在析构里另写一套。

职责划分：**清理归 channel，恢复权归协程侧**，两个机制各管一件事，不互相踩。

## 3.4 恢复走队列 + loop 线程契约

```cpp
// rpc_awaiter.h 的 done 闭包
loop->QueueInLoop([guard]
    {
        if (const auto resumed = guard->handle.exchange(nullptr); resumed != nullptr)
        {
            resumed.resume();
        }
    });
```

两条理由：

1. **`CallMethod` 会同步失败**。method 为空 / 序列化失败 / 帧编码失败都会走
   `loop_->RunInLoop(CompleteFailure(...))`，而 `RunInLoop` 在 loop 线程上是**内联执行**的
   （`event_loop.cpp:108`）。于是 `done->Run()` 发生在 `await_suspend` 还没返回时——
   此刻协程**尚未挂起**，`resume()` 它是 UB。
   `QueueInLoop` 把恢复变成积压任务，`DoPendingFunctors()` 只在当前栈全部展开后跑 → 时序安全。
2. 顺带抹平 done 四条路径「内联 / 排队」的差异，恢复点统一在 loop 线程队列里。

配套契约：**`co_await` 必须发生在 channel 的属主 loop 线程**，用
`channel_->Loop()->AssertInLoopThread()` 强制。理由：跨线程时「挂起」与「恢复」可能并发，
`QueueInLoop` 只保证「不早于当前栈展开」，而另一个线程的栈没有早晚可言。
要真支持任意线程需引入 executor，本轮不做。

------------------------------------------------------------------------

# 4. 终态可读：`MarkCallState`

原计划「`await_resume` 里检查 `TryComplete` 的最终状态」拿不到——最终状态住在 `PendingCall`
内部，`erase` 之后就没了。补法：`RpcController` 增加 `call_state_`，由**抢到完成权**的路径写入。

```
rpc_channel.cpp
    CompleteCallWithFrame   → SetCallState(controller, Completed)
    CompleteCallWithFailure → SetCallState(controller, state)      // Timeout / Failed
    FailAllPendingNow       → SetCallState(controller, Failed)
    CompleteCallWithCancel  → MarkCanceled() 里顺带写 Cancelled

rpc_awaiter.h　await_resume 判断顺序（顺序有意义）
    CallState()==Cancelled      → RpcError(Cancelled)      取消故意不 SetFailed，必须最先判
    CallState()==Timeout/Failed → RpcError(state, ErrorText())
    controller_.Failed()        → RpcError(Failed, ...)    仲裁赢 Completed 但业务失败（ERROR 帧等）
    else                        → 返回 response
```

不这么做就只能靠 `ErrorText() == "RPC timeout"` 字符串嗅探，脆。

**必须由赢家写**：写在 `CallMethod` 会被后来者覆盖；写在 `StartCancel` 会让「取消输给响应」
也留下 Cancelled 终态，与「response 已被填充」自相矛盾——和把 `IsCanceled()` 改成终态语义
要解决的是同一个问题。

------------------------------------------------------------------------

# 5. 验收

```
tests/rpc_coroutine_test.cpp
    case1  co_await 拿到正确响应
    case2  服务端延迟 250ms + 客户端 100ms → RpcError(Timeout)，迟到响应被丢弃
    case3  超时之后同一条 channel 仍能正常发请求
    case4  发出请求后立刻丢弃 Task（析构即取消）→ 不崩、通道不被搞坏
    watchdog 3s：协程若永久挂起会判定失败（而不是卡死）
```

本机无法构建时用的替代验证（项目 Linux-only）：

- `Task<T>` 机制写了一个可运行自检：惰性 / 挂起 / 恢复 / 嵌套对称转移 / 返回值 / 异常 / move
  七组断言 → 全过；
- `RpcAwaiter` 用 MSVC `/std:c++20 /W4 /Zs` 做语法与语义检查 → 0 warning；
- **尚未跑**：`ctest --preset test-linux-debug`、`test-linux-asan`。ASan 是这批生命周期代码
  唯一有效的验证手段，普通 Debug 跑通不代表干净。

------------------------------------------------------------------------

# 6. 遗留

- `Task<void>` 未做（需要 promise 特化）。
- 恢复线程只支持属主 loop 线程；executor 化未做。
- 服务端侧协程（handler 里 `co_await`）未做——需要 `RpcServer` 也接一套完成语义。
- `nebula/rpc/rpc_call_context.h` 零引用死文件，2026-09-25 已删除。
