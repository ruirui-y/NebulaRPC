# playground —— 知识点探针沙盒

## 这是什么

放**单个知识点**的最小验证程序。和 `tests/` 的分工：

| | `playground/` | `tests/` |
|---|---|---|
| 目的 | 把一个语言/库机制单独跑通、看清时序 | 验证 NebulaRPC 的功能与契约 |
| 依赖 | 多数纯 C++20 标准库；碰网络机制的链 `nebula_net` | 依赖 `nebula` 库、protobuf、EventLoop |
| 形态 | 每个文件一个 `main()`，打印时序 | ctest 断言用例 |

它和 `tests/` 一样由**根 `CMakeLists.txt` 统一引入**（`:42` 的 `add_subdirectory(playground)`），所以也是 Linux-only：

```cmake
# CMakeLists.txt:5-7
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "NebulaRPC baseline currently supports Linux only")
endif()
```

编译选项直接复用根的 `nebula_build_options`，警告级别 / sanitizer 开关跟主库完全一致，不另起一套。

## 约定

- 命名：`NN_topic.cpp` → 目标名 `pg_<topic>`，序号只增不改。
- 一个文件**只验证一个点**，够短、能一口气读完。
- 结论写在文件头注释里，正文靠 `printf` 打印执行顺序自证。
- 注释只用单行；画图用 ASCII。

## 构建

跟主体一起走根 preset，不单独 configure：

```
cmake --preset linux-debug
cmake --build build/linux-debug --target pg_coroutine_suspend_order
./build/linux-debug/bin/pg_coroutine_suspend_order
```

产物统一落在 `build/linux-debug/bin/`（根 `CMakeLists.txt:14` 设的 `CMAKE_RUNTIME_OUTPUT_DIRECTORY`）。

它**不在 ctest 里** —— 探针是给人看时序的，不是断言用例，所以没有 `add_test`。

## 预期输出（`01_coroutine_suspend_order`）

```
--- 步骤 1：构造协程对象（initial_suspend 挂住，函数体一行都没跑）---
--- 步骤 2：构造完成。上面没有 <Coro> 字样 => 函数体确实没执行 ---

--- 步骤 3：Start() -> resume，跑进 Coro 直到 co_await ---
      <Coro> 函数体【开始】执行 —— 说明协程真被 resume 了
      <Coro> 马上 co_await
        [A] await_ready()   -> false
        [A] await_suspend() 进入   <- 协程【此刻已经被视为挂起】
        [A] await_suspend() 返回 void = 保持挂起
--- 步骤 4：Start() 已经返回了！ <== 线程回到 main，不是 co_await 的下一行 ---
                Done()=0（false = 协程还挂着）

--- 步骤 5：手动 resume（现实中这里换成 IO 就绪 / 超时定时器 / done 回调）---
        [A] await_resume()  <- co_await 表达式在这里取到值
      <Coro> co_await 的【下一行】   <== 必须等再次 resume 才到这里
--- 步骤 6：resume 返回，Done()=1（true = 协程跑完，停在 final suspend）---
```

**盯住「步骤 4」的位置**：它插在 `await_suspend 返回` 之后、`await_resume` 之前。这就是「`await_suspend` 跑完 ≠ co_await 下一行」的全部证据 —— 中间隔着一次 `Start()` 返回和一次全新的 `Resume()`。

## 预期输出（`03_client_reconnect`）

时间轴：`0ms` 连上 → `400ms` 停掉 server → `1200ms` 重启 server → `3000ms` 退出。

```
t=    0ms  client CONNECTED
t=  400ms  client DISCONNECTED
t=  400ms  connect failed: Connection refused      <- 报丧后立刻重试第一次，不排队
t=  500ms  connect failed: Connection refused
t=  700ms  connect failed: Connection refused
t= 1100ms  connect failed: Connection refused
t= 1900ms  client CONNECTED                        <- 服务端已回来，这次成功
```

（时间戳有几十毫秒抖动，看**间隔**而不是绝对值：400→500→700→1100 正是 100/200/400 的退避。）

两个判据：

- **第二行 `DISCONNECTED` 之后必须还有一次 `CONNECTED`** —— 这是本轮改动（`TcpClient::RemoveConnection` 报丧 + `Connector::Restart`）唯一的证据。只删掉报丧那三行，输出会停在 `DISCONNECTED`。
- **`DISCONNECTED` 打印在第一次失败之前** —— `TcpConnection::HandleClose` 先回调 `connection_callback_` 再回调 `close_callback_`（`tcp_connection.cpp:225-232`），重连是在后者里发起的。

## 清单

| 文件 | 验证什么 |
|---|---|
| `01_coroutine_suspend_order.cpp` | `await_ready/await_suspend/await_resume` 的调用时刻；证伪「await_suspend 跑完就执行 co_await 下一行」 |
| `02_connector_backoff.cpp` | 连不上时 `Connector` 的退避节奏（100→200→400→800→1600→5000 封顶） |
| `03_client_reconnect.cpp` | 已建连接掉线后 `TcpClient` 能否报丧给 `Connector` 并重新连上 |

## 待办（按需添加，不预建空文件）

- `04_coroutine_reentrant_resume.cpp`：在 `await_suspend` 内部同步 `resume()` → 重入，观察 awaiter 临时对象的生命周期
- `05_atomic_memory_order.cpp`：两个线程 + 两块数据，`relaxed` vs `release/acquire` 的实际差别
- `06_coroutine_exception_from_await.cpp`：`await_suspend` 抛异常 / `await_resume` 抛异常的落点
- `07_symmetric_transfer.cpp`：`await_suspend` 返回 `coroutine_handle` 与返回 `void` 的栈深差异

