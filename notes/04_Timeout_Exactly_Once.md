# RPC Timeout / Exactly-Once Completion

## 1. Timeout

RPC 请求发送后：

    CallMethod
        ↓
    PendingCall
        ↓
    发送请求

如果服务器没有及时响应：

    TimerQueue
        ↓
    timeout callback
        ↓
    完成 RPC

------------------------------------------------------------------------

## 2. timeout 与 deadline

timeout:

    还能等待多久

类型：

    Duration

例如：

    500ms

deadline:

    最晚完成时间

类型：

    TimePoint

内部通常：

    deadline = now + timeout

当前 NebulaRPC 阶段主要使用 timeout。

------------------------------------------------------------------------

## 3. PendingCall

PendingCall 保存一次未完成 RPC：

    request_id
    response
    controller
    done callback
    timer

请求完成后：

    PendingCall
        ↓
    remove
        ↓
    done()

------------------------------------------------------------------------

## 4. Exactly-Once Completion

RPC 有多个完成来源：

    Response
    Timeout
    Disconnect
    Cancel

问题：

    Response
        \
         \
          Complete()
         /
    Timeout

必须保证：

    done callback 只执行一次

核心方式：

    TakePendingCall(request_id)

谁先拿走 PendingCall：

    谁完成 RPC

之后其他事件：

    找不到 request_id

直接丢弃。

------------------------------------------------------------------------

## 5. 后续扩展

在此基础上继续增加：

    Cancel
    Retry
    Coroutine

都需要依赖稳定的 RPC 状态机。
