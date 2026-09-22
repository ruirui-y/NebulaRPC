# TimerQueue

## 1. 为什么需要 TimerQueue

RPC Timeout、定时任务等功能需要一个可靠的时间事件机制。

传统方式：

    while(true)
    {
        检查当前时间
    }

会导致：

-   CPU 空转
-   无法统一管理事件
-   和 Reactor 模型割裂

NebulaRPC 使用 Linux timerfd，将时间事件转换成 fd 事件。

    Timer
     ↓
    timerfd
     ↓
    epoll
     ↓
    EventLoop

------------------------------------------------------------------------

## 2. TimerQueue 设计

TimerQueue 负责：

-   添加定时任务
-   删除定时任务
-   找到最近到期任务
-   处理 timerfd 事件

核心流程：

    AddTimer
        ↓
    保存 deadline
        ↓
    ResetTimerFd
        ↓
    timerfd_settime
        ↓
    等待 epoll 事件

------------------------------------------------------------------------

## 3. steady_clock

Timer 使用：

``` cpp
std::chrono::steady_clock
```

原因：

-   单调递增
-   不受系统时间修改影响

区分：

    TimePoint
    = 某个时间点

    Duration
    = 一段时间

------------------------------------------------------------------------

## 4. timerfd 工作流程

没有任务：

    timerfd disable

有任务：

    找到最近 deadline

    now -> deadline

    设置 timerfd

到期：

    timerfd 可读
        ↓
    EventLoop
        ↓
    TimerQueue::HandleRead()
        ↓
    执行 callback

------------------------------------------------------------------------

## 5. Reactor 中的时间事件

NebulaRPC 统一使用 EventLoop：

    socket fd
    timer fd
    eventfd

全部进入：

    epoll_wait()

因此 Reactor 不需要额外轮询时间。
