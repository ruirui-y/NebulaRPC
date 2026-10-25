#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"
#include "nebula/net/timer_notify.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <unordered_map>
#include <variant>

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
    TimerId AddTimer(TimePoint deadline, TimerNotify notify);
    void Cancel(TimerId timer_id);

private:
    using DeadlineMap = std::multimap<TimePoint, std::uint64_t>;    // 同一 deadline 可挂多个定时器
    using TimerAction = std::variant<Functor, TimerNotify>;

    struct Timer
    {
        TimerAction action;
        DeadlineMap::iterator deadline_it;                          // 指向 deadlines_ 中位置，取消时 O(1) 摘除
    };

    void AddTimerInLoop(TimerId timer_id,
                        TimePoint deadline,
                        TimerAction action);
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
