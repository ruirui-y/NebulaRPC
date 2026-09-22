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

class TimerQueue final : private base::Noncopyable
{
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Functor = std::function<void()>;

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerId AddTimer(TimePoint deadline, Functor callback);
    void Cancel(TimerId timer_id);

private:
    using DeadlineMap = std::multimap<TimePoint, std::uint64_t>;

    struct Timer
    {
        Functor callback;
        DeadlineMap::iterator deadline_it;
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

    std::atomic_uint64_t next_timer_id_{1};
    DeadlineMap deadlines_;
    std::unordered_map<std::uint64_t, Timer> timers_;
};

}  // namespace nebula::net
