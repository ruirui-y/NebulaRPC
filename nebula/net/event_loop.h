#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace nebula::net
{

class Channel;
class Poller;
class TimerQueue;

class EventLoop final : private base::Noncopyable
{
public:
    using Functor = std::function<void()>;
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    EventLoop();
    ~EventLoop();

    void Loop();
    void Quit();

    void RunInLoop(Functor cb);
    void QueueInLoop(Functor cb);

    TimerId RunAt(TimePoint deadline, Functor cb);
    TimerId RunAfter(std::chrono::milliseconds delay, Functor cb);
    void CancelTimer(TimerId timer_id);

    void UpdateChannel(Channel* channel);
    void RemoveChannel(Channel* channel);
    [[nodiscard]] bool HasChannel(Channel* channel) const;

    [[nodiscard]] bool IsInLoopThread() const noexcept
    {
        return thread_id_ == std::this_thread::get_id();
    }

    void AssertInLoopThread() const;

private:
    void WakeUp();
    void HandleWakeUpRead();
    void DoPendingFunctors();

    using ChannelList = std::vector<Channel*>;

    std::atomic_bool looping_{false};
    std::atomic_bool quit_{false};
    std::atomic_bool calling_pending_functors_{false};
    const std::thread::id thread_id_;

    std::unique_ptr<Poller> poller_;
    int wakeup_fd_;
    std::unique_ptr<Channel> wakeup_channel_;
    std::unique_ptr<TimerQueue> timer_queue_;
    ChannelList active_channels_;

    mutable std::mutex mutex_;
    std::vector<Functor> pending_functors_;
};

}  // namespace nebula::net
