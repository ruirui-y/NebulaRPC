#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
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

    // 信号处理函数专用：只做原子置位与 async-signal-safe 的 eventfd 写入，不取锁、不分配
    void NotifyFromSignal() noexcept;

    // 收到信号通知后在 loop 线程执行一次；必须在 Loop() 启动前设置
    void SetSignalCallback(Functor cb)
    {
        signal_callback_ = std::move(cb);
    }

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
    std::atomic_bool signal_pending_{false};
    const std::thread::id thread_id_;

    // 只在 Loop() 启动前写、之后只读，因此不需要额外同步
    Functor signal_callback_;

    std::unique_ptr<Poller> poller_;
    int wakeup_fd_;
    std::unique_ptr<Channel> wakeup_channel_;
    std::unique_ptr<TimerQueue> timer_queue_;
    ChannelList active_channels_;

    mutable std::mutex mutex_;
    std::vector<Functor> pending_functors_;
};

}  // namespace nebula::net
