#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"
#include "nebula/net/timer_notify.h"

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

// 事件循环：poll 事件、定时器、跨线程投递都收敛到所属线程执行。
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

    void NotifyFromSignal() noexcept;                                   // 信号处理专用：只做原子置位与 eventfd 写，不取锁不分配

    void SetSignalCallback(Functor cb)                                  // 回调在 loop 线程执行；必须在 Loop() 前设置
    {
        signal_callback_ = std::move(cb);
    }

    TimerId RunAt(TimePoint deadline, Functor cb);
    TimerId RunAt(TimePoint deadline, TimerNotify notify);
    TimerId RunAfter(std::chrono::milliseconds delay, Functor cb);
    TimerId RunAfter(std::chrono::milliseconds delay, TimerNotify notify);
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

    std::atomic_bool looping_{ false };
    std::atomic_bool quit_{ false };
    std::atomic_bool calling_pending_functors_{ false };                // 批次执行中投递需要补一次 WakeUp
    std::atomic_bool signal_pending_{ false };
    const std::thread::id thread_id_;

    Functor signal_callback_;                                           // 只在 Loop() 前写，之后只读

    std::unique_ptr<Poller> poller_;
    int wakeup_fd_;
    std::unique_ptr<Channel> wakeup_channel_;
    std::unique_ptr<TimerQueue> timer_queue_;
    ChannelList active_channels_;

    std::vector<Functor> local_pending_functors_;                       // loop 线程本地投递缓冲
    std::vector<Functor> local_executing_functors_;                     // 本地执行缓冲，与 pending 交换复用容量

    std::mutex remote_mutex_;                                           // 保护跨线程投递队列
    std::vector<Functor> remote_pending_functors_;                      // 跨线程投递缓冲
    std::vector<Functor> remote_executing_functors_;                    // 跨线程执行缓冲，与 pending 交换复用容量
};

}  // namespace nebula::net