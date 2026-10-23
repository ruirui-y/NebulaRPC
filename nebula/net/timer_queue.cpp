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

TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop),
      timer_fd_(CreateTimerFd()),
      timer_channel_(std::make_unique<Channel>(loop, timer_fd_))
{
    timer_channel_->SetReadCallback([this]
        {
            HandleRead();
        });
    timer_channel_->EnableReading();
}

TimerQueue::~TimerQueue()
{
    loop_->AssertInLoopThread();
    // 先从 poller 摘掉 Channel 再关 fd，否则 epoll 里会留下已关闭的 fd 描述。
    timer_channel_->DisableAll();
    timer_channel_->Remove();
    ::close(timer_fd_);
}

TimerId TimerQueue::AddTimer(TimePoint deadline, Functor callback)
{
    const TimerId timer_id(next_timer_id_.fetch_add(1));

    loop_->RunInLoop([this,
                      timer_id,
                      deadline,
                      callback = std::move(callback)]() mutable
        {
            AddTimerInLoop(timer_id, deadline, std::move(callback));
        });

    return timer_id;
}

void TimerQueue::Cancel(TimerId timer_id)
{
    if (!timer_id.Valid())
    {
        return;
    }

    loop_->RunInLoop([this, timer_id]
        {
            CancelInLoop(timer_id);
        });
}

void TimerQueue::AddTimerInLoop(TimerId timer_id,
                                TimePoint deadline,
                                Functor callback)
{
    loop_->AssertInLoopThread();

    // 只有新定时器成为最早到期项时才重设 timerfd，否则沿用已有超时。
    const bool reset_timer_fd = deadlines_.empty() || deadline < deadlines_.begin()->first;
    auto deadline_it = deadlines_.emplace(deadline, timer_id.Value());
    timers_.emplace(timer_id.Value(), Timer{std::move(callback), deadline_it});

    if (reset_timer_fd)
    {
        ResetTimerFd();
    }
}

void TimerQueue::CancelInLoop(TimerId timer_id)
{
    loop_->AssertInLoopThread();

    const auto timer_it = timers_.find(timer_id.Value());

    if (timer_it == timers_.end())
    {
        return;
    }

    // 旧逻辑：取消队首时立即 ResetTimerFd，会产生高频 timerfd_settime syscall。
    //const bool reset_timer_fd = timer_it->second.deadline_it == deadlines_.begin();

    // 新逻辑：只删除用户态 timer，允许旧 deadline stale wakeup，
    // HandleRead 最后会重新武装到新的最早 deadline。
    deadlines_.erase(timer_it->second.deadline_it);
    timers_.erase(timer_it);

    //if (reset_timer_fd)
    //{
    //    ResetTimerFd();
    //}
}

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
        const auto deadline_it = deadlines_.begin();
        const std::uint64_t timer_id = deadline_it->second;
        const auto timer_it = timers_.find(timer_id);

        if (timer_it == timers_.end())
        {
            deadlines_.erase(deadline_it);
            continue;
        }

        Functor callback = std::move(timer_it->second.callback);
        deadlines_.erase(deadline_it);
        timers_.erase(timer_it);

        if (callback)
        {
            callback();
        }
    }

    ResetTimerFd();
}

void TimerQueue::ResetTimerFd()
{
    // it_value 决定首次到期时间，it_interval 全 0 表示不重复；第二参数 0 表示按相对时长设置。
    itimerspec value{};

    if (!deadlines_.empty())
    {
        auto delay = std::chrono::duration_cast<std::chrono::nanoseconds>(
            deadlines_.begin()->first - Clock::now());

        if (delay.count() <= 0)
        {
            // 已过期时给 1ns 而不是 0：it_value 为 0 表示解除 timerfd 定时。
            delay = std::chrono::nanoseconds(1);
        }

        // itimerspec 只认 {秒, 纳秒} 两段，chrono 的纳秒时长要在这里拆开。
        constexpr std::int64_t kNanosecondsPerSecond = 1000000000LL;
        const std::int64_t total_nanoseconds = delay.count();

        value.it_value.tv_sec = static_cast<time_t>(
            total_nanoseconds / kNanosecondsPerSecond);
        value.it_value.tv_nsec = static_cast<long>(
            total_nanoseconds % kNanosecondsPerSecond);
    }

    // 空表时 value 全 0 等于 disarm；非空时按相对时长重新武装，两种情况都要走一次系统调用。
    if (::timerfd_settime(timer_fd_, 0, &value, nullptr) < 0)
    {
        throw std::runtime_error("timerfd_settime failed: " + std::string(std::strerror(errno)));
    }
}

}  // namespace nebula::net
