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

    const bool reset_timer_fd = timer_it->second.deadline_it == deadlines_.begin();
    deadlines_.erase(timer_it->second.deadline_it);
    timers_.erase(timer_it);

    if (reset_timer_fd)
    {
        ResetTimerFd();
    }
}

void TimerQueue::HandleRead()
{
    loop_->AssertInLoopThread();

    std::uint64_t expirations = 0;
    const ssize_t n = ::read(timer_fd_, &expirations, sizeof(expirations));

    if (n < 0 && errno != EAGAIN && errno != EINTR)
    {
        throw std::runtime_error("timerfd read failed: " + std::string(std::strerror(errno)));
    }

    const TimePoint now = Clock::now();

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
    itimerspec value{};

    if (!deadlines_.empty())
    {
        auto delay = std::chrono::duration_cast<std::chrono::nanoseconds>(
            deadlines_.begin()->first - Clock::now());

        if (delay.count() <= 0)
        {
            delay = std::chrono::nanoseconds(1);
        }

        constexpr std::int64_t kNanosecondsPerSecond = 1000000000LL;
        const std::int64_t total_nanoseconds = delay.count();

        value.it_value.tv_sec = static_cast<time_t>(
            total_nanoseconds / kNanosecondsPerSecond);
        value.it_value.tv_nsec = static_cast<long>(
            total_nanoseconds % kNanosecondsPerSecond);
    }

    if (::timerfd_settime(timer_fd_, 0, &value, nullptr) < 0)
    {
        throw std::runtime_error("timerfd_settime failed: " + std::string(std::strerror(errno)));
    }
}

}  // namespace nebula::net
