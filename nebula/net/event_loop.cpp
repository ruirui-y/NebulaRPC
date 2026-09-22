#include "nebula/net/event_loop.h"

#include "nebula/net/channel.h"
#include "nebula/net/poller.h"
#include "nebula/net/timer_queue.h"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <sys/eventfd.h>
#include <unistd.h>

namespace nebula::net
{
namespace
{

thread_local EventLoop* g_loop_in_this_thread = nullptr;

int CreateEventFd()
{
    const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);

    if (fd < 0)
    {
        throw std::runtime_error("eventfd failed: " + std::string(std::strerror(errno)));
    }

    return fd;
}

}  // namespace

EventLoop::EventLoop()
    : thread_id_(std::this_thread::get_id()),
      poller_(Poller::NewDefaultPoller(this)),
      wakeup_fd_(CreateEventFd()),
      wakeup_channel_(std::make_unique<Channel>(this, wakeup_fd_))
{
    if (g_loop_in_this_thread != nullptr)
    {
        throw std::logic_error("only one EventLoop is allowed per thread");
    }

    g_loop_in_this_thread = this;

    wakeup_channel_->SetReadCallback([this]
        {
            HandleWakeUpRead();
        });
    wakeup_channel_->EnableReading();

    timer_queue_ = std::make_unique<TimerQueue>(this);
}

EventLoop::~EventLoop()
{
    AssertInLoopThread();

    timer_queue_.reset();
    wakeup_channel_->DisableAll();
    wakeup_channel_->Remove();
    ::close(wakeup_fd_);

    g_loop_in_this_thread = nullptr;
}

void EventLoop::Loop()
{
    AssertInLoopThread();

    if (looping_.exchange(true))
    {
        throw std::logic_error("EventLoop::Loop called while already looping");
    }

    quit_ = false;

    while (!quit_.load())
    {
        active_channels_.clear();
        poller_->Poll(10000, &active_channels_);

        for (Channel* channel : active_channels_)
        {
            channel->HandleEvent();
        }

        DoPendingFunctors();
    }

    looping_ = false;
}

void EventLoop::Quit()
{
    quit_ = true;

    if (!IsInLoopThread())
    {
        WakeUp();
    }
}

void EventLoop::RunInLoop(Functor cb)
{
    if (IsInLoopThread())
    {
        cb();
    }
    else
    {
        QueueInLoop(std::move(cb));
    }
}

void EventLoop::QueueInLoop(Functor cb)
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_functors_.push_back(std::move(cb));
    }

    if (!IsInLoopThread() || calling_pending_functors_.load())
    {
        WakeUp();
    }
}

TimerId EventLoop::RunAt(TimePoint deadline, Functor cb)
{
    return timer_queue_->AddTimer(deadline, std::move(cb));
}

TimerId EventLoop::RunAfter(std::chrono::milliseconds delay, Functor cb)
{
    return RunAt(Clock::now() + delay, std::move(cb));
}

void EventLoop::CancelTimer(TimerId timer_id)
{
    timer_queue_->Cancel(timer_id);
}

void EventLoop::UpdateChannel(Channel* channel)
{
    AssertInLoopThread();
    poller_->UpdateChannel(channel);
}

void EventLoop::RemoveChannel(Channel* channel)
{
    AssertInLoopThread();
    poller_->RemoveChannel(channel);
}

bool EventLoop::HasChannel(Channel* channel) const
{
    return poller_->HasChannel(channel);
}

void EventLoop::AssertInLoopThread() const
{
    if (!IsInLoopThread())
    {
        throw std::logic_error("EventLoop used from a non-owner thread");
    }
}

void EventLoop::WakeUp()
{
    constexpr std::uint64_t one = 1;
    const ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));

    if (n < 0 && errno != EAGAIN)
    {
        throw std::runtime_error("eventfd write failed: " + std::string(std::strerror(errno)));
    }
}

void EventLoop::HandleWakeUpRead()
{
    std::uint64_t value = 0;
    const ssize_t n = ::read(wakeup_fd_, &value, sizeof(value));

    if (n < 0 && errno != EAGAIN)
    {
        throw std::runtime_error("eventfd read failed: " + std::string(std::strerror(errno)));
    }
}

void EventLoop::DoPendingFunctors()
{
    std::vector<Functor> functors;
    calling_pending_functors_ = true;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        functors.swap(pending_functors_);
    }

    for (auto& functor : functors)
    {
        if (functor)
        {
            functor();
        }
    }

    calling_pending_functors_ = false;
}

}  // namespace nebula::net
