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
    // eventfd 只承载唤醒信号；NONBLOCK 保证计数写满时不会把调用线程阻塞住。
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
    // TimerQueue 与 wakeup fd 都按线程归属绑定，一个线程只允许一个 EventLoop。
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

    // TimerQueue 析构时还要用 loop_ 注销自己的 timerfd channel，必须先于 wakeup channel 释放。
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

        // 信号只负责把循环叫醒并置位，真正的动作回到这里执行。
        if (signal_pending_.exchange(false, std::memory_order_acq_rel) && signal_callback_)
        {
            signal_callback_();
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
    if (IsInLoopThread())
    {
        // 同线程投递时当前不可能阻塞在 epoll_wait。
        // 若本轮尚未开始处理 pending，后续会自然进入 DoPendingFunctors，无需唤醒；
        // 若当前批次已开始执行，新任务只能留到下一轮，因此需要唤醒避免下一轮 epoll_wait 阻塞。
        local_pending_functors_.push_back(std::move(cb));

        if (calling_pending_functors_.load())
        {
            WakeUp();
        }

        return;
    }

    // 跨线程投递时 EventLoop 可能正阻塞在 epoll_wait，因此必须唤醒。
    {
        std::lock_guard<std::mutex> lock(remote_mutex_);
        remote_pending_functors_.push_back(std::move(cb));
    }

    WakeUp();
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
    // eventfd 写入 8 字节计数；EAGAIN 说明计数已饱和，此时本来就处于唤醒状态。
    constexpr std::uint64_t one = 1;
    const ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));

    if (n < 0 && errno != EAGAIN)
    {
        throw std::runtime_error("eventfd write failed: " + std::string(std::strerror(errno)));
    }
}

void EventLoop::NotifyFromSignal() noexcept
{
    signal_pending_.store(true, std::memory_order_release);

    // 信号上下文里唯一安全的动作：原子写 + write；QueueInLoop 不是 async-signal-safe，不能用。
    constexpr std::uint64_t one = 1;
    [[maybe_unused]] const ssize_t n = ::write(wakeup_fd_, &one, sizeof(one));
}

void EventLoop::HandleWakeUpRead()
{
    // 读满 8 字节把计数清零；不清零 eventfd 会一直可读，下一轮 poll 立刻返回导致空转。
    std::uint64_t value = 0;
    const ssize_t n = ::read(wakeup_fd_, &value, sizeof(value));

    if (n < 0 && errno != EAGAIN)
    {
        throw std::runtime_error("eventfd read failed: " + std::string(std::strerror(errno)));
    }
}

void EventLoop::DoPendingFunctors()
{
    // 执行期间新任务进入 pending，留到下一批处理。
    calling_pending_functors_ = true;

    local_executing_functors_.swap(local_pending_functors_);

    {
        std::lock_guard<std::mutex> lock(remote_mutex_);
        remote_executing_functors_.swap(remote_pending_functors_);
    }

    for (auto& functor : local_executing_functors_)
    {
        if (functor)
        {
            functor();
        }
    }

    for (auto& functor : remote_executing_functors_)
    {
        if (functor)
        {
            functor();
        }
    }

    calling_pending_functors_ = false;

    // executing 只清元素不释放容量，与 pending 反复交换后两块缓冲都能长期复用
    local_executing_functors_.clear();
    remote_executing_functors_.clear();
}

}  // namespace nebula::net