#include "nebula/net/event_loop.h"

namespace nebula::net
{

EventLoop::EventLoop()
    : thread_id_(std::this_thread::get_id())
{
}

void EventLoop::Loop()
{
    // TODO(NRPC-S1-01):
    // 1. 创建/接入 Poller
    // 2. epoll_wait 获取 active Channels
    // 3. 遍历 Channel::HandleEvent()
}

void EventLoop::Quit() noexcept
{
    quit_.store(true, std::memory_order_relaxed);
}

bool EventLoop::IsInLoopThread() const noexcept
{
    return thread_id_ == std::this_thread::get_id();
}

} // namespace nebula::net
