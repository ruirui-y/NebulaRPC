#pragma once

#include <atomic>
#include <thread>

namespace nebula::net
{

class EventLoop
{
public:
    EventLoop();
    ~EventLoop() = default;

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    // Stage 1 / NRPC-S1-01：这里开始实现 epoll_wait -> Channel::HandleEvent。
    void Loop();
    void Quit() noexcept;

    [[nodiscard]] bool IsInLoopThread() const noexcept;

private:
    std::atomic_bool quit_{false};
    const std::thread::id thread_id_;
};

} // namespace nebula::net
