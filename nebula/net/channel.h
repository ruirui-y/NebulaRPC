#pragma once

#include <functional>

namespace nebula::net
{

class EventLoop;

class Channel
{
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd) noexcept;

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    // Stage 1 / NRPC-S1-01：后续在这里完成可读事件分发。
    void HandleEvent();

    void SetReadCallback(EventCallback callback);

    [[nodiscard]] int fd() const noexcept { return fd_; }

private:
    EventLoop* loop_{nullptr};
    const int fd_{-1};
    EventCallback read_callback_;
};

} // namespace nebula::net
