#pragma once

#include "nebula/base/noncopyable.h"

#include <cstdint>
#include <functional>
#include <memory>

namespace nebula::net
{

class EventLoop;

class Channel final : private base::Noncopyable
{
public:
    using EventCallback = std::function<void()>;

    Channel(EventLoop* loop, int fd);
    ~Channel() = default;

    void HandleEvent();
    void Tie(const std::shared_ptr<void>& owner);

    void SetReadCallback(EventCallback cb)
    {
        read_callback_ = std::move(cb);
    }
    void SetWriteCallback(EventCallback cb)
    {
        write_callback_ = std::move(cb);
    }
    void SetCloseCallback(EventCallback cb)
    {
        close_callback_ = std::move(cb);
    }
    void SetErrorCallback(EventCallback cb)
    {
        error_callback_ = std::move(cb);
    }

    [[nodiscard]] int Fd() const noexcept
    {
        return fd_;
    }
    [[nodiscard]] std::uint32_t Events() const noexcept
    {
        return events_;
    }
    void SetRevents(std::uint32_t revents) noexcept
    {
        revents_ = revents;
    }

    [[nodiscard]] bool IsNoneEvent() const noexcept
    {
        return events_ == kNoneEvent;
    }
    [[nodiscard]] bool IsWriting() const noexcept
    {
        return (events_ & kWriteEvent) != 0U;
    }
    [[nodiscard]] bool IsReading() const noexcept
    {
        return (events_ & kReadEvent) != 0U;
    }

    void EnableReading();
    void DisableReading();
    void EnableWriting();
    void DisableWriting();
    void DisableAll();
    void Remove();

    [[nodiscard]] int Index() const noexcept
    {
        return index_;
    }
    void SetIndex(int index) noexcept
    {
        index_ = index;
    }
    [[nodiscard]] EventLoop* OwnerLoop() const noexcept
    {
        return loop_;
    }

private:
    void Update();
    void HandleEventWithGuard();

    static constexpr std::uint32_t kNoneEvent = 0U;
    static const std::uint32_t kReadEvent;
    static const std::uint32_t kWriteEvent;

    EventLoop* loop_;
    const int fd_;
    std::uint32_t events_{0};
    std::uint32_t revents_{0};
    int index_{-1};

    std::weak_ptr<void> tie_;
    bool tied_{false};

    EventCallback read_callback_;
    EventCallback write_callback_;
    EventCallback close_callback_;
    EventCallback error_callback_;
};

}  // namespace nebula::net
