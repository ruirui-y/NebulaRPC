#pragma once

#include "nebula/net/event_loop.h"
#include "nebula/net/timer_id.h"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <memory>

namespace nebula::net
{

class SleepAwaiter final
{
public:
    SleepAwaiter(EventLoop* loop, std::chrono::milliseconds delay)
        : loop_(loop),
          delay_(delay)
    {
    }

    SleepAwaiter(const SleepAwaiter&) = delete;
    SleepAwaiter& operator=(const SleepAwaiter&) = delete;

    ~SleepAwaiter()
    {
        Cancel();
    }

    bool await_ready() const noexcept
    {
        return delay_.count() <= 0;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        loop_->AssertInLoopThread();

        state_ = std::make_shared<State>();
        state_->loop = loop_;
        state_->handle.store(handle, std::memory_order_release);

        const std::weak_ptr<State> weak_state = state_;
        state_->timer = loop_->RunAfter(delay_, [weak_state]
            {
                const auto state = weak_state.lock();
                if (state == nullptr)
                {
                    return;
                }

                EventLoop* loop = state->loop;
                loop->QueueInLoop([state]
                    {
                        if (const std::coroutine_handle<> resumed =
                                state->handle.exchange(nullptr, std::memory_order_acq_rel);
                            resumed != nullptr)
                        {
                            resumed.resume();
                        }
                    });
            });
    }

    void await_resume() noexcept
    {
    }

private:
    struct State
    {
        EventLoop* loop{};
        TimerId timer{};
        std::atomic<std::coroutine_handle<>> handle{};
    };

    void Cancel() noexcept
    {
        if (state_ == nullptr)
        {
            return;
        }

        const std::coroutine_handle<> pending =
            state_->handle.exchange(nullptr, std::memory_order_acq_rel);

        if (pending != nullptr && state_->timer.Valid() && state_->loop->IsInLoopThread())
        {
            state_->loop->CancelTimer(state_->timer);
        }

        state_.reset();
    }

    EventLoop* loop_;
    std::chrono::milliseconds delay_;
    std::shared_ptr<State> state_;
};

[[nodiscard]] inline SleepAwaiter SleepFor(EventLoop* loop,
                                           std::chrono::milliseconds delay)
{
    return SleepAwaiter(loop, delay);
}

class YieldAwaiter final
{
public:
    explicit YieldAwaiter(EventLoop* loop)
        : loop_(loop)
    {
    }

    YieldAwaiter(const YieldAwaiter&) = delete;
    YieldAwaiter& operator=(const YieldAwaiter&) = delete;

    ~YieldAwaiter()
    {
        if (state_ != nullptr)
        {
            state_->handle.exchange(nullptr, std::memory_order_acq_rel);
        }
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        loop_->AssertInLoopThread();

        state_ = std::make_shared<State>();
        state_->handle.store(handle, std::memory_order_release);

        const auto state = state_;
        loop_->QueueInLoop([state]
            {
                if (const std::coroutine_handle<> resumed =
                        state->handle.exchange(nullptr, std::memory_order_acq_rel);
                    resumed != nullptr)
                {
                    resumed.resume();
                }
            });
    }

    void await_resume() const noexcept
    {
    }

private:
    struct State
    {
        std::atomic<std::coroutine_handle<>> handle{};
    };

    EventLoop* loop_;
    std::shared_ptr<State> state_;
};

[[nodiscard]] inline YieldAwaiter Yield(EventLoop* loop)
{
    return YieldAwaiter(loop);
}

}  // namespace nebula::net
