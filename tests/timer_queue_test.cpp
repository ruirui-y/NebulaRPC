#include "nebula/net/event_loop.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <thread>

namespace
{

struct NotifyState
{
    bool fired{false};
    std::uint64_t token{0};
};

void OnTimerNotify(void* owner, std::uint64_t token)
{
    auto* state = static_cast<NotifyState*>(owner);
    state->fired = true;
    state->token = token;
}

bool expired_notify_fired = false;

void OnExpiredTimerNotify(void*, std::uint64_t)
{
    expired_notify_fired = true;
}

}  // namespace

int main()
{
    using namespace std::chrono_literals;

    nebula::net::EventLoop loop;
    bool fired = false;
    bool canceled_fired = false;
    bool passed = false;
    std::atomic_bool cross_thread_canceled_fired{false};
    auto notify_state = std::make_shared<NotifyState>();

    const auto canceled = loop.RunAfter(20ms, [&]
        {
            canceled_fired = true;
        });
    loop.CancelTimer(canceled);

    loop.RunAfter(40ms, [&]
        {
            fired = true;
        });

    // canceled 对应的 slot 已经释放并被后续 timer 复用；旧 TimerId 不能误取消新 timer。
    loop.CancelTimer(canceled);

    // 跨线程 Add/Cancel 保留原有兼容语义：真正修改 TimerQueue 的动作仍回到 owner loop。
    std::thread cross_thread(
        [&]
        {
            const auto cross_timer = loop.RunAfter(30ms, [&]
                {
                    cross_thread_canceled_fired.store(true, std::memory_order_release);
                });
            loop.CancelTimer(cross_timer);
        });
    cross_thread.join();

    loop.RunAt(
        nebula::net::EventLoop::Clock::now() + 60ms,
        nebula::net::TimerNotify{
            .owner = std::weak_ptr<void>(notify_state),
            .handler = &OnTimerNotify,
            .token = 42,
        });

    auto expired_owner = std::make_shared<int>(0);
    loop.RunAt(
        nebula::net::EventLoop::Clock::now() + 70ms,
        nebula::net::TimerNotify{
            .owner = std::weak_ptr<void>(expired_owner),
            .handler = &OnExpiredTimerNotify,
            .token = 7,
        });
    expired_owner.reset();

    loop.RunAfter(100ms, [&]
        {
            passed = fired &&
                     !canceled_fired &&
                     !cross_thread_canceled_fired.load(std::memory_order_acquire) &&
                     notify_state->fired &&
                     notify_state->token == 42 &&
                     !expired_notify_fired;
            loop.Quit();
        });

    loop.Loop();

    if (!passed)
    {
        std::cerr << "timer queue test failed\n";
        return 1;
    }

    std::cout << "timer queue test passed\n";
    return 0;
}
