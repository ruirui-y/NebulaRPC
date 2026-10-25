#include "nebula/net/event_loop.h"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>

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