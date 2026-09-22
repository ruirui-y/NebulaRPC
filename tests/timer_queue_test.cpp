#include "nebula/net/event_loop.h"

#include <chrono>
#include <iostream>

int main()
{
    using namespace std::chrono_literals;

    nebula::net::EventLoop loop;
    bool fired = false;
    bool canceled_fired = false;
    bool passed = false;

    const auto canceled = loop.RunAfter(20ms, [&]
        {
            canceled_fired = true;
        });
    loop.CancelTimer(canceled);

    loop.RunAfter(40ms, [&]
        {
            fired = true;
        });

    loop.RunAfter(100ms, [&]
        {
            passed = fired && !canceled_fired;
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
