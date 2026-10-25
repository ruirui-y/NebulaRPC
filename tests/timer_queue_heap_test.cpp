#include "nebula/net/event_loop.h"

#include <chrono>
#include <iostream>
#include <vector>

int main()
{
    using namespace std::chrono_literals;

    nebula::net::EventLoop loop;
    const auto now = nebula::net::EventLoop::Clock::now();
    std::vector<int> fired_order;

    loop.RunAt(now + 50ms, [&] { fired_order.push_back(50); });
    const auto cancel_30 = loop.RunAt(now + 30ms, [&] { fired_order.push_back(30); });
    loop.RunAt(now + 20ms, [&] { fired_order.push_back(20); });
    const auto cancel_70 = loop.RunAt(now + 70ms, [&] { fired_order.push_back(70); });
    loop.RunAt(now + 40ms, [&] { fired_order.push_back(40); });

    // 从 heap 中间和后部删除 timer，剩余 timer 仍必须保持最小堆顺序。
    loop.CancelTimer(cancel_30);
    loop.CancelTimer(cancel_70);

    // 相同 deadline 保持注册顺序，与原 multimap 行为一致。
    const auto same_deadline = now + 60ms;
    loop.RunAt(same_deadline, [&] { fired_order.push_back(61); });
    loop.RunAt(same_deadline, [&] { fired_order.push_back(62); });
    loop.RunAt(same_deadline, [&] { fired_order.push_back(63); });

    loop.RunAt(now + 100ms, [&] { loop.Quit(); });
    loop.Loop();

    const std::vector<int> expected{20, 40, 50, 61, 62, 63};

    if (fired_order != expected)
    {
        std::cerr << "timer heap order test failed:";

        for (const int value : fired_order)
        {
            std::cerr << ' ' << value;
        }

        std::cerr << '\n';
        return 1;
    }

    std::cout << "timer heap order test passed\n";
    return 0;
}
