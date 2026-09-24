#include "nebula/rpc/rpc_call.h"

#include <atomic>
#include <iostream>
#include <barrier>
#include <thread>
#include <vector>

namespace
{

// 单线程顺序验证：只有第一次 TryComplete 成功，状态定格在第一个赢家
bool TestSingleThread()
{
    nebula::rpc::RpcCall call;

    if (call.State() != nebula::rpc::RpcCallState::Pending)
    {
        std::cerr << "initial state is not Pending\n";
        return false;
    }

    // ---- 第一个完成来源赢得完成权 ----
    if (!call.TryComplete(nebula::rpc::RpcCallState::Completed))
    {
        std::cerr << "first TryComplete should win\n";
        return false;
    }

    // ---- 后续所有完成来源都必须失败 ----
    if (call.TryComplete(nebula::rpc::RpcCallState::Timeout))
    {
        std::cerr << "second TryComplete should lose\n";
        return false;
    }

    if (call.TryComplete(nebula::rpc::RpcCallState::Cancelled))
    {
        std::cerr << "third TryComplete should lose\n";
        return false;
    }

    if (call.State() != nebula::rpc::RpcCallState::Completed)
    {
        std::cerr << "final state should be Completed\n";
        return false;
    }

    return true;
}

// 多线程恰好一次：线程常驻 + barrier 分轮，每轮 N 线程真正同时抢（churn = kThreads）
bool TestConcurrentRace()
{
    constexpr int kThreads = 8;
    constexpr int kRounds = 10000;

    std::atomic<nebula::rpc::RpcCall*> current{ nullptr };
    std::atomic_int  winners{ 0 };
    std::atomic_int  winner_state{ 0 };
    std::barrier     sync{ kThreads + 1 };      // 主线程也算一方

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i)
    {
        threads.emplace_back([&, i]
            {
                const auto target = (i % 2 == 0)
                    ? nebula::rpc::RpcCallState::Completed
                    : nebula::rpc::RpcCallState::Timeout;
                for (int round = 0; round < kRounds; ++round)
                {
                    sync.arrive_and_wait();                  // 等主线程发布本轮 call
                    if (auto* c = current.load(); c != nullptr && c->TryComplete(target))
                    {
                        winners.fetch_add(1);
                        winner_state.store(static_cast<int>(target));
                    }
                    sync.arrive_and_wait();                  // 等全员收工
                }
            });
    }

    bool ok = true;
    for (int round = 0; round < kRounds; ++round)         // 不 break，保证 barrier 配对不失衡
    {
        nebula::rpc::RpcCall call;                       // 每轮全新对象
        current.store(&call);
        winners.store(0);

        sync.arrive_and_wait();                          // 放行：8 个线程同时抢
        sync.arrive_and_wait();                          // 收工

        const int  w = winners.load();
        const auto final_state = call.State();
        current.store(nullptr);

        if (w != 1)
        {
            std::cerr << "race round " << round << " winners=" << w << "\n";
            ok = false;
        }
        // 新增：赢家写的状态必须等于最终状态（原来这条没验，写错状态的 bug 会漏检）
        else if (final_state != static_cast<nebula::rpc::RpcCallState>(winner_state.load()))
        {
            std::cerr << "race round " << round << " final state mismatch\n";
            ok = false;
        }
    }

    for (auto& t : threads) { t.join(); }
    return ok;
}

}  // namespace

int main()
{
    if (!TestSingleThread())
    {
        std::cerr << "rpc_call single thread test failed\n";
        return 1;
    }

    std::cout << "rpc_call single thread test passed\n";

    if (!TestConcurrentRace())
    {
        std::cerr << "rpc_call concurrent race test failed\n";
        return 1;
    }

    std::cout << "rpc_call exactly-once race test passed\n";
    return 0;
}
