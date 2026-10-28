#include "nebula/net/event_loop.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

namespace
{

    using Clock = std::chrono::steady_clock;
    using namespace std::chrono_literals;

}  // namespace

int main()
{
    nebula::net::EventLoop loop;

    // ------------------------------------------------------------
    // 验证目标：
    //
    // T1 = 100ms，是当前最早到期的 timer。
    // T2 = 300ms，是真正需要触发的 timer。
    //
    // 在队列同时持有 T1、T2 时取消 T1。
    //
    // 期望行为：
    //
    //   被取消的 T1 绝不能执行 callback；
    //   取消 T1 不能让 T2 丢失 —— T2 仍必须在 300ms 附近触发。
    //
    // 内部用什么结构推进到期判断不在本测试范围内，
    // 只要"取消一个 timer 不影响其他 timer"这条外部行为成立即可。
    // ------------------------------------------------------------

    const auto started = Clock::now();

    bool cancelled_timer_fired = false;
    bool real_timer_fired = false;
    Clock::time_point real_timer_fired_at{};

    std::atomic_bool test_finished{ false };
    std::atomic_bool watchdog_timeout{ false };

    std::mutex watchdog_mutex;
    std::condition_variable watchdog_cv;

    // ------------------------------------------------------------
    // 外部 watchdog 不能使用 TimerQueue 自己实现。
    //
    // 因为我们测试的正是 TimerQueue：
    // 如果取消之后队列不再推进，再注册一个 TimerQueue watchdog
    // 也同样可能永远不会触发。
    //
    // 因此这里使用独立 OS 线程，在 1500ms 后强制退出 EventLoop。
    // ------------------------------------------------------------
    std::thread watchdog(
        [&]
        {
            std::unique_lock<std::mutex> lock(watchdog_mutex);

            const bool finished = watchdog_cv.wait_for(
                lock,
                1500ms,
                [&]
                {
                    return test_finished.load(std::memory_order_acquire);
                });

            if (!finished)
            {
                watchdog_timeout.store(true, std::memory_order_release);
                loop.Quit();
            }
        });

    // ------------------------------------------------------------
    // T1：100ms，作为当前最早到期的 timer 注册。
    // ------------------------------------------------------------
    const auto cancelled_timer = loop.RunAt(
        started + 100ms,
        [&]
        {
            cancelled_timer_fired = true;
        });

    // ------------------------------------------------------------
    // T2：300ms。
    // ------------------------------------------------------------
    loop.RunAt(
        started + 300ms,
        [&]
        {
            real_timer_fired = true;
            real_timer_fired_at = Clock::now();

            test_finished.store(true, std::memory_order_release);
            watchdog_cv.notify_one();

            loop.Quit();
        });

    // ------------------------------------------------------------
    // 关键操作：取消最早到期的 T1。
    //
    // 此后队列里只剩 T2，T2 的触发不能被这次取消影响。
    // ------------------------------------------------------------
    loop.CancelTimer(cancelled_timer);

    loop.Loop();

    // 无论测试是正常结束还是 watchdog 强制退出，
    // 都通知 watchdog 线程退出。
    test_finished.store(true, std::memory_order_release);
    watchdog_cv.notify_one();

    watchdog.join();

    // ------------------------------------------------------------
    // 检查 1：
    // 被取消的 T1 绝对不能执行 callback。
    // ------------------------------------------------------------
    if (cancelled_timer_fired)
    {
        std::cerr
            << "[FAIL] cancelled timer callback was executed\n";
        return 1;
    }

    // ------------------------------------------------------------
    // 检查 2：
    // 如果 watchdog 超时，说明取消最早到期的 timer 之后
    // 队列不再推进到后续 timer，T2 被丢失。
    // ------------------------------------------------------------
    if (watchdog_timeout.load(std::memory_order_acquire))
    {
        std::cerr
            << "[FAIL] watchdog timeout: "
            << "later timer was lost after cancelling the earliest one\n";
        return 2;
    }

    // ------------------------------------------------------------
    // 检查 3：
    // 真正的 T2 必须执行。
    // ------------------------------------------------------------
    if (!real_timer_fired)
    {
        std::cerr
            << "[FAIL] real timer did not fire\n";
        return 3;
    }

    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            real_timer_fired_at - started);

    // ------------------------------------------------------------
    // 检查 4：
    // T2 目标是 300ms。
    //
    // 下限给 200ms：
    // 如果取消操作把后续 timer 的到期时间算错、导致过早触发，这里能够发现。
    //
    // 上限给 1000ms：
    // 给虚拟机调度留下较大容差，但不能无限拖延。
    // ------------------------------------------------------------
    if (elapsed < 200ms)
    {
        std::cerr
            << "[FAIL] real timer fired too early: "
            << elapsed.count()
            << " ms\n";
        return 4;
    }

    if (elapsed > 1000ms)
    {
        std::cerr
            << "[FAIL] real timer fired too late: "
            << elapsed.count()
            << " ms\n";
        return 5;
    }

    std::cout
        << "[PASS] cancelling the earliest timer keeps later timers alive\n"
        << "  cancelled timer fired : no\n"
        << "  later timer fired     : yes\n"
        << "  later timer elapsed   : "
        << elapsed.count()
        << " ms\n";

    return 0;
}
