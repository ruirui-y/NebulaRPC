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
    // 测试目标：
    //
    // T1 = 100ms，作为最早 timer，使 kernel timerfd 首先武装到 T1。
    // T2 = 300ms，作为真正需要触发的 timer。
    //
    // 随后立即取消 T1。
    //
    // lazy cancel 实现不会在 Cancel(T1) 时重新调用 timerfd_settime，
    // 因此 kernel timerfd 仍然会在约 100ms 时产生一次 stale wakeup。
    //
    // 正确行为应该是：
    //
    //   100ms:
    //       timerfd 唤醒
    //       T1 已取消，不执行 T1 callback
    //       T2 尚未到期，不执行 T2 callback
    //       HandleRead() 最后重新把 timerfd 武装到 T2
    //
    //   300ms:
    //       T2 正常触发
    //
    // 如果 stale wakeup 后没有正确重新武装 timerfd，
    // 那么 T2 将永远不会触发，外部 watchdog 会使测试失败。
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
    // 如果 timerfd 在 stale wakeup 后没有重新武装，
    // 再注册一个 TimerQueue watchdog 也同样可能永远不会触发。
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
    // T1：100ms。
    //
    // 因为当前 TimerQueue 为空，所以加入 T1 时，
    // AddTimerInLoop 会调用 ResetTimerFd，
    // kernel timerfd 被武装到 started + 100ms。
    // ------------------------------------------------------------
    const auto cancelled_timer = loop.RunAt(
        started + 100ms,
        [&]
        {
            cancelled_timer_fired = true;
        });

    // ------------------------------------------------------------
    // T2：300ms。
    //
    // T1 此时仍然是队首，因此加入 T2 不需要重新设置 timerfd。
    // kernel timerfd 仍然指向 100ms。
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
    // 关键操作：
    //
    // 删除当前最早的 T1。
    //
    // lazy cancel 下这里只删除用户态 TimerQueue 记录，
    // 不重新调用 timerfd_settime。
    //
    // 因此：
    //
    //   deadlines_   -> 只剩 T2 = 300ms
    //   kernel timerfd -> 仍然是 T1 = 100ms
    //
    // 这正是我们要人为制造的 stale wakeup。
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
    // 如果 watchdog 超时，说明：
    //
    // stale timerfd 在 100ms 唤醒以后，
    // HandleRead 没有把 timerfd 重新武装到 T2，
    // 导致真正的 300ms timer 丢失。
    // ------------------------------------------------------------
    if (watchdog_timeout.load(std::memory_order_acquire))
    {
        std::cerr
            << "[FAIL] watchdog timeout: "
            << "timerfd was not rearmed after stale wakeup\n";
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
    // 如果 stale wakeup 在约 100ms 时错误地触发了 T2，
    // 这里能够发现。
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
        << "[PASS] lazy timer cancellation is correct\n"
        << "  cancelled timer fired : no\n"
        << "  stale wakeup           : survived\n"
        << "  real timer fired       : yes\n"
        << "  real timer elapsed     : "
        << elapsed.count()
        << " ms\n";

    return 0;
}