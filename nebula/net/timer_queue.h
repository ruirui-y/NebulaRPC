#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"
#include "nebula/net/timer_notify.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <unordered_map>
#include <variant>
#include <vector>

namespace nebula::net
{

class Channel;
class EventLoop;

// 定时器队列：timerfd 接入 EventLoop 的 epoll，定时回调统一在本 loop 线程执行。
class TimerQueue final : private base::Noncopyable
{
public:
    using Clock = std::chrono::steady_clock;                        // 单调时钟，不受系统时间跳变影响
    using TimePoint = Clock::time_point;
    using Functor = std::function<void()>;

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerId AddTimer(TimePoint deadline, Functor callback);
    TimerId AddTimer(TimePoint deadline, TimerNotify notify);
    void Cancel(TimerId timer_id);

private:
    using TimerAction = std::variant<Functor, TimerNotify>;

    static constexpr std::uint32_t kInvalidSlot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::uint64_t kExternalTimerMask = std::uint64_t{1} << 63;
    static constexpr std::uint64_t kSlotIndexMask = (std::uint64_t{1} << 31) - 1;
    static constexpr std::uint64_t kGenerationMask = 0xffffffffULL;

    struct TimerSlot
    {
        TimePoint deadline{};
        TimerAction action{};
        std::uint64_t external_timer_id{0};                         // 仅跨线程兼容路径使用
        std::uint64_t sequence{0};                                  // deadline 相同时保持注册顺序
        std::uint32_t heap_index{kInvalidSlot};
        std::uint32_t next_free{kInvalidSlot};
        std::uint32_t generation{0};
        bool active{false};
    };

    TimerId AddTimerInLoop(TimePoint deadline,
                           TimerAction action,
                           std::uint64_t external_timer_id = 0);
    void CancelInLoop(TimerId timer_id);
    void HandleRead();
    void ResetTimerFd();

    std::uint32_t AllocateSlot(TimePoint deadline,
                               TimerAction action,
                               std::uint64_t external_timer_id);
    void ReleaseSlot(std::uint32_t slot_index);
    void RemoveTimerSlot(std::uint32_t slot_index);

    void PushHeap(std::uint32_t slot_index);
    void RemoveHeapAt(std::uint32_t heap_index);
    void SiftUp(std::uint32_t heap_index);
    void SiftDown(std::uint32_t heap_index);
    void SwapHeapEntries(std::uint32_t lhs, std::uint32_t rhs);
    [[nodiscard]] bool Earlier(std::uint32_t lhs_slot,
                               std::uint32_t rhs_slot) const;

    [[nodiscard]] TimerId MakeSlotTimerId(std::uint32_t slot_index,
                                          std::uint32_t generation) const;
    [[nodiscard]] TimerId MakeExternalTimerId();
    [[nodiscard]] bool DecodeSlotTimerId(TimerId timer_id,
                                         std::uint32_t* slot_index,
                                         std::uint32_t* generation) const;
    [[nodiscard]] static bool IsExternalTimerId(TimerId timer_id) noexcept;

    static void InvokeAction(TimerAction action);

    EventLoop* loop_;
    int timer_fd_;
    std::unique_ptr<Channel> timer_channel_;

    // owner loop 热路径：Timer 存在可复用 slot 中，heap 只保存 slot 下标。
    std::vector<TimerSlot> slots_;
    std::vector<std::uint32_t> heap_;
    std::uint32_t free_slot_head_{kInvalidSlot};
    std::uint64_t next_sequence_{1};

    // 跨线程 AddTimer 必须在返回前先生成可用 TimerId，因此单独保留冷路径映射。
    // RPC timeout 热路径始终在 owner loop，不会进入这里，也不会产生节点分配。
    std::atomic_uint64_t next_external_timer_id_{1};
    std::unordered_map<std::uint64_t, std::uint32_t> external_timer_slots_;
};

}  // namespace nebula::net
