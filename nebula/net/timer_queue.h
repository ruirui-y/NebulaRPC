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
// 定时器存在可复用槽位里，heap 按 deadline 只排槽位下标；Cancel 是 lazy 的，靠 stale wakeup 重新武装。
class TimerQueue final : private base::Noncopyable
{
public:
    using Clock = std::chrono::steady_clock;                                    // 单调时钟，不受系统时间跳变影响
    using TimePoint = Clock::time_point;
    using Functor = std::function<void()>;

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerId AddTimer(TimePoint deadline, Functor callback);                     // 通用路径：回调是 std::function，会分配
    TimerId AddTimer(TimePoint deadline, TimerNotify notify);                   // 热路径：固定布局通知，不构造 std::function
    void Cancel(TimerId timer_id);                                              // lazy：只摘用户态记录，不重设 timerfd

private:
    using TimerAction = std::variant<Functor, TimerNotify>;                     // 二选一：热路径不构造 std::function

    // TimerId 布局（64 bit）：bit63 = 跨线程标记，bit62..31 = generation，bit30..0 = slot_index + 1。
    // 下标 +1 让 0 成为无效哨兵；generation 非 0 让槽位复用后的旧 id 自然失效。
    static constexpr std::uint32_t kInvalidSlot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::uint64_t kExternalTimerMask = std::uint64_t{1} << 63;
    static constexpr std::uint64_t kSlotIndexMask = (std::uint64_t{1} << 31) - 1;
    static constexpr std::uint64_t kGenerationMask = 0xffffffffULL;

    // 同一槽位兼作堆节点与空闲链表节点：heap_index 与 next_free 不会同时有效。
    struct TimerSlot
    {
        TimePoint deadline{};
        TimerAction action{};
        std::uint64_t external_timer_id{0};                                     // 仅跨线程兼容路径使用
        std::uint64_t sequence{0};                                              // deadline 相同时保持注册顺序
        std::uint32_t heap_index{kInvalidSlot};                                 // 在 heap_ 中的下标，kInvalidSlot 表示不在堆内
        std::uint32_t next_free{kInvalidSlot};                                  // 空闲链指针，仅 active == false 时有意义
        std::uint32_t generation{0};                                            // 每次复用 +1 且跳过 0，旧 TimerId 靠它失效
        bool active{false};
    };

    TimerId AddTimerInLoop(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id = 0);
    void CancelInLoop(TimerId timer_id);
    void HandleRead();
    void ResetTimerFd();

    // 三个动作分工：取槽位（优先复用 free list）、归还槽位、从堆与外部映射摘除。
    std::uint32_t AllocateSlot(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id);
    void ReleaseSlot(std::uint32_t slot_index);
    void RemoveTimerSlot(std::uint32_t slot_index);

    void PushHeap(std::uint32_t slot_index);
    void RemoveHeapAt(std::uint32_t heap_index);
    void SiftUp(std::uint32_t heap_index);
    void SiftDown(std::uint32_t heap_index);
    void SwapHeapEntries(std::uint32_t lhs, std::uint32_t rhs);

    // deadline 小者优先；相等时比 sequence，保证同 deadline 按注册顺序触发。
    [[nodiscard]] bool Earlier(std::uint32_t lhs_slot, std::uint32_t rhs_slot) const;

    [[nodiscard]] TimerId MakeSlotTimerId(std::uint32_t slot_index, std::uint32_t generation) const;
    [[nodiscard]] TimerId MakeExternalTimerId();                                // 跨线程冷路径 id：高位置标记，与槽位 id 区分
    [[nodiscard]] bool DecodeSlotTimerId(TimerId timer_id, std::uint32_t* slot_index, std::uint32_t* generation) const;
    [[nodiscard]] static bool IsExternalTimerId(TimerId timer_id) noexcept;

    static void InvokeAction(TimerAction action);

    EventLoop* loop_;
    int timer_fd_;
    std::unique_ptr<Channel> timer_channel_;

    // owner loop 热路径：Timer 存在可复用 slot 中，heap 只保存 slot 下标。
    std::vector<TimerSlot> slots_;
    std::vector<std::uint32_t> heap_;
    std::uint32_t free_slot_head_{kInvalidSlot};                                // 空闲槽链表头，kInvalidSlot 表示没有空闲槽
    std::uint64_t next_sequence_{1};                                            // deadline 相同时的注册顺序

    // 跨线程 AddTimer 必须在返回前先生成可用 TimerId，因此单独保留冷路径映射。
    // RPC timeout 热路径始终在 owner loop，不会进入这里，也不会产生节点分配。
    std::atomic_uint64_t next_external_timer_id_{1};
    std::unordered_map<std::uint64_t, std::uint32_t> external_timer_slots_;
};

}  // namespace nebula::net
