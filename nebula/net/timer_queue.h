#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"
#include "nebula/net/timer_notify.h"

#include <array>
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

// 定时器队列：timerfd 以 1ms tick 驱动四级时间轮，定时回调统一在本 loop 线程执行。
// TimerSlot 既是时间轮侵入式链表节点也是 free list 节点，取消与槽位复用都不产生节点分配。
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
    void Cancel(TimerId timer_id);                                              // owner loop 内 O(1) 从 bucket 摘除并立即复用 slot

private:
    using TimerAction = std::variant<Functor, TimerNotify>;                     // 二选一：热路径不构造 std::function

    // TimerId 布局（64 bit）：bit63 = 跨线程标记，bit62..31 = generation，bit30..0 = slot_index + 1。
    // 下标 +1 让 0 成为无效哨兵；generation 非 0 让槽位复用后的旧 id 自然失效。
    static constexpr std::uint32_t kInvalidSlot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::uint64_t kExternalTimerMask = std::uint64_t{1} << 63;
    static constexpr std::uint64_t kSlotIndexMask = (std::uint64_t{1} << 31) - 1;
    static constexpr std::uint64_t kGenerationMask = 0xffffffffULL;

    static constexpr std::uint8_t kWheelLevelCount = 4;
    static constexpr std::uint8_t kWheelSize = 10;
    static constexpr std::uint8_t kInvalidWheelLevel = std::numeric_limits<std::uint8_t>::max();
    static constexpr std::uint8_t kInvalidBucket = std::numeric_limits<std::uint8_t>::max();
    static constexpr std::array<std::uint64_t, kWheelLevelCount> kWheelQuantumTicks{1, 10, 100, 1000};

    struct TimerBucket
    {
        std::uint32_t head{kInvalidSlot};                                       // bucket 内侵入式双向链表头
        std::uint32_t tail{kInvalidSlot};                                       // 尾插保持同 tick timer 的注册顺序
    };

    // Active 时 prev/next 属于 wheel bucket；Free 时 next_free 属于 free list，两种链不会同时有效。
    struct TimerSlot
    {
        TimePoint deadline{};
        TimerAction action{};
        std::uint64_t external_timer_id{0};                                     // 仅跨线程兼容路径使用
        std::uint64_t wheel_rounds{0};                                          // 仅 L3 使用，支持超过 10s 的 deadline
        std::uint32_t wheel_prev{kInvalidSlot};                                 // 当前 bucket 前驱，头节点为 kInvalidSlot
        std::uint32_t wheel_next{kInvalidSlot};                                 // 当前 bucket 后继，尾节点为 kInvalidSlot
        std::uint32_t next_free{kInvalidSlot};                                  // 空闲链指针，仅 active == false 时有意义
        std::uint32_t generation{0};                                            // 每次复用 +1 且跳过 0，旧 TimerId 靠它失效
        std::uint8_t wheel_level{kInvalidWheelLevel};                           // 0..3，kInvalidWheelLevel 表示当前未挂入时间轮
        std::uint8_t bucket_index{kInvalidBucket};                              // 当前层 0..9 的 bucket 下标
        bool active{false};
    };

    TimerId AddTimerInLoop(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id = 0);
    void CancelInLoop(TimerId timer_id);
    void HandleRead();
    void ArmTimerFd();
    void DisarmTimerFd();
    void SyncCurrentTick();

    // 三个动作分工：取槽位（优先复用 free list）、归还槽位、从时间轮与外部映射摘除。
    std::uint32_t AllocateSlot(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id);
    void ReleaseSlot(std::uint32_t slot_index);
    void RemoveTimerSlot(std::uint32_t slot_index);

    void ScheduleTimerSlot(std::uint32_t slot_index);
    void PlaceTimerSlot(std::uint32_t slot_index, std::uint64_t expiry_tick);
    void LinkTimerSlot(std::uint32_t slot_index, std::uint8_t wheel_level, std::uint8_t bucket_index);
    void UnlinkTimerSlot(std::uint32_t slot_index);
    void AdvanceWheel(std::uint64_t expirations);
    void CascadeLevel(std::uint8_t wheel_level);
    void ExpireCurrentBucket();

    [[nodiscard]] std::uint64_t DeadlineTick(TimePoint deadline) const;
    [[nodiscard]] TimerId MakeSlotTimerId(std::uint32_t slot_index, std::uint32_t generation) const;
    [[nodiscard]] TimerId MakeExternalTimerId();                                // 跨线程冷路径 id：高位置标记，与槽位 id 区分
    [[nodiscard]] bool DecodeSlotTimerId(TimerId timer_id, std::uint32_t* slot_index, std::uint32_t* generation) const;
    [[nodiscard]] static bool IsExternalTimerId(TimerId timer_id) noexcept;

    static void InvokeAction(TimerAction action);

    EventLoop* loop_;
    int timer_fd_;
    std::unique_ptr<Channel> timer_channel_;

    // owner loop 热路径：40 个 bucket 只挂 slot 下标，TimerSlot 与 free list 都复用 slots_ 内存。
    std::vector<TimerSlot> slots_;
    std::array<std::array<TimerBucket, kWheelSize>, kWheelLevelCount> wheel_{};
    std::uint32_t free_slot_head_{kInvalidSlot};                                // 空闲槽链表头，kInvalidSlot 表示没有空闲槽
    std::uint64_t active_timer_count_{0};                                       // 0->1 时 arm，1->0 时 disarm timerfd
    TimePoint wheel_epoch_{Clock::now()};                                       // 所有 1ms tick 都相对同一单调时钟起点计算
    std::uint64_t current_tick_{0};                                             // 已处理完成的 1ms tick 序号
    bool timer_fd_armed_{false};                                                // 周期 timerfd 是否处于 1ms armed 状态

    // 跨线程 AddTimer 必须在返回前先生成可用 TimerId，因此单独保留冷路径映射。
    // RPC timeout 热路径始终在 owner loop，不会进入这里，也不会产生节点分配。
    std::atomic_uint64_t next_external_timer_id_{1};
    std::unordered_map<std::uint64_t, std::uint32_t> external_timer_slots_;
};

}  // namespace nebula::net
