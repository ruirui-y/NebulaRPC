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

// TimerQueue 管一组定时器：Linux timerfd 每 1ms 提醒一次 EventLoop，时间轮据此判断哪些 timer 到期。
// TimerSlot 用完后不会释放，而是放回空闲链表复用，避免 Add/Cancel 时反复创建和销毁节点。
class TimerQueue final : private base::Noncopyable
{
public:
    using Clock = std::chrono::steady_clock;                                                                    // 只会向前走的时钟；系统改时间不会影响 timeout
    using TimePoint = Clock::time_point;                                                                        // Clock 时间轴上的一个具体时刻，例如 Clock::now() + 100ms；不是“100ms”这种时长
    using Functor = std::function<void()>;                                                                      // timer 到期后要执行的回调

    explicit TimerQueue(EventLoop* loop);
    ~TimerQueue();

    TimerId AddTimer(TimePoint deadline, Functor callback);                                                     // 普通回调版本；std::function 可能发生内存分配
    TimerId AddTimer(TimePoint deadline, TimerNotify notify);                                                   // RPC 热路径版本；固定大小通知，避免 std::function 分配
    void Cancel(TimerId timer_id);                                                                              // 取消 timer；在所属 EventLoop 线程里可直接从时间轮摘掉

private:
    using TimerAction = std::variant<Functor, TimerNotify>;                                                     // timer 到期后，要么执行普通回调，要么发送 TimerNotify

    // TimerId 同时保存“是否跨线程”“slot 在哪”“这个 slot 是第几次被复用”三类信息。
    // generation 每复用一次都会变化，所以旧 TimerId 不会误指向后来占用同一 slot 的新 timer。
    static constexpr std::uint32_t kInvalidSlot = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::uint64_t kExternalTimerMask = std::uint64_t{1} << 63;
    static constexpr std::uint64_t kSlotIndexMask = (std::uint64_t{1} << 31) - 1;
    static constexpr std::uint64_t kGenerationMask = 0xffffffffULL;

    static constexpr std::uint8_t kWheelLevelCount = 4;
    static constexpr std::uint8_t kWheelSize = 10;
    static constexpr std::uint8_t kInvalidWheelLevel = std::numeric_limits<std::uint8_t>::max();
    static constexpr std::uint8_t kInvalidBucket = std::numeric_limits<std::uint8_t>::max();
    static constexpr std::array<std::uint64_t, kWheelLevelCount> kWheelQuantumTicks{1, 10, 100, 1000};          // L0~L3 每个格子分别代表 1/10/100/1000ms

    struct TimerBucket
    {
        std::uint32_t head{kInvalidSlot};                                                                       // 这个 bucket 里第一个 timer 的 slot 下标
        std::uint32_t tail{kInvalidSlot};                                                                       // 这个 bucket 里最后一个 timer 的 slot 下标
    };

    // slot 正在使用时，wheel_prev/wheel_next 把它串在 bucket 里；空闲时只用 next_free 串进复用链表。
    struct TimerSlot
    {
        TimePoint deadline{};                                                                                   // 这个 timer 真正应该到期的时间点
        TimerAction action{};                                                                                   // 到期后要执行的回调或通知
        std::uint64_t external_timer_id{0};                                                                     // 跨线程 AddTimer 预先返回的 id；普通路径为 0
        std::uint64_t wheel_rounds{0};                                                                          // timer 放在最粗的 1s 层（L3）时还要多等几整圈；用于超过 10s 的 timeout
        std::uint32_t wheel_prev{kInvalidSlot};                                                                 // 同一个 bucket 里前一个 timer 的 slot 下标
        std::uint32_t wheel_next{kInvalidSlot};                                                                 // 同一个 bucket 里后一个 timer 的 slot 下标
        std::uint32_t next_free{kInvalidSlot};                                                                  // slot 空闲时，下一个可复用 slot 的下标
        std::uint32_t generation{0};                                                                            // slot 每复用一次就 +1，让旧 TimerId 自动失效
        std::uint8_t wheel_level{kInvalidWheelLevel};                                                           // 当前在哪一层：0=1ms，1=10ms，2=100ms，3=1s
        std::uint8_t bucket_index{kInvalidBucket};                                                              // 当前在这一层的第几个格子，范围 0~9
        bool active{false};                                                                                     // 这个 slot 当前是否正被一个 timer 使用
    };

    TimerId AddTimerInLoop(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id = 0);
    void CancelInLoop(TimerId timer_id);
    void HandleRead();
    void ArmTimerFd();                                                                                          // 启动 1ms“时钟”，让时间轮继续往前走
    void DisarmTimerFd();                                                                                       // 没有 timer 时停掉 1ms“时钟”，避免 EventLoop 空转
    void SyncCurrentTick();                                                                                     // timerfd 停着时，用真实经过的毫秒数把 current_tick_ 追到现在

    // 这三个函数管 slot 的一生：拿一个空位给新 timer 用、用完放回空闲链表、删除 timer 时顺便回收。
    std::uint32_t AllocateSlot(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id);        // 拿一个可复用 slot，并填入新 timer 的数据
    void ReleaseSlot(std::uint32_t slot_index);                                                                 // 清空这个 slot，并放回复用链表
    void RemoveTimerSlot(std::uint32_t slot_index);                                                             // 把 timer 从映射和时间轮中移除，再回收 slot

    // 下面这一组负责把 timer 放进或移出时间轮；时间往前走时，再把远处的 timer 一层层搬近并执行到期 timer。
    void ScheduleTimerSlot(std::uint32_t slot_index);                                                           // 根据 timer 的到期时间，把它放进时间轮
    void PlaceTimerSlot(std::uint32_t slot_index, std::uint64_t expiry_tick);                                   // 看 timer 还剩多久，决定放哪一级、哪一个格子
    void LinkTimerSlot(std::uint32_t slot_index, std::uint8_t wheel_level, std::uint8_t bucket_index);          // 把 timer 接到指定 bucket 链表末尾
    void UnlinkTimerSlot(std::uint32_t slot_index);                                                             // 把 timer 从当前 bucket 链表里摘下来
    void AdvanceWheel(std::uint64_t expirations);                                                               // 过去了多少毫秒，时间轮就往前走多少格
    void CascadeLevel(std::uint8_t wheel_level);                                                                // 上层格子走到当前时，把里面的 timer 重新放到更细的层级
    void ExpireCurrentBucket();                                                                                 // 检查当前 1ms 格子：到期就执行，没到期就重新安排

    [[nodiscard]] std::uint64_t DeadlineTick(TimePoint deadline) const;                                         // 算出 deadline 是时间轮开始后的第几毫秒；不足 1ms 向后取整
    [[nodiscard]] TimerId MakeSlotTimerId(std::uint32_t slot_index, std::uint32_t generation) const;            // 把 slot 下标和 generation 编成 TimerId
    [[nodiscard]] TimerId MakeExternalTimerId();                                                                // 跨线程 AddTimer 先生成这个 id，真正入轮后仍使用它
    [[nodiscard]] bool DecodeSlotTimerId(TimerId timer_id, std::uint32_t* slot_index, 
        std::uint32_t* generation) const;                                                                       // 从普通 TimerId 拆出 slot 下标和 generation
    [[nodiscard]] static bool IsExternalTimerId(TimerId timer_id) noexcept;                                     // 判断是不是跨线程路径生成的 TimerId

    static void InvokeAction(TimerAction action);                                                               // 执行普通 callback，或发送 TimerNotify

    EventLoop* loop_;                                                                                           // 这个 TimerQueue 属于哪个 EventLoop；时间轮状态只在它的线程里修改
    int timer_fd_;                                                                                              // Linux timerfd，负责每 1ms 提醒一次 EventLoop
    std::unique_ptr<Channel> timer_channel_;                                                                    // 把 timer_fd_ 的可读事件接入 EventLoop

    // 时间轮本身只保存 slot 下标；TimerSlot 用完后继续留在 slots_ 里复用。
    std::vector<TimerSlot> slots_;                                                                              // 所有 timer 的槽位，空闲槽也保留在这里等待复用
    std::array<std::array<TimerBucket, kWheelSize>, kWheelLevelCount> wheel_{};                                 // 四层、每层 10 个格子；每个格子串着一组 timer
    std::uint32_t free_slot_head_{kInvalidSlot};                                                                // 当前第一个可复用的空闲 slot；没有时为 kInvalidSlot
    std::uint64_t active_timer_count_{0};                                                                       // 当前还有多少个正在等待触发或取消的 timer
    TimePoint wheel_epoch_{Clock::now()};                                                                       // 时间轮开始计时的时间点，后面的 tick 都从这里往后数
    std::uint64_t current_tick_{0};                                                                             // 时间轮现在已经走过了多少毫秒，1 tick = 1ms
    bool timer_fd_armed_{false};                                                                                // 1ms 周期 timerfd 现在是否正在运行

    // 别的线程调用 AddTimer 时，必须先把 TimerId 返回给调用者，再由所属 EventLoop 线程真正把 timer 放进时间轮。
    // RPC timeout 正常就在所属 EventLoop 线程里执行，不走这套额外映射。
    std::atomic_uint64_t next_external_timer_id_{1};                                                            // 下一个跨线程 TimerId 使用的自增序号
    std::unordered_map<std::uint64_t, std::uint32_t> external_timer_slots_;                                     // 跨线程 TimerId -> 实际 slot 下标
};

}// namespace nebula::net
