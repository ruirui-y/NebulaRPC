#include "nebula/net/timer_queue.h"

#include "nebula/net/channel.h"
#include "nebula/net/event_loop.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/timerfd.h>
#include <unistd.h>
#include <utility>

namespace nebula::net
{
namespace
{

int CreateTimerFd()
{
    // CLOCK_MONOTONIC 与 TimerQueue::Clock 对应；NONBLOCK 保证 HandleRead 不会被 read 挂住。
    const int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);

    if (fd < 0)
    {
        throw std::runtime_error("timerfd_create failed: " + std::string(std::strerror(errno)));
    }

    return fd;
}

}  // namespace

TimerQueue::TimerQueue(EventLoop* loop)
    : loop_(loop),
      timer_fd_(CreateTimerFd()),
      timer_channel_(std::make_unique<Channel>(loop, timer_fd_))
{
    timer_channel_->SetReadCallback([this]
        {
            HandleRead();
        });
    timer_channel_->EnableReading();
}

TimerQueue::~TimerQueue()
{
    loop_->AssertInLoopThread();
    // 先从 poller 摘掉 Channel 再关 fd，否则 epoll 里会留下已关闭的 fd 描述。
    timer_channel_->DisableAll();
    timer_channel_->Remove();
    ::close(timer_fd_);
}

TimerId TimerQueue::AddTimer(TimePoint deadline, Functor callback)
{
    TimerAction action(std::in_place_type<Functor>, std::move(callback));

    // owner loop 直接分配可复用 slot 并挂入时间轮；不再维护 heap，也不创建节点型容器。
    if (loop_->IsInLoopThread())
    {
        return AddTimerInLoop(deadline, std::move(action));
    }

    // 跨线程调用必须先返回一个稳定 TimerId，再由 owner loop 真正建立 timer。
    // 这条兼容路径不是 RPC timeout 热路径。
    const TimerId timer_id = MakeExternalTimerId();
    loop_->QueueInLoop([this,
                        timer_id,
                        deadline,
                        action = std::move(action)]() mutable
        {
            AddTimerInLoop(deadline, std::move(action), timer_id.Value());
        });

    return timer_id;
}

TimerId TimerQueue::AddTimer(TimePoint deadline, TimerNotify notify)
{
    TimerAction action(std::in_place_type<TimerNotify>, std::move(notify));

    // RPC timeout 热路径：固定布局通知直接进入 slot + timing wheel，不构造 std::function 节点。
    if (loop_->IsInLoopThread())
    {
        return AddTimerInLoop(deadline, std::move(action));
    }

    const TimerId timer_id = MakeExternalTimerId();
    loop_->QueueInLoop([this,
                        timer_id,
                        deadline,
                        action = std::move(action)]() mutable
        {
            AddTimerInLoop(deadline, std::move(action), timer_id.Value());
        });

    return timer_id;
}

void TimerQueue::Cancel(TimerId timer_id)
{
    if (!timer_id.Valid())
    {
        return;
    }

    // owner loop 已经具备修改 TimerQueue 的权限，直接取消，避免再次包装并投递给自己。
    if (loop_->IsInLoopThread())
    {
        CancelInLoop(timer_id);
        return;
    }

    loop_->QueueInLoop([this, timer_id]
        {
            CancelInLoop(timer_id);
        });
}

TimerId TimerQueue::AddTimerInLoop(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id)
{
    loop_->AssertInLoopThread();

    // timerfd 关闭期间没有活动 timer，可以直接把逻辑 tick 快进到当前毫秒再安排新节点。
    if (!timer_fd_armed_)
    {
        SyncCurrentTick();
    }

    const std::uint32_t slot_index = AllocateSlot(
        deadline,
        std::move(action),
        external_timer_id);

    if (external_timer_id != 0)
    {
        try
        {
            external_timer_slots_.emplace(external_timer_id, slot_index);
        }
        catch (...)
        {
            ReleaseSlot(slot_index);
            throw;
        }
    }

    ScheduleTimerSlot(slot_index);
    ++active_timer_count_;

    // 周期 timerfd 只在第一个活动 timer 到来时启动，后续 Add/Cancel 不重复 settime。
    if (!timer_fd_armed_)
    {
        try
        {
            ArmTimerFd();
        }
        catch (...)
        {
            RemoveTimerSlot(slot_index);
            throw;
        }
    }

    // 跨线程调用在入队前就拿到了 id，这里必须原样返回，不能换成槽位 id。
    if (external_timer_id != 0)
    {
        return TimerId(external_timer_id);
    }

    const TimerSlot& slot = slots_[slot_index];
    return MakeSlotTimerId(slot_index, slot.generation);
}

void TimerQueue::CancelInLoop(TimerId timer_id)
{
    loop_->AssertInLoopThread();

    std::uint32_t slot_index = kInvalidSlot;

    if (IsExternalTimerId(timer_id))
    {
        const auto timer_it = external_timer_slots_.find(timer_id.Value());

        if (timer_it == external_timer_slots_.end())
        {
            return;
        }

        slot_index = timer_it->second;
    }
    else
    {
        std::uint32_t generation = 0;

        if (!DecodeSlotTimerId(timer_id, &slot_index, &generation) ||
            slot_index >= slots_.size())
        {
            return;
        }

        const TimerSlot& slot = slots_[slot_index];

        // slot 已复用时 generation 会变化，旧 TimerId 不能误取消新 timer。
        if (!slot.active || slot.generation != generation)
        {
            return;
        }
    }

    // bucket 是侵入式双向链表，任意 Timer Cancel 都直接摘链并立即归还 slot。
    RemoveTimerSlot(slot_index);

    if (active_timer_count_ == 0)
    {
        DisarmTimerFd();
    }
}

void TimerQueue::HandleRead()
{
    loop_->AssertInLoopThread();

    std::uint64_t expirations = 0;
    // timerfd 的 read 返回累计到期次数，必须一次读满 8 字节；每次 expiration 对应一个 1ms tick。
    const ssize_t n = ::read(timer_fd_, &expirations, sizeof(expirations));

    // 非阻塞 fd 无到期数据时是 EAGAIN，被信号打断是 EINTR，两者都不推进时间轮。
    if (n < 0)
    {
        if (errno == EAGAIN || errno == EINTR)
        {
            return;
        }

        throw std::runtime_error("timerfd read failed: " + std::string(std::strerror(errno)));
    }

    AdvanceWheel(expirations);

    // HandleRead 内 callback 可能取消最后一个 timer；统一在本轮推进完成后再 disarm。
    if (active_timer_count_ == 0)
    {
        DisarmTimerFd();
    }
}

void TimerQueue::ArmTimerFd()
{
    if (timer_fd_armed_)
    {
        return;
    }

    const TimePoint next_tick = wheel_epoch_ + std::chrono::milliseconds(static_cast<std::int64_t>(current_tick_ + 1));
    auto delay = std::chrono::duration_cast<std::chrono::nanoseconds>(next_tick - Clock::now());

    if (delay.count() <= 0)
    {
        // it_value 为 0 表示 disarm；已经跨过边界时用 1ns 立即补上下一 tick。
        delay = std::chrono::nanoseconds(1);
    }

    itimerspec value{};
    constexpr std::int64_t kNanosecondsPerSecond = 1000000000LL;
    const std::int64_t total_nanoseconds = delay.count();
    value.it_value.tv_sec = static_cast<time_t>(total_nanoseconds / kNanosecondsPerSecond);
    value.it_value.tv_nsec = static_cast<long>(total_nanoseconds % kNanosecondsPerSecond);
    value.it_interval.tv_sec = 0;
    value.it_interval.tv_nsec = 1000000L;

    // 首次到期对齐下一毫秒边界，之后固定 1ms 周期；累计 expiration 由 HandleRead 一次补齐。
    if (::timerfd_settime(timer_fd_, 0, &value, nullptr) < 0)
    {
        throw std::runtime_error("timerfd_settime failed: " + std::string(std::strerror(errno)));
    }

    timer_fd_armed_ = true;
}

void TimerQueue::DisarmTimerFd()
{
    if (!timer_fd_armed_)
    {
        return;
    }

    itimerspec value{};

    if (::timerfd_settime(timer_fd_, 0, &value, nullptr) < 0)
    {
        throw std::runtime_error("timerfd_settime failed: " + std::string(std::strerror(errno)));
    }

    timer_fd_armed_ = false;
}

void TimerQueue::SyncCurrentTick()
{
    const auto elapsed = Clock::now() - wheel_epoch_;
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
    const std::uint64_t now_tick = elapsed_ms.count() > 0
        ? static_cast<std::uint64_t>(elapsed_ms.count())
        : 0;

    // 只有 timerfd 已 disarm 才调用这里，此时没有活动节点，快进不会跳过任何 timeout。
    if (now_tick > current_tick_)
    {
        current_tick_ = now_tick;
    }
}

std::uint32_t TimerQueue::AllocateSlot(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id)
{
    std::uint32_t slot_index = kInvalidSlot;

    // 复用优先：free list 非空就不扩 slots_，否则长时间运行后槽位只增不减。
    if (free_slot_head_ != kInvalidSlot)
    {
        slot_index = free_slot_head_;
        free_slot_head_ = slots_[slot_index].next_free;
    }
    else
    {
        // TimerId 的低 31 bit 保存 slot_index + 1。
        if (slots_.size() >= kSlotIndexMask)
        {
            throw std::overflow_error("timer slot index exhausted");
        }

        slot_index = static_cast<std::uint32_t>(slots_.size());
        slots_.emplace_back();
    }

    TimerSlot& slot = slots_[slot_index];
    // 回绕时跳 0：0 是无效 id 的标记，跳过它才能保证旧 TimerId 一定不等于新 TimerId。
    slot.generation = slot.generation == std::numeric_limits<std::uint32_t>::max()
        ? 1
        : slot.generation + 1;
    slot.deadline = deadline;
    slot.action = std::move(action);
    slot.external_timer_id = external_timer_id;
    slot.wheel_rounds = 0;
    slot.wheel_prev = kInvalidSlot;
    slot.wheel_next = kInvalidSlot;
    slot.next_free = kInvalidSlot;
    slot.wheel_level = kInvalidWheelLevel;
    slot.bucket_index = kInvalidBucket;
    slot.active = true;

    return slot_index;
}

void TimerQueue::ReleaseSlot(std::uint32_t slot_index)
{
    TimerSlot& slot = slots_[slot_index];

    // action 的 RAII 析构发生在这里；TimerSlot 本体保留并挂回 free list 供下一次 timer 复用。
    slot.action.template emplace<Functor>();
    slot.external_timer_id = 0;
    slot.wheel_rounds = 0;
    slot.wheel_prev = kInvalidSlot;
    slot.wheel_next = kInvalidSlot;
    slot.wheel_level = kInvalidWheelLevel;
    slot.bucket_index = kInvalidBucket;
    slot.active = false;
    slot.next_free = free_slot_head_;
    free_slot_head_ = slot_index;
}

// 摘除三件事一起做：外部 id 映射、bucket 链位置、槽位归还；任何一步都不能漏。
void TimerQueue::RemoveTimerSlot(std::uint32_t slot_index)
{
    TimerSlot& slot = slots_[slot_index];

    if (!slot.active)
    {
        return;
    }

    if (slot.external_timer_id != 0)
    {
        external_timer_slots_.erase(slot.external_timer_id);
    }

    if (slot.wheel_level != kInvalidWheelLevel)
    {
        UnlinkTimerSlot(slot_index);
    }

    --active_timer_count_;
    ReleaseSlot(slot_index);
}

void TimerQueue::ScheduleTimerSlot(std::uint32_t slot_index)
{
    std::uint64_t expiry_tick = DeadlineTick(slots_[slot_index].deadline);

    // 新增 timer 已经过期时也不能提前在当前 bucket 执行，统一安排到下一 1ms tick。
    if (expiry_tick <= current_tick_)
    {
        expiry_tick = current_tick_ + 1;
    }

    PlaceTimerSlot(slot_index, expiry_tick);
}

void TimerQueue::PlaceTimerSlot(std::uint32_t slot_index, std::uint64_t expiry_tick)
{
    TimerSlot& slot = slots_[slot_index];
    const std::uint64_t delta_ticks = expiry_tick - current_tick_;
    std::uint8_t wheel_level = 0;

    if (delta_ticks < 10)
    {
        wheel_level = 0;
    }
    else if (delta_ticks < 100)
    {
        wheel_level = 1;
    }
    else if (delta_ticks < 1000)
    {
        wheel_level = 2;
    }
    else
    {
        wheel_level = 3;
    }

    const std::uint64_t quantum_ticks = kWheelQuantumTicks[wheel_level];
    const std::uint8_t bucket_index = static_cast<std::uint8_t>((expiry_tick / quantum_ticks) % kWheelSize);
    slot.wheel_rounds = 0;

    // L3 每个 bucket 10s 才再次访问一次，rounds 让四级轮仍能承载任意更长的 timeout。
    if (wheel_level == 3)
    {
        const std::uint64_t expiry_level_tick = expiry_tick / quantum_ticks;
        const std::uint64_t current_level_tick = current_tick_ / quantum_ticks;
        const std::uint64_t level_distance = expiry_level_tick - current_level_tick;
        slot.wheel_rounds = (level_distance - 1) / kWheelSize;
    }

    LinkTimerSlot(slot_index, wheel_level, bucket_index);
}

void TimerQueue::LinkTimerSlot(std::uint32_t slot_index, std::uint8_t wheel_level, std::uint8_t bucket_index)
{
    TimerSlot& slot = slots_[slot_index];
    TimerBucket& bucket = wheel_[wheel_level][bucket_index];

    slot.wheel_level = wheel_level;
    slot.bucket_index = bucket_index;
    slot.wheel_prev = bucket.tail;
    slot.wheel_next = kInvalidSlot;

    if (bucket.tail != kInvalidSlot)
    {
        slots_[bucket.tail].wheel_next = slot_index;
    }
    else
    {
        bucket.head = slot_index;
    }

    bucket.tail = slot_index;
}

void TimerQueue::UnlinkTimerSlot(std::uint32_t slot_index)
{
    TimerSlot& slot = slots_[slot_index];
    TimerBucket& bucket = wheel_[slot.wheel_level][slot.bucket_index];

    if (slot.wheel_prev != kInvalidSlot)
    {
        slots_[slot.wheel_prev].wheel_next = slot.wheel_next;
    }
    else
    {
        bucket.head = slot.wheel_next;
    }

    if (slot.wheel_next != kInvalidSlot)
    {
        slots_[slot.wheel_next].wheel_prev = slot.wheel_prev;
    }
    else
    {
        bucket.tail = slot.wheel_prev;
    }

    slot.wheel_prev = kInvalidSlot;
    slot.wheel_next = kInvalidSlot;
    slot.wheel_level = kInvalidWheelLevel;
    slot.bucket_index = kInvalidBucket;
}

void TimerQueue::AdvanceWheel(std::uint64_t expirations)
{
    for (std::uint64_t i = 0; i < expirations; ++i)
    {
        ++current_tick_;

        // 高层先下放，保证恰好落在当前 tick 的 timer 能在本轮进入 L0 并立即触发。
        if (current_tick_ % kWheelQuantumTicks[3] == 0)
        {
            CascadeLevel(3);
        }

        if (current_tick_ % kWheelQuantumTicks[2] == 0)
        {
            CascadeLevel(2);
        }

        if (current_tick_ % kWheelQuantumTicks[1] == 0)
        {
            CascadeLevel(1);
        }

        ExpireCurrentBucket();
    }
}

void TimerQueue::CascadeLevel(std::uint8_t wheel_level)
{
    const std::uint64_t quantum_ticks = kWheelQuantumTicks[wheel_level];
    const std::uint8_t bucket_index = static_cast<std::uint8_t>((current_tick_ / quantum_ticks) % kWheelSize);
    TimerBucket& bucket = wheel_[wheel_level][bucket_index];
    std::uint32_t slot_index = bucket.head;

    while (slot_index != kInvalidSlot)
    {
        TimerSlot& slot = slots_[slot_index];
        const std::uint32_t next_slot = slot.wheel_next;

        // 超过 10s 的 Timer 只在 L3 对应 bucket 再次转到时扣一圈，不需要额外 overflow 结构。
        if (wheel_level == 3 && slot.wheel_rounds > 0)
        {
            --slot.wheel_rounds;
            slot_index = next_slot;
            continue;
        }

        UnlinkTimerSlot(slot_index);
        const std::uint64_t expiry_tick = DeadlineTick(slot.deadline);

        // deadline 恰好落在当前边界时先挂进当前 L0，统一由 ExpireCurrentBucket 发通知。
        if (expiry_tick <= current_tick_)
        {
            LinkTimerSlot(slot_index, 0, static_cast<std::uint8_t>(current_tick_ % kWheelSize));
        }
        else
        {
            PlaceTimerSlot(slot_index, expiry_tick);
        }

        slot_index = next_slot;
    }
}

void TimerQueue::ExpireCurrentBucket()
{
    TimerBucket& bucket = wheel_[0][static_cast<std::uint8_t>(current_tick_ % kWheelSize)];

    // callback 前先摘链并回收 slot，回调里即使继续 Add/Cancel 也不会破坏当前 bucket 遍历。
    while (bucket.head != kInvalidSlot)
    {
        const std::uint32_t slot_index = bucket.head;
        TimerSlot& slot = slots_[slot_index];
        const std::uint64_t expiry_tick = DeadlineTick(slot.deadline);

        // 防止任何边界计算误把未来 timer 提前触发；重新分层最多只会让它晚一个 1ms tick。
        if (expiry_tick > current_tick_)
        {
            UnlinkTimerSlot(slot_index);
            PlaceTimerSlot(slot_index, expiry_tick);
            continue;
        }

        TimerAction action = std::move(slot.action);
        RemoveTimerSlot(slot_index);
        InvokeAction(std::move(action));
    }
}

std::uint64_t TimerQueue::DeadlineTick(TimePoint deadline) const
{
    if (deadline <= wheel_epoch_)
    {
        return 0;
    }

    // ceil 到毫秒保证 timeout 只会等于或晚于 deadline，绝不因为离散 tick 提前触发。
    const auto elapsed = deadline - wheel_epoch_;
    const auto elapsed_ms = std::chrono::ceil<std::chrono::milliseconds>(elapsed);
    return static_cast<std::uint64_t>(elapsed_ms.count());
}

TimerId TimerQueue::MakeSlotTimerId(std::uint32_t slot_index, std::uint32_t generation) const
{
    const std::uint64_t slot_token = static_cast<std::uint64_t>(slot_index) + 1;
    const std::uint64_t value =
        (static_cast<std::uint64_t>(generation) << 31) |
        slot_token;

    return TimerId(value);
}

// 高位置 1 作跨线程标记，低 63 bit 直接取序号；DecodeSlotTimerId 靠这一位把它挡回去。
TimerId TimerQueue::MakeExternalTimerId()
{
    const std::uint64_t sequence = next_external_timer_id_.fetch_add(
        1,
        std::memory_order_relaxed);
    return TimerId(kExternalTimerMask | (sequence & ~kExternalTimerMask));
}

bool TimerQueue::DecodeSlotTimerId(TimerId timer_id, std::uint32_t* slot_index, std::uint32_t* generation) const
{
    if (!timer_id.Valid() ||
        IsExternalTimerId(timer_id) ||
        slot_index == nullptr ||
        generation == nullptr)
    {
        return false;
    }

    // 与 MakeSlotTimerId 对称：低 31 bit 是槽位 token，bit62..31 是 generation。
    const std::uint64_t value = timer_id.Value();
    const std::uint64_t slot_token = value & kSlotIndexMask;

    if (slot_token == 0)
    {
        return false;
    }

    *slot_index = static_cast<std::uint32_t>(slot_token - 1);
    *generation = static_cast<std::uint32_t>(
        (value >> 31) & kGenerationMask);

    return *generation != 0;
}

bool TimerQueue::IsExternalTimerId(TimerId timer_id) noexcept
{
    return (timer_id.Value() & kExternalTimerMask) != 0;
}

void TimerQueue::InvokeAction(TimerAction action)
{
    if (auto* callback = std::get_if<Functor>(&action))
    {
        if (*callback)
        {
            (*callback)();
        }

        return;
    }

    std::get<TimerNotify>(action).Invoke();
}

}  // namespace nebula::net
