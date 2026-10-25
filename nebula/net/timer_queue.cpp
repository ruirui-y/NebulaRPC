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

    // owner loop 直接分配可复用 slot，并插入最小堆；不再创建节点型容器。
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

    // RPC timeout 热路径：固定布局通知直接进入 slot + heap，不构造 std::function 节点。
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

TimerId TimerQueue::AddTimerInLoop(TimePoint deadline,
                                   TimerAction action,
                                   std::uint64_t external_timer_id)
{
    loop_->AssertInLoopThread();

    // 只有新 timer 比当前堆顶更早时才需要立即重设 timerfd。
    const bool reset_timer_fd = heap_.empty() || deadline < slots_[heap_.front()].deadline;

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

    try
    {
        PushHeap(slot_index);
    }
    catch (...)
    {
        if (external_timer_id != 0)
        {
            external_timer_slots_.erase(external_timer_id);
        }

        ReleaseSlot(slot_index);
        throw;
    }

    if (reset_timer_fd)
    {
        ResetTimerFd();
    }

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

    // lazy cancel：只删用户态结构，不立即重设 timerfd；允许旧 deadline stale wakeup。
    RemoveTimerSlot(slot_index);
}

void TimerQueue::HandleRead()
{
    loop_->AssertInLoopThread();

    std::uint64_t expirations = 0;
    // timerfd 的 read 返回累计到期次数，必须一次读满 8 字节；读完计数清零。
    const ssize_t n = ::read(timer_fd_, &expirations, sizeof(expirations));

    // 非阻塞 fd 无到期数据时是 EAGAIN，被信号打断是 EINTR，两者都不算错误。
    if (n < 0 && errno != EAGAIN && errno != EINTR)
    {
        throw std::runtime_error("timerfd read failed: " + std::string(std::strerror(errno)));
    }

    const TimePoint now = Clock::now();

    // heap_[0] 永远是最早 deadline；先摘除并回收 slot，再执行通知。
    while (!heap_.empty())
    {
        const std::uint32_t slot_index = heap_.front();
        TimerSlot& slot = slots_[slot_index];

        if (slot.deadline > now)
        {
            break;
        }

        TimerAction action = std::move(slot.action);
        RemoveTimerSlot(slot_index);
        InvokeAction(std::move(action));
    }

    ResetTimerFd();
}

void TimerQueue::ResetTimerFd()
{
    // it_value 决定首次到期时间，it_interval 全 0 表示不重复；第二参数 0 表示按相对时长设置。
    itimerspec value{};

    if (!heap_.empty())
    {
        const TimerSlot& earliest = slots_[heap_.front()];
        auto delay = std::chrono::duration_cast<std::chrono::nanoseconds>(
            earliest.deadline - Clock::now());

        if (delay.count() <= 0)
        {
            // 已过期时给 1ns 而不是 0：it_value 为 0 表示解除 timerfd 定时。
            delay = std::chrono::nanoseconds(1);
        }

        // itimerspec 只认 {秒, 纳秒} 两段，chrono 的纳秒时长要在这里拆开。
        constexpr std::int64_t kNanosecondsPerSecond = 1000000000LL;
        const std::int64_t total_nanoseconds = delay.count();

        value.it_value.tv_sec = static_cast<time_t>(
            total_nanoseconds / kNanosecondsPerSecond);
        value.it_value.tv_nsec = static_cast<long>(
            total_nanoseconds % kNanosecondsPerSecond);
    }

    // 空表时 value 全 0 等于 disarm；非空时按相对时长重新武装，两种情况都要走一次系统调用。
    if (::timerfd_settime(timer_fd_, 0, &value, nullptr) < 0)
    {
        throw std::runtime_error("timerfd_settime failed: " + std::string(std::strerror(errno)));
    }
}

std::uint32_t TimerQueue::AllocateSlot(TimePoint deadline,
                                       TimerAction action,
                                       std::uint64_t external_timer_id)
{
    std::uint32_t slot_index = kInvalidSlot;

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
    slot.generation = slot.generation == std::numeric_limits<std::uint32_t>::max()
        ? 1
        : slot.generation + 1;
    slot.deadline = deadline;
    slot.action = std::move(action);
    slot.external_timer_id = external_timer_id;
    slot.sequence = next_sequence_++;
    slot.heap_index = kInvalidSlot;
    slot.next_free = kInvalidSlot;
    slot.active = true;

    return slot_index;
}

void TimerQueue::ReleaseSlot(std::uint32_t slot_index)
{
    TimerSlot& slot = slots_[slot_index];

    // 销毁本次 action，但保留 TimerSlot 本身供下一次 timer 复用。
    slot.action.template emplace<Functor>();
    slot.external_timer_id = 0;
    slot.heap_index = kInvalidSlot;
    slot.active = false;
    slot.next_free = free_slot_head_;
    free_slot_head_ = slot_index;
}

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

    if (slot.heap_index != kInvalidSlot)
    {
        RemoveHeapAt(slot.heap_index);
    }

    ReleaseSlot(slot_index);
}

void TimerQueue::PushHeap(std::uint32_t slot_index)
{
    heap_.push_back(slot_index);
    slots_[slot_index].heap_index = static_cast<std::uint32_t>(heap_.size() - 1);
    SiftUp(slots_[slot_index].heap_index);
}

void TimerQueue::RemoveHeapAt(std::uint32_t heap_index)
{
    const std::uint32_t removed_slot = heap_[heap_index];
    const std::uint32_t last_slot = heap_.back();
    heap_.pop_back();

    if (heap_index < heap_.size())
    {
        heap_[heap_index] = last_slot;
        slots_[last_slot].heap_index = heap_index;

        if (heap_index > 0)
        {
            const std::uint32_t parent = (heap_index - 1) / 2;

            if (Earlier(last_slot, heap_[parent]))
            {
                SiftUp(heap_index);
                slots_[removed_slot].heap_index = kInvalidSlot;
                return;
            }
        }

        SiftDown(heap_index);
    }

    slots_[removed_slot].heap_index = kInvalidSlot;
}

void TimerQueue::SiftUp(std::uint32_t heap_index)
{
    while (heap_index > 0)
    {
        const std::uint32_t parent = (heap_index - 1) / 2;

        if (!Earlier(heap_[heap_index], heap_[parent]))
        {
            break;
        }

        SwapHeapEntries(heap_index, parent);
        heap_index = parent;
    }
}

void TimerQueue::SiftDown(std::uint32_t heap_index)
{
    while (true)
    {
        const std::uint32_t left = heap_index * 2 + 1;

        if (left >= heap_.size())
        {
            return;
        }

        const std::uint32_t right = left + 1;
        std::uint32_t earliest = left;

        if (right < heap_.size() && Earlier(heap_[right], heap_[left]))
        {
            earliest = right;
        }

        if (!Earlier(heap_[earliest], heap_[heap_index]))
        {
            return;
        }

        SwapHeapEntries(heap_index, earliest);
        heap_index = earliest;
    }
}

void TimerQueue::SwapHeapEntries(std::uint32_t lhs, std::uint32_t rhs)
{
    std::swap(heap_[lhs], heap_[rhs]);
    slots_[heap_[lhs]].heap_index = lhs;
    slots_[heap_[rhs]].heap_index = rhs;
}

bool TimerQueue::Earlier(std::uint32_t lhs_slot,
                         std::uint32_t rhs_slot) const
{
    const TimerSlot& lhs = slots_[lhs_slot];
    const TimerSlot& rhs = slots_[rhs_slot];

    if (lhs.deadline != rhs.deadline)
    {
        return lhs.deadline < rhs.deadline;
    }

    return lhs.sequence < rhs.sequence;
}

TimerId TimerQueue::MakeSlotTimerId(std::uint32_t slot_index,
                                    std::uint32_t generation) const
{
    const std::uint64_t slot_token = static_cast<std::uint64_t>(slot_index) + 1;
    const std::uint64_t value =
        (static_cast<std::uint64_t>(generation) << 31) |
        slot_token;

    return TimerId(value);
}

TimerId TimerQueue::MakeExternalTimerId()
{
    const std::uint64_t sequence = next_external_timer_id_.fetch_add(
        1,
        std::memory_order_relaxed);
    return TimerId(kExternalTimerMask | (sequence & ~kExternalTimerMask));
}

bool TimerQueue::DecodeSlotTimerId(TimerId timer_id,
                                   std::uint32_t* slot_index,
                                   std::uint32_t* generation) const
{
    if (!timer_id.Valid() ||
        IsExternalTimerId(timer_id) ||
        slot_index == nullptr ||
        generation == nullptr)
    {
        return false;
    }

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
