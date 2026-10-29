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
    // timerfd 和 TimerQueue 都用单调时钟；NONBLOCK 避免没有 tick 时 read 把 EventLoop 卡住。
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
    // 先让 EventLoop 不再监听这个 fd，再关闭它；否则 epoll 里会留下已经失效的 fd。
    timer_channel_->DisableAll();
    timer_channel_->Remove();
    ::close(timer_fd_);
}

TimerId TimerQueue::AddTimer(TimePoint deadline, Functor callback)
{
    TimerAction action(std::in_place_type<Functor>, std::move(callback));

    // 当前线程就是这个 TimerQueue 所属的 EventLoop 线程，直接创建 timer，不需要再排队给自己。
    if (loop_->IsInLoopThread())
    {
        return AddTimerInLoop(deadline, std::move(action));
    }

    // 别的线程调用时，函数要先把 TimerId 返回；真正把 timer 放进时间轮的动作再交给所属 EventLoop 线程执行。
    // RPC timeout 正常就在所属 EventLoop 线程里执行，不走这条跨线程兼容路径。
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

    // RPC timeout 用固定大小的 TimerNotify，避免频繁创建 std::function 带来的额外内存分配。
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

    // 当前线程就是这个 TimerQueue 所属的 EventLoop 线程，直接取消，不需要再排队给自己。
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

    // timerfd 停着说明现在没有活动 timer；先把 current_tick_ 更新到“从时间轮开始到现在一共过去了多少毫秒”，再安排新 timer。
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

    // AllocateSlot 只是找了一个空位并存好 timer 数据；这一句才真正决定它应该放到时间轮的哪里。
    ScheduleTimerSlot(slot_index);
    ++active_timer_count_;

    // 只有第一个活动 timer 到来时才启动 1ms timerfd；后面的 timer 都共用这一个“时钟”。
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

    // 别的线程调用 AddTimer 时已经拿到了这个 id，所以 timer 真正放进时间轮后也必须继续使用同一个 id。
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

    std::uint32_t slot_index = kInvalidSlot;                                        // 先准备好 slot 下标，后面两条路径都会得到它

    // 跨线程创建的 Timer 使用独立 ID，需要通过哈希表找到对应的 slot。
    if (IsExternalTimerId(timer_id))
    {
        const auto timer_it = external_timer_slots_.find(timer_id.Value());

        if (timer_it == external_timer_slots_.end())
        {
            return;                                                                 // 找不到说明这个 Timer 已经不存在，无需取消
        }

        slot_index = timer_it->second;                                              // 从映射表中取出真正的 slot 下标
    }
    else
    {
        std::uint32_t generation = 0;

        // 本线程创建的 TimerId 自带 slot 下标和 generation，直接拆出来，不需要查映射表。
        if (!DecodeSlotTimerId(timer_id, &slot_index, &generation) ||
            slot_index >= slots_.size())
        {
            return;                                                                 // ID 无效，或者下标超出了 slots_ 的范围
        }

        const TimerSlot& slot = slots_[slot_index];

        // slot 可能已经被取消、超时，或者被新 Timer 复用；必须确认它仍然是原来的 Timer。
        if (!slot.active || slot.generation != generation)
        {
            return;                                                                 // Timer 已失效，不能误取消复用这个 slot 的新 Timer
        }
    }

    // 两条路径最终都拿到了 slot 下标，直接摘除 Timer、清理映射并把 slot 放回空闲链表。
    RemoveTimerSlot(slot_index);

    // 最后一个 Timer 被取消后，就不需要再每 1ms 唤醒 EventLoop 了。
    if (active_timer_count_ == 0)
    {
        DisarmTimerFd();
    }
}

void TimerQueue::HandleRead()
{
    loop_->AssertInLoopThread();

    std::uint64_t expirations = 0;
    // EventLoop 忙的时候可能一次错过多个 1ms 提醒；timerfd 会把次数累加起来，读到几次，时间轮就补走几毫秒。
    const ssize_t n = ::read(timer_fd_, &expirations, sizeof(expirations));

    // 没有新的计时提醒时 read 会返回 EAGAIN，被信号打断时返回 EINTR；这两种情况都说明这次不用往前走时间轮。
    if (n < 0)
    {
        if (errno == EAGAIN || errno == EINTR)
        {
            return;
        }

        throw std::runtime_error("timerfd read failed: " + std::string(std::strerror(errno)));
    }

    AdvanceWheel(expirations);

    // timer 回调里可能顺手取消最后一个 timer，所以这一轮处理完后再统一判断要不要停掉 timerfd。
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
        // timerfd 里 it_value=0 表示“关闭”。如果下一毫秒边界已经错过，就设成 1ns，让它几乎立刻补一次提醒。
        delay = std::chrono::nanoseconds(1);
    }

    itimerspec value{};
    constexpr std::int64_t kNanosecondsPerSecond = 1000000000LL;
    const std::int64_t total_nanoseconds = delay.count();
    value.it_value.tv_sec = static_cast<time_t>(total_nanoseconds / kNanosecondsPerSecond);
    value.it_value.tv_nsec = static_cast<long>(total_nanoseconds % kNanosecondsPerSecond);
    value.it_interval.tv_sec = 0;
    value.it_interval.tv_nsec = 1000000L;

    // 第一次提醒对齐到下一毫秒，之后每 1ms 提醒一次；EventLoop 忙时错过的次数会在下一次 read 时一起拿回来。
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

    // 这里只会在没有活动 timer 时调用，所以把 current_tick_ 直接更新到“现在过去了多少毫秒”不会漏掉任何 timer。
    if (now_tick > current_tick_)
    {
        current_tick_ = now_tick;
    }
}

std::uint32_t TimerQueue::AllocateSlot(TimePoint deadline, TimerAction action, std::uint64_t external_timer_id)
{
    std::uint32_t slot_index = kInvalidSlot;

    // 先看看以前用过的 slot 里有没有空位；有就直接复用，全部占满了才给 slots_ 增加新位置。
    if (free_slot_head_ != kInvalidSlot)
    {
        slot_index = free_slot_head_;
        free_slot_head_ = slots_[slot_index].next_free;
    }
    else
    {
        // TimerId 里保存的是 slot_index + 1，这样 0 可以专门表示“无效 TimerId”。
        if (slots_.size() >= kSlotIndexMask)
        {
            throw std::overflow_error("timer slot index exhausted");
        }

        slot_index = static_cast<std::uint32_t>(slots_.size());
        slots_.emplace_back();
    }

    TimerSlot& slot = slots_[slot_index];
    // 同一个 slot 每重新使用一次就把 generation 加 1；加到最大值后从 1 重新开始，0 永远表示无效。
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

    // 把这个 timer 留下的数据清掉，再把 slot 放回“可复用空位”链表；这块内存本身继续留着下次用。
    slot.action.template emplace<Functor>();                                // 销毁旧的回调或通知，换成一个空回调，释放本次 timer 持有的资源
    slot.external_timer_id = 0;                                             // 清除跨线程使用的 ID
    slot.wheel_rounds = 0;                                                  // 清除还要等待的圈数
    slot.wheel_prev = kInvalidSlot;                                         // 清除前一个 timer 的位置
    slot.wheel_next = kInvalidSlot;                                         // 清除后一个 timer 的位置
    slot.wheel_level = kInvalidWheelLevel;                                  // 标记当前 slot 已经不属于任何一层时间轮
    slot.bucket_index = kInvalidBucket;                                     // 清除原来所在的格子编号
    slot.active = false;                                                    // 标记这个 slot 已经空闲，可以重新使用
    slot.next_free = free_slot_head_;                                       // 让当前 slot 指向原来排在最前面的空闲 slot
    free_slot_head_ = slot_index;                                           // 把当前 slot 放到空闲链表最前面，下次优先复用它
}

// 真正删除一个 timer：先删掉用来找到它的额外映射，再从时间轮里摘掉，最后把占用的 slot 还回空闲链表。
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

// 先算这个 timer 应该在时间轮开始后的第几毫秒到期，再把它放到对应的位置。
void TimerQueue::ScheduleTimerSlot(std::uint32_t slot_index)
{
    std::uint64_t expiry_tick = DeadlineTick(slots_[slot_index].deadline);

    // 如果到期时间已经过去，最早也只能安排到下一毫秒；时间轮不能倒着走回已经处理过的格子。
    if (expiry_tick <= current_tick_)
    {
        expiry_tick = current_tick_ + 1;
    }

    PlaceTimerSlot(slot_index, expiry_tick);
}

// 看这个 timer 距离到期还剩多久：快到期的放到更细的层，离得远的先放到更粗的层。
void TimerQueue::PlaceTimerSlot(std::uint32_t slot_index, std::uint64_t expiry_tick)
{
    TimerSlot& slot = slots_[slot_index];
    const std::uint64_t delta_ticks = expiry_tick - current_tick_;                  // 距离到期还剩多少毫秒
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

    const std::uint64_t quantum_ticks = kWheelQuantumTicks[wheel_level];            // 当前层一个格子代表多少毫秒
    const std::uint8_t bucket_index =
        static_cast<std::uint8_t>((expiry_tick / quantum_ticks) % kWheelSize);      // 算出应该放到这一层的第几个格子

    slot.wheel_rounds = 0;                                                          // 默认下一次走到这个格子时就轮到它

    // L3 已经是最高层，超过 10 秒的 timer 只能继续复用这 10 个格子，
    // 所以需要额外记录同一个 bucket 还要白经过几次。
    if (wheel_level == 3)
    {
        const std::uint64_t expiry_level_tick = expiry_tick / quantum_ticks;        // deadline 对应 L3 的第几个 1 秒格子
        const std::uint64_t current_level_tick = current_tick_ / quantum_ticks;     // 当前位于 L3 的第几个 1 秒格子
        const std::uint64_t level_distance = expiry_level_tick - current_level_tick;

        slot.wheel_rounds = (level_distance - 1) / kWheelSize;                      // 同一个 bucket 还要完整跳过多少次
    }

    LinkTimerSlot(slot_index, wheel_level, bucket_index);
}

// 把 timer 接到这个格子的链表末尾；如果多个 timer 同一毫秒到期，就按注册先后顺序处理。
void TimerQueue::LinkTimerSlot(std::uint32_t slot_index, std::uint8_t wheel_level, std::uint8_t bucket_index)
{
    TimerSlot& slot = slots_[slot_index];                                    // 当前要挂进时间轮的 timer
    TimerBucket& bucket = wheel_[wheel_level][bucket_index];                 // 找到目标层级里的目标 bucket

    slot.wheel_level = wheel_level;                                          // 记住自己现在位于时间轮哪一层
    slot.bucket_index = bucket_index;                                        // 记住自己现在位于这一层的哪个格子
    slot.wheel_prev = bucket.tail;                                           // 新 timer 插到尾部，前一个就是原来的尾节点
    slot.wheel_next = kInvalidSlot;                                          // 新 timer 会成为尾节点，所以后面暂时没人

    if (bucket.tail != kInvalidSlot)
    {
        slots_[bucket.tail].wheel_next = slot_index;                         // bucket 非空：让原尾节点的 next 指向新 timer
    }
    else
    {
        bucket.head = slot_index;                                            // bucket 为空：新 timer 同时成为第一个节点
    }

    bucket.tail = slot_index;                                                // 新 timer 最终成为 bucket 的新尾节点
}


// 根据 slot 记录的前后位置，直接把它从链表中摘下来，不用从头查找。
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

    slot.wheel_prev = kInvalidSlot;                                     // 清除前一个 timer 的位置记录
    slot.wheel_next = kInvalidSlot;                                     // 清除后一个 timer 的位置记录
    slot.wheel_level = kInvalidWheelLevel;                              // 标记它当前不属于任何一层时间轮
    slot.bucket_index = kInvalidBucket;                                 // 清除它原来所在的格子编号
}

// timerfd 告诉我们这次累计过去了多少个 1ms。
// 比如 expirations = 5，就要补走 5 格，不能只走 1 格，否则时间轮会落后于真实时间。
void TimerQueue::AdvanceWheel(std::uint64_t expirations)
{
    for (std::uint64_t i = 0; i < expirations; ++i)
    {
        ++current_tick_;                                                    // 时间轮前进 1ms

        // 每过 10ms、100ms、1s，就有对应的上层格子轮到处理。
        // 先从最粗的一层开始往下搬，再检查当前毫秒有没有 Timer 到期。
        if (current_tick_ % kWheelQuantumTicks[3] == 0)
        {
            CascadeLevel(3);                                                // 每过 1s，检查 L3 当前轮到的格子
        }

        if (current_tick_ % kWheelQuantumTicks[2] == 0)
        {
            CascadeLevel(2);                                                // 每过 100ms，检查 L2 当前轮到的格子
        }

        if (current_tick_ % kWheelQuantumTicks[1] == 0)
        {
            CascadeLevel(1);                                                // 每过 10ms，检查 L1 当前轮到的格子
        }

        ExpireCurrentBucket();                                              // 最后检查当前 1ms 格子，执行真正到期的 Timer
    }
}

// 上层格子轮到时，把里面的 Timer 取出来，重新判断距离到期还剩多久。
// 还没到期的放回合适的层，已经到期的放到 L0，交给 ExpireCurrentBucket 处理。
void TimerQueue::CascadeLevel(std::uint8_t wheel_level)
{
    const std::uint64_t quantum_ticks = kWheelQuantumTicks[wheel_level];    // 当前层一个格子代表多少毫秒
    const std::uint8_t bucket_index = static_cast<std::uint8_t>(
        (current_tick_ / quantum_ticks) % kWheelSize);                      // 算出当前这一刻轮到当前层的哪个格子

    TimerBucket& bucket = wheel_[wheel_level][bucket_index];                // 找到这个格子
    std::uint32_t slot_index = bucket.head;                                 // 从格子里的第一个 Timer 开始处理

    while (slot_index != kInvalidSlot)
    {
        TimerSlot& slot = slots_[slot_index];
        const std::uint32_t next_slot = slot.wheel_next;                    // 先记住下一个，避免摘除当前节点后找不到它

        // L3 只有 10 个格子，一圈是 10s。
        // 如果这个 Timer 还没等够圈数，就留在原格子，等下次转到这里再检查。
        if (wheel_level == 3 && slot.wheel_rounds > 0)
        {
            --slot.wheel_rounds;                                            // 这次经过先不处理，剩余圈数减 1
            slot_index = next_slot;
            continue;
        }

        UnlinkTimerSlot(slot_index);                                        // 先从原来的格子摘下来，避免同一个 Timer 同时挂在两个格子里
        const std::uint64_t expiry_tick = DeadlineTick(slot.deadline);      // 重新算出它应该在时间轮的第几毫秒到期

        if (expiry_tick <= current_tick_)
        {
            // 已经到期，放进当前的 L0 格子，稍后由 ExpireCurrentBucket 统一执行。
            LinkTimerSlot(slot_index, 0, static_cast<std::uint8_t>(current_tick_ % kWheelSize));
        }
        else
        {
            PlaceTimerSlot(slot_index, expiry_tick);                        // 还没到期，根据剩余时间重新决定放哪一层
        }

        slot_index = next_slot;                                             // 继续处理原格子里的下一个 Timer
    }
}

// L0 是最后一道检查：到期的 Timer 执行通知，还没到期的重新安排。
void TimerQueue::ExpireCurrentBucket()
{
    TimerBucket& bucket = wheel_[0][static_cast<std::uint8_t>(current_tick_ % kWheelSize)]; // 找到当前这一毫秒对应的格子

    while (bucket.head != kInvalidSlot)
    {
        const std::uint32_t slot_index = bucket.head;                       // 取出当前格子的第一个 Timer
        TimerSlot& slot = slots_[slot_index];
        const std::uint64_t expiry_tick = DeadlineTick(slot.deadline);      // 再核对一次它真正的到期时间

        // 放进 L0 不代表一定已经到期。
        // 如果还没到，就重新安排，不能因为时间轮走到了这个格子而提前执行。
        if (expiry_tick > current_tick_)
        {
            UnlinkTimerSlot(slot_index);
            PlaceTimerSlot(slot_index, expiry_tick);
            continue;
        }

        TimerAction action = std::move(slot.action);                        // 先取出要执行的通知或回调
        RemoveTimerSlot(slot_index);                                        // 从时间轮移除并归还 slot，防止回调操作时重复处理
        InvokeAction(std::move(action));                                    // 最后才执行通知或回调
    }
}

// 把“具体什么时候到期”换算成“这是时间轮开始后的第几毫秒”。
std::uint64_t TimerQueue::DeadlineTick(TimePoint deadline) const
{
    if (deadline <= wheel_epoch_)
    {
        return 0;
    }

    // 例如到期时间落在第 8.3ms，就按第 9ms 处理；这样只会稍晚，不会提前超时。
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

// 跨线程 TimerId 用最高位做标记，其余位置保存自增序号；这样一眼就能和普通 slot TimerId 区分开。
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

    // 普通 TimerId 里同时塞了“slot 在哪里”和“这个 slot 是第几次被复用”；这里把两部分拆出来。
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
