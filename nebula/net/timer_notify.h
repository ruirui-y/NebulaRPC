#pragma once

#include <cstdint>
#include <memory>

namespace nebula::net
{

// 轻量定时通知：TimerQueue 只保存目标生命周期、通知函数和 token，不感知上层对象类型。
struct TimerNotify final
{
    using Handler = void (*)(void* owner, std::uint64_t token);

    std::weak_ptr<void> owner;
    Handler handler{nullptr};
    std::uint64_t token{0};

    void Invoke() const
    {
        if (handler == nullptr)
        {
            return;
        }

        // lock 成功后在整个 handler 调用期间保持 owner 存活。
        const std::shared_ptr<void> locked_owner = owner.lock();

        if (locked_owner == nullptr)
        {
            return;
        }

        handler(locked_owner.get(), token);
    }
};

}  // namespace nebula::net
