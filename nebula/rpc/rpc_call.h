#pragma once

#include <atomic>

namespace nebula::rpc
{

enum class RpcCallState
{
    Pending,
    Completed,
    Cancelled,
    Timeout,
    Failed
};

class RpcCall final
{
public:
    RpcCall() = default;
    ~RpcCall() = default;

    RpcCall(const RpcCall&) = delete;
    RpcCall& operator=(const RpcCall&) = delete;

    // 移动构造：只发生在调用入队前（状态必然还是 Pending），直接搬运状态值安全
    RpcCall(RpcCall&& other) noexcept
        : state_(other.state_.load(std::memory_order_acquire))
    {
    }

    RpcCall& operator=(RpcCall&&) = delete;

    [[nodiscard]] RpcCallState State() const noexcept;                     // 读取当前状态

    bool TryComplete(RpcCallState state) noexcept;                         // CAS 抢占完成权，只有第一次成功

private:
    std::atomic<RpcCallState> state_{RpcCallState::Pending};               // 初始为在途
};

}  // namespace nebula::rpc
