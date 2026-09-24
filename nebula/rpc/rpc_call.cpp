#include "nebula/rpc/rpc_call.h"

namespace nebula::rpc
{

RpcCallState RpcCall::State() const noexcept
{
    return state_.load(std::memory_order_acquire);
}

bool RpcCall::TryComplete(RpcCallState state) noexcept
{
    RpcCallState expected = RpcCallState::Pending;

    return state_.compare_exchange_strong(
        expected,
        state,
        std::memory_order_acq_rel,
        std::memory_order_acquire);
}

}  // namespace nebula::rpc
