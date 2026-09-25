#pragma once

#include "nebula/rpc/rpc_call.h"

#include <stdexcept>
#include <string>

namespace nebula::rpc
{

// 协程调用失败时抛出的异常：带完成终态，业务可按 Timeout / Cancelled / Failed 分流
class RpcError final : public std::runtime_error
{
public:
    RpcError(RpcCallState state, const std::string& message)
        : std::runtime_error(message),
          state_(state)
    {
    }

    [[nodiscard]] RpcCallState State() const noexcept
    {
        return state_;
    }

private:
    RpcCallState state_;
};

}  // namespace nebula::rpc
