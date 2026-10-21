#pragma once

#include "nebula/rpc/rpc_call.h"

#include <cstdint>
#include <stdexcept>
#include <string>

namespace nebula::rpc
{

// 服务端错误码：线上仍以 int32 承载，两端各自强转一次
enum class RpcErrorCode : std::int32_t
{
    Ok            = 0,
    BadRequest    = 400,
    NotFound      = 404,                                // service / method 不存在
    InternalError = 500,                                // 响应序列化 / 编码失败
};


// 协程 Service 主动结束当前 RPC 时使用：异常只在框架内部传播到 dispatch 边界，
// 最终转换成 ERROR 帧，不会越过 EventLoop。
class RpcServiceError final : public std::runtime_error
{
public:
    RpcServiceError(RpcErrorCode code, const std::string& message)
        : std::runtime_error(message),
          code_(code)
    {
    }

    [[nodiscard]] RpcErrorCode Code() const noexcept
    {
        return code_;
    }

private:
    RpcErrorCode code_;
};

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
