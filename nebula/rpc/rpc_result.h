#pragma once

#include "nebula/rpc/rpc_call.h"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>

namespace nebula::rpc
{

struct RpcCallOptions
{
    std::chrono::milliseconds timeout{0};
    std::optional<std::chrono::steady_clock::time_point> deadline;

    // true: 适合普通业务代码，挂起前立即结束当前批次并 Flush。
    // false: 适合批量启动多个调用后由外层统一 Flush。
    bool flush{true};
};

template <typename Response>
class RpcResult
{
public:
    RpcResult(Response response,
              RpcCallState state,
              std::string error_text,
              bool cancelled,
              std::uint64_t request_id,
              std::string trace_id,
              std::chrono::microseconds latency,
              std::int32_t remote_error_code)
        : response_(std::move(response)),
          state_(state),
          error_text_(std::move(error_text)),
          cancelled_(cancelled),
          request_id_(request_id),
          trace_id_(std::move(trace_id)),
          latency_(latency),
          remote_error_code_(remote_error_code)
    {
    }

    [[nodiscard]] bool Ok() const noexcept
    {
        return state_ == RpcCallState::Completed && !cancelled_ && error_text_.empty();
    }

    explicit operator bool() const noexcept
    {
        return Ok();
    }

    [[nodiscard]] RpcCallState State() const noexcept
    {
        return state_;
    }

    [[nodiscard]] bool Cancelled() const noexcept
    {
        return cancelled_;
    }

    [[nodiscard]] const std::string& ErrorText() const noexcept
    {
        return error_text_;
    }

    [[nodiscard]] std::uint64_t RequestId() const noexcept
    {
        return request_id_;
    }

    [[nodiscard]] const std::string& TraceId() const noexcept
    {
        return trace_id_;
    }

    [[nodiscard]] std::chrono::microseconds Latency() const noexcept
    {
        return latency_;
    }

    [[nodiscard]] std::int32_t RemoteErrorCode() const noexcept
    {
        return remote_error_code_;
    }

    [[nodiscard]] const Response& ResponseValue() const& noexcept
    {
        return response_;
    }

    [[nodiscard]] Response& ResponseValue() & noexcept
    {
        return response_;
    }

    [[nodiscard]] Response TakeResponse() &&
    {
        return std::move(response_);
    }

private:
    Response response_;
    RpcCallState state_{RpcCallState::Pending};
    std::string error_text_;
    bool cancelled_{false};
    std::uint64_t request_id_{0};
    std::string trace_id_;
    std::chrono::microseconds latency_{0};
    std::int32_t remote_error_code_{0};
};

}  // namespace nebula::rpc
