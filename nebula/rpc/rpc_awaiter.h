#pragma once

#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_controller.h"
#include "nebula/rpc/rpc_error.h"
#include "nebula/rpc/rpc_result.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <chrono>
#include <coroutine>
#include <memory>
#include <string>
#include <utility>

namespace nebula::rpc
{

[[nodiscard]] inline const google::protobuf::MethodDescriptor* FindMethod(
    const google::protobuf::ServiceDescriptor* service,
    const std::string& method_name)
{
    if (service == nullptr)
    {
        return nullptr;
    }

    return service->FindMethodByName(method_name);
}

namespace detail
{

template <typename Request, typename Response>
class RpcAwaiterStorage
{
public:
    RpcAwaiterStorage(RpcChannel* channel,
                      const google::protobuf::MethodDescriptor* method,
                      Request request,
                      RpcCallOptions options)
        : channel_(channel),
          method_(method),
          request_(std::move(request)),
          options_(options)
    {
    }

    RpcAwaiterStorage(const RpcAwaiterStorage&) = delete;
    RpcAwaiterStorage& operator=(const RpcAwaiterStorage&) = delete;

    ~RpcAwaiterStorage()
    {
        if (completion_ == nullptr)
        {
            return;
        }

        if (completion_->handle.exchange(nullptr, std::memory_order_acq_rel) == nullptr)
        {
            return;
        }

        const std::uint64_t request_id = completion_->request_id.load(std::memory_order_acquire);

        if (request_id != 0U)
        {
            channel_->CancelCoroutineCall(request_id);
        }
    }

    bool Ready() const noexcept
    {
        return false;
    }

    void Suspend(std::coroutine_handle<> handle)
    {
        channel_->Loop()->AssertInLoopThread();

        completion_ = std::make_shared<RpcCoroutineCompletion>();
        completion_->handle.store(handle, std::memory_order_release);

        if (options_.deadline.has_value())
        {
            controller_.SetDeadline(*options_.deadline);
        }
        else if (options_.timeout.count() > 0)
        {
            controller_.SetTimeout(options_.timeout);
        }

        // 协程原生入口：不再先构造 RpcClosure 再把 callback 包回协程。
        channel_->StartCoroutineCall(
            method_,
            &controller_,
            &request_,
            &response_,
            completion_);

        if (options_.flush)
        {
            channel_->Flush();
        }
    }

    [[nodiscard]] RpcController& Controller() noexcept
    {
        return controller_;
    }

    [[nodiscard]] Response&& TakeResponse() noexcept
    {
        return std::move(response_);
    }

    [[nodiscard]] const std::shared_ptr<RpcCoroutineCompletion>& Completion() const noexcept
    {
        return completion_;
    }

private:
    RpcChannel* channel_;
    const google::protobuf::MethodDescriptor* method_;
    Request request_;
    Response response_;
    RpcController controller_;
    RpcCallOptions options_;
    std::shared_ptr<RpcCoroutineCompletion> completion_;
};

}  // namespace detail

// 兼容现有 benchmark：成功直接得到 Response，失败抛 RpcError。
// 默认不自动 Flush，继续支持“先启动一批协程，再由外层统一 Flush”的显式批处理方式。
template <typename Request, typename Response>
class RpcAwaiter
{
public:
    RpcAwaiter(RpcChannel* channel,
               const google::protobuf::MethodDescriptor* method,
               Request request,
               std::chrono::milliseconds timeout = std::chrono::milliseconds(0))
        : storage_(channel,
                   method,
                   std::move(request),
                   RpcCallOptions{.timeout = timeout, .deadline = std::nullopt, .flush = false})
    {
    }

    RpcAwaiter(const RpcAwaiter&) = delete;
    RpcAwaiter& operator=(const RpcAwaiter&) = delete;

    bool await_ready() const noexcept
    {
        return storage_.Ready();
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        storage_.Suspend(handle);
    }

    Response await_resume()
    {
        RpcController& controller = storage_.Controller();
        const RpcCallState state = controller.CallState();

        if (state == RpcCallState::Cancelled)
        {
            throw RpcError(state, "RPC cancelled");
        }

        if (state == RpcCallState::Timeout || state == RpcCallState::Failed ||
            state == RpcCallState::Invalid)
        {
            throw RpcError(state, controller.ErrorText());
        }

        if (controller.Failed())
        {
            throw RpcError(RpcCallState::Failed, controller.ErrorText());
        }

        return storage_.TakeResponse();
    }

private:
    detail::RpcAwaiterStorage<Request, Response> storage_;
};

// 新业务代码推荐入口：返回 Response + 状态/错误，而不是只能拿到 Response。
// 默认 flush=true，单次调用可直接 co_await；需要显式批处理时把 flush 设为 false，
// 启动完一批调用后由上层统一 RpcChannel::Flush()。
template <typename Request, typename Response>
class RpcResultAwaiter
{
public:
    RpcResultAwaiter(RpcChannel* channel,
                     const google::protobuf::MethodDescriptor* method,
                     Request request,
                     RpcCallOptions options = {})
        : storage_(channel, method, std::move(request), options)
    {
    }

    RpcResultAwaiter(const RpcResultAwaiter&) = delete;
    RpcResultAwaiter& operator=(const RpcResultAwaiter&) = delete;

    bool await_ready() const noexcept
    {
        return storage_.Ready();
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        storage_.Suspend(handle);
    }

    RpcResult<Response> await_resume()
    {
        RpcController& controller = storage_.Controller();

        const auto& completion = storage_.Completion();

        return RpcResult<Response>(
            storage_.TakeResponse(),
            controller.CallState(),
            controller.ErrorText(),
            controller.IsCanceled(),
            completion != nullptr
                ? completion->request_id.load(std::memory_order_acquire)
                : 0U,
            completion != nullptr ? completion->trace_id : 0U,
            completion != nullptr ? completion->latency : std::chrono::microseconds{0},
            completion != nullptr ? completion->remote_error_code : 0);
    }

private:
    detail::RpcAwaiterStorage<Request, Response> storage_;
};

template <typename Request, typename Response>
[[nodiscard]] RpcResultAwaiter<Request, Response> CoCall(
    RpcChannel* channel,
    const google::protobuf::MethodDescriptor* method,
    Request request,
    RpcCallOptions options = {})
{
    return RpcResultAwaiter<Request, Response>(
        channel,
        method,
        std::move(request),
        options);
}

}  // namespace nebula::rpc
