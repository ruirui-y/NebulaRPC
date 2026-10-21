#pragma once

#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_task.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

namespace nebula::rpc
{

class RpcServerContext final
{
public:
    RpcServerContext(net::EventLoop* loop,
                     std::uint64_t request_id,
                     std::string trace_id,
                     std::string connection_name)
        : loop_(loop),
          request_id_(request_id),
          trace_id_(std::move(trace_id)),
          connection_name_(std::move(connection_name))
    {
    }

    [[nodiscard]] net::EventLoop* Loop() const noexcept
    {
        return loop_;
    }

    [[nodiscard]] std::uint64_t RequestId() const noexcept
    {
        return request_id_;
    }

    [[nodiscard]] const std::string& TraceId() const noexcept
    {
        return trace_id_;
    }

    [[nodiscard]] const std::string& ConnectionName() const noexcept
    {
        return connection_name_;
    }

private:
    net::EventLoop* loop_;
    std::uint64_t request_id_;
    std::string trace_id_;
    std::string connection_name_;
};

// 新服务推荐入口：业务 handler 返回 Task<Response>，不再接收 done Closure。
// google::protobuf::Service 仍保留给旧 examples/tests 做兼容，新代码不需要继承它。
class CoroutineService final
{
public:
    struct Method final
    {
        const google::protobuf::MethodDescriptor* descriptor{};
        std::function<std::unique_ptr<google::protobuf::Message>()> new_request;
        std::function<Task<std::unique_ptr<google::protobuf::Message>>(
            RpcServerContext,
            std::unique_ptr<google::protobuf::Message>)> invoke;
    };

    explicit CoroutineService(const google::protobuf::ServiceDescriptor* descriptor)
        : descriptor_(descriptor)
    {
        if (descriptor_ == nullptr)
        {
            throw std::invalid_argument("CoroutineService requires a service descriptor");
        }
    }

    CoroutineService(const CoroutineService&) = delete;
    CoroutineService& operator=(const CoroutineService&) = delete;

    [[nodiscard]] const google::protobuf::ServiceDescriptor* Descriptor() const noexcept
    {
        return descriptor_;
    }

    template <typename Request, typename Response, typename Handler>
    void Register(const std::string& method_name, Handler&& handler)
    {
        static_assert(std::is_base_of_v<google::protobuf::Message, Request>);
        static_assert(std::is_base_of_v<google::protobuf::Message, Response>);

        const auto* method = descriptor_->FindMethodByName(method_name);
        if (method == nullptr)
        {
            throw std::invalid_argument("method not found in service descriptor: " + method_name);
        }

        if (method->input_type()->full_name() != Request::descriptor()->full_name() ||
            method->output_type()->full_name() != Response::descriptor()->full_name())
        {
            throw std::invalid_argument("registered coroutine method type does not match protobuf descriptor: " + method_name);
        }

        using HandlerType = std::decay_t<Handler>;
        using HandlerResult = std::invoke_result_t<
            HandlerType&,
            RpcServerContext&,
            const Request&>;

        static_assert(
            std::is_same_v<HandlerResult, Task<Response>>,
            "CoroutineService handler must return Task<Response>");

        auto shared_handler = std::make_shared<HandlerType>(std::forward<Handler>(handler));

        Method entry;
        entry.descriptor = method;
        entry.new_request = []
            {
                return std::make_unique<Request>();
            };

        entry.invoke = [shared_handler](RpcServerContext context,
                                        std::unique_ptr<google::protobuf::Message> request)
            -> Task<std::unique_ptr<google::protobuf::Message>>
            {
                auto* typed_request = dynamic_cast<Request*>(request.get());
                if (typed_request == nullptr)
                {
                    throw std::runtime_error("coroutine service request type mismatch");
                }

                Response response = co_await (*shared_handler)(context, *typed_request);
                co_return std::make_unique<Response>(std::move(response));
            };

        methods_[method_name] = std::move(entry);
    }

    [[nodiscard]] const Method* FindMethod(const std::string& method_name) const noexcept
    {
        const auto it = methods_.find(method_name);
        return it == methods_.end() ? nullptr : &it->second;
    }

private:
    const google::protobuf::ServiceDescriptor* descriptor_;
    std::unordered_map<std::string, Method> methods_;
};

}  // namespace nebula::rpc
