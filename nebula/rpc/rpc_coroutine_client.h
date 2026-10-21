#pragma once

#include "nebula/rpc/rpc_awaiter.h"

#include <google/protobuf/descriptor.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace nebula::rpc
{

// 轻量协程 Stub：不走 generated Stub 的 done Closure 写法。
// Service 只用于拿 protobuf service descriptor；未来若增加 protoc 插件，可在生成层进一步去掉 method_name 字符串。
template <typename Service>
class CoroutineStub final
{
public:
    explicit CoroutineStub(RpcChannel* channel)
        : channel_(channel)
    {
        if (channel_ == nullptr)
        {
            throw std::invalid_argument("CoroutineStub requires a RpcChannel");
        }
    }

    template <typename Request, typename Response>
    [[nodiscard]] RpcResultAwaiter<Request, Response> Call(
        const std::string& method_name,
        Request request,
        RpcCallOptions options = {})
    {
        const google::protobuf::MethodDescriptor* method = FindMethod(Service::descriptor(), method_name);

        if (method == nullptr)
        {
            throw std::invalid_argument("RPC method not found: " + method_name);
        }

        return CoCall<Request, Response>(
            channel_,
            method,
            std::move(request),
            options);
    }

    void Flush()
    {
        channel_->Flush();
    }

private:
    RpcChannel* channel_;
};

}  // namespace nebula::rpc
