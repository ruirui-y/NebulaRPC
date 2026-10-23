#pragma once

#include "nebula/rpc/rpc_awaiter.h"

#include <google/protobuf/descriptor.h>

#include <stdexcept>
#include <string>
#include <utility>

namespace nebula::rpc
{

template <typename Service>
class CoroutineStub final
{
public:
    explicit CoroutineStub(RpcChannel* channel) : channel_(channel)
    {
        if (channel_ == nullptr)
        {
            throw std::invalid_argument("CoroutineStub requires a RpcChannel");
        }
    }

    // 将方法名解析为 MethodDescriptor，可在初始化阶段缓存，避免 RPC 热路径重复查找。
    [[nodiscard]] const google::protobuf::MethodDescriptor* ResolveMethod(const std::string& method_name) const
    {
        const google::protobuf::MethodDescriptor* method = FindMethod(Service::descriptor(), method_name);

        if (method == nullptr)
        {
            throw std::invalid_argument("RPC method not found: " + method_name);
        }

        return method;
    }

    // 推荐调用入口：直接使用已经解析好的 MethodDescriptor。
    template <typename Request, typename Response>
    [[nodiscard]] RpcResultAwaiter<Request, Response> Call(
        const google::protobuf::MethodDescriptor* method,
        Request request,
        RpcCallOptions options = {})
    {
        return CoCall<Request, Response>(channel_, method, std::move(request), options);
    }

    // 便利调用入口：按名字解析后调用，不建议在高频 RPC 热路径中重复使用。
    template <typename Request, typename Response>
    [[nodiscard]] RpcResultAwaiter<Request, Response> CallByName(
        const std::string& method_name,
        Request request,
        RpcCallOptions options = {})
    {
        return Call<Request, Response>(ResolveMethod(method_name), std::move(request), options);
    }

    void Flush()
    {
        channel_->Flush();
    }

private:
    RpcChannel* channel_;
};

}  // namespace nebula::rpc