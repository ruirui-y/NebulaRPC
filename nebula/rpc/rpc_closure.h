#pragma once

#include <google/protobuf/service.h>

#include <functional>
#include <utility>

namespace nebula::rpc
{

class RpcClosure final : public google::protobuf::Closure
{
public:
    explicit RpcClosure(std::function<void()> callback)
        : callback_(std::move(callback))
        {
        }

    void Run() override
    {
        auto callback = std::move(callback_);
        delete this;
        if (callback)
        {
            callback();
        }
    }

private:
    std::function<void()> callback_;
};

}  // namespace nebula::rpc
