#pragma once

#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"
#include "nebula/rpc/rpc_error.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

#include <atomic>
#include <chrono>
#include <coroutine>
#include <memory>
#include <string>
#include <utility>

namespace nebula::rpc
{

// 按名字查方法描述符：service 为空或方法不存在都返回 nullptr
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

// 协程恢复点：closure 与 awaiter 各持一份 shared_ptr，谁先拿走 handle 谁负责恢复
struct ResumeGuard
{
    std::atomic<std::coroutine_handle<>> handle{};   // 非空 = 协程还挂着没恢复
};

// 把一次异步 RPC 变成可 co_await 的 awaiter：完成路径仍走 done，协程只是换了个收尾方式
template <typename Request, typename Response>
class RpcAwaiter
{
public:
    RpcAwaiter(RpcChannel* channel,
               const google::protobuf::MethodDescriptor* method,
               Request request,
               std::chrono::milliseconds timeout = std::chrono::milliseconds(0))
        : channel_(channel),
          method_(method),
          request_(std::move(request)),
          timeout_(timeout)
    {
    }

    RpcAwaiter(const RpcAwaiter&) = delete;
    RpcAwaiter& operator=(const RpcAwaiter&) = delete;

    // 析构即取消：帧被销毁（Task 中途丢弃）时若还挂着，撤掉在途调用，避免 channel 回写已释放对象
    ~RpcAwaiter()
    {
        if (guard_ != nullptr && guard_->handle.exchange(nullptr) != nullptr)
        {
            controller_.StartCancel();
        }
    }

    bool await_ready() const noexcept
    {
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        // 契约：co_await 必须发生在 channel 的属主 loop 线程，否则恢复可能与挂起并发
        channel_->Loop()->AssertInLoopThread();

        guard_ = std::make_shared<ResumeGuard>();
        guard_->handle.store(handle, std::memory_order_release);

        if (timeout_.count() > 0)
        {
            controller_.SetTimeout(timeout_);
        }

        // done 是完成路径唯一出口：只要是个 Closure，response/超时/取消/断连都自动覆盖
        auto* done = new RpcClosure([guard = guard_, loop = channel_->Loop()]
            {
                // 推迟到队列再恢复：CallMethod 可能同步失败并内联跑完 done，直接 resume 会重入
                loop->QueueInLoop([guard]
                    {
                        if (const std::coroutine_handle<> resumed =
                                guard->handle.exchange(nullptr);
                            resumed != nullptr)
                        {
                            resumed.resume();
                        }
                    });
            });

        channel_->CallMethod(method_, &controller_, &request_, &response_, done);
    }

    Response await_resume()
    {
        const RpcCallState state = controller_.CallState();

        if (state == RpcCallState::Cancelled)
        {
            throw RpcError(state, "RPC cancelled");
        }

        if (state == RpcCallState::Timeout || state == RpcCallState::Failed)
        {
            throw RpcError(state, controller_.ErrorText());
        }

        // 仲裁赢了但业务失败：服务端 ERROR 帧、响应解析失败等
        if (controller_.Failed())
        {
            throw RpcError(RpcCallState::Failed, controller_.ErrorText());
        }

        return std::move(response_);
    }

private:
    RpcChannel* channel_;
    const google::protobuf::MethodDescriptor* method_;
    Request request_;
    Response response_;
    RpcController controller_;
    std::chrono::milliseconds timeout_;
    std::shared_ptr<ResumeGuard> guard_;
};

}  // namespace nebula::rpc
