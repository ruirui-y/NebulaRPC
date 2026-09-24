#include "nebula/rpc/rpc_controller.h"

namespace nebula::rpc
{

RpcController::~RpcController()
{
    // 不能指望 RpcChannel 摘干净：漏摘的闭包会永久留在堆上，而 controller 常是短命的调用方对象
    ReleaseCancelCallbacks();
}

void RpcController::Reset()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        failed_ = false;
        cancel_requested_ = false;
        canceled_ = false;
        error_text_.clear();
        timeout_.reset();
        deadline_.reset();
    }

    // 复用前清掉尚未触发的取消回调，所有权在 controller，需释放
    ReleaseCancelCallbacks();
}

void RpcController::ReleaseCancelCallbacks()
{
    std::vector<std::pair<int, google::protobuf::Closure*>> callbacks;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        callbacks.swap(cancel_callbacks_);
    }

    // 锁外释放：closure 的析构可能回调业务代码，不该持有 mutex_
    for (auto& [token, callback] : callbacks)
    {
        (void)token;
        delete callback;
    }
}

bool RpcController::Failed() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return failed_;
}

std::string RpcController::ErrorText() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return error_text_;
}

void RpcController::StartCancel()
{
    std::vector<std::pair<int, google::protobuf::Closure*>> callbacks;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 互锁用 cancel_requested_，须在 swap 之前置位：否则「列表已换走、标志还是 false」窗口里注册的回调没人执行
        if (cancel_requested_)
        {
            return;
        }

        cancel_requested_ = true;

        // 整体换走列表、锁外执行：所有权当场转移，RemoveOnCancel 之后摘不到，不会 double free
        callbacks.swap(cancel_callbacks_);
    }

    for (auto& [token, callback] : callbacks)
    {
        (void)token;

        if (callback != nullptr)
        {
            callback->Run();
        }
    }
}

void RpcController::SetFailed(const std::string& reason)
{
    std::lock_guard<std::mutex> lock(mutex_);

    failed_ = true;
    error_text_ = reason;
}

bool RpcController::IsCanceled() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return canceled_;
}

void RpcController::MarkCanceled()
{
    // 迟到的取消会被 TryComplete 挡掉、IsCanceled() 保持 false，不会与「response 已被填充」同时成立
    std::lock_guard<std::mutex> lock(mutex_);
    canceled_ = true;
}

void RpcController::NotifyOnCancel(google::protobuf::Closure* callback)
{
    if (callback == nullptr)
    {
        return;
    }

    // 转调带 token 的实现；-1 表示取消已受理、回调已立即执行
    (void)RegisterOnCancel(callback);
}

int RpcController::RegisterOnCancel(google::protobuf::Closure* callback)
{
    if (callback == nullptr)
    {
        return -1;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!cancel_requested_)
        {
            // 取消尚未受理：分配 token 存入列表，回调所有权移交给 controller
            const int token = next_cancel_token_++;
            cancel_callbacks_.emplace_back(token, callback);
            return token;
        }
    }

    // 取消已受理：立即执行，所有权随执行转移（RpcClosure 自删除），调用方不得再摘除
    callback->Run();
    return -1;
}

void RpcController::RemoveOnCancel(int token)
{
    google::protobuf::Closure* callback = nullptr;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 找不到是正常情况：回调可能已被 StartCancel 换走执行
        for (auto it = cancel_callbacks_.begin(); it != cancel_callbacks_.end(); ++it)
        {
            if (it->first == token)
            {
                callback = it->second;
                cancel_callbacks_.erase(it);
                break;
            }
        }
    }

    // 摘除方负责释放，与 StartCancel 的换走执行互斥，不会 double free
    delete callback;
}

void RpcController::SetTimeout(std::chrono::milliseconds timeout)
{
    if (timeout.count() < 0)
    {
        timeout = std::chrono::milliseconds(0);
    }

    std::lock_guard<std::mutex> lock(mutex_);

    timeout_ = timeout;
    deadline_.reset();
}

void RpcController::SetDeadline(TimePoint deadline)
{
    std::lock_guard<std::mutex> lock(mutex_);

    deadline_ = deadline;
    timeout_.reset();
}

std::optional<std::chrono::milliseconds> RpcController::Timeout() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return timeout_;
}

std::optional<RpcController::TimePoint> RpcController::Deadline() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return deadline_;
}

}  // namespace nebula::rpc
