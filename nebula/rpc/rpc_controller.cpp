#include "nebula/rpc/rpc_controller.h"

namespace nebula::rpc
{

void RpcController::Reset()
{
    std::lock_guard<std::mutex> lock(mutex_);

    failed_ = false;
    canceled_ = false;
    error_text_.clear();
    cancel_callbacks_.clear();
    timeout_.reset();
    deadline_.reset();
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
    std::vector<google::protobuf::Closure*> callbacks;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (canceled_)
        {
            return;
        }

        canceled_ = true;
        callbacks.swap(cancel_callbacks_);
    }

    for (auto* callback : callbacks)
    {
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

void RpcController::NotifyOnCancel(google::protobuf::Closure* callback)
{
    if (callback == nullptr)
    {
        return;
    }

    bool run_now = false;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (canceled_)
        {
            run_now = true;
        }
        else
        {
            cancel_callbacks_.push_back(callback);
        }
    }

    if (run_now)
    {
        callback->Run();
    }
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
