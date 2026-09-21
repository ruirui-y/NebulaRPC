#include "nebula/rpc/rpc_controller.h"

namespace nebula::rpc {

void RpcController::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    failed_ = false;
    canceled_ = false;
    error_text_.clear();
    cancel_callbacks_.clear();
}

bool RpcController::Failed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return failed_;
}

std::string RpcController::ErrorText() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return error_text_;
}

void RpcController::StartCancel() {
    std::vector<google::protobuf::Closure*> callbacks;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (canceled_) {
            return;
        }
        canceled_ = true;
        callbacks.swap(cancel_callbacks_);
    }

    for (auto* callback : callbacks) {
        if (callback != nullptr) {
            callback->Run();
        }
    }
}

void RpcController::SetFailed(const std::string& reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    failed_ = true;
    error_text_ = reason;
}

bool RpcController::IsCanceled() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return canceled_;
}

void RpcController::NotifyOnCancel(google::protobuf::Closure* callback) {
    if (callback == nullptr) {
        return;
    }

    bool run_now = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (canceled_) {
            run_now = true;
        } else {
            cancel_callbacks_.push_back(callback);
        }
    }

    if (run_now) {
        callback->Run();
    }
}

}  // namespace nebula::rpc
