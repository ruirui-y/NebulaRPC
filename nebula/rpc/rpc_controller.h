#pragma once

#include "nebula/base/noncopyable.h"

#include <google/protobuf/service.h>

#include <mutex>
#include <string>
#include <vector>

namespace nebula::rpc {

class RpcController final : public google::protobuf::RpcController,
                            private base::Noncopyable {
public:
    RpcController() = default;
    ~RpcController() override = default;

    void Reset() override;
    [[nodiscard]] bool Failed() const override;
    [[nodiscard]] std::string ErrorText() const override;
    void StartCancel() override;
    void SetFailed(const std::string& reason) override;
    [[nodiscard]] bool IsCanceled() const override;
    void NotifyOnCancel(google::protobuf::Closure* callback) override;

private:
    mutable std::mutex mutex_;
    bool failed_{false};
    bool canceled_{false};
    std::string error_text_;
    std::vector<google::protobuf::Closure*> cancel_callbacks_;
};

}  // namespace nebula::rpc
