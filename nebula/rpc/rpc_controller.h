#pragma once

#include "nebula/base/noncopyable.h"

#include <google/protobuf/service.h>
#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace nebula::rpc
{

class RpcController final : public google::protobuf::RpcController,
                            private base::Noncopyable
{
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    RpcController() = default;
    ~RpcController() override;

    void Reset() override;
    [[nodiscard]] bool Failed() const override;
    [[nodiscard]] std::string ErrorText() const override;
    void StartCancel() override;
    void SetFailed(const std::string& reason) override;
    [[nodiscard]] bool IsCanceled() const override;
    void NotifyOnCancel(google::protobuf::Closure* callback) override;

    void SetTimeout(std::chrono::milliseconds timeout);
    void SetDeadline(TimePoint deadline);
    [[nodiscard]] std::optional<std::chrono::milliseconds> Timeout() const;
    [[nodiscard]] std::optional<TimePoint> Deadline() const;

    // 返回 >=0 的 token 供 RemoveOnCancel 摘除；返回 -1 = 取消已受理、回调已内联执行（不能再摘）
    int RegisterOnCancel(google::protobuf::Closure* callback);
    void RemoveOnCancel(int token);                                       // 按 token 摘除回调并释放

    // 仅由取消完成路径抢到完成权后调用：IsCanceled() 是「取消已生效」终态，输给 response/timeout 保持 false
    void MarkCanceled();

private:
    // 摘除并释放所有尚未触发的取消回调（所有权在 controller，必须显式 delete）
    void ReleaseCancelCallbacks();

    mutable std::mutex mutex_;
    bool failed_{false};
    bool cancel_requested_{false};   // 取消已受理：StartCancel 入口置位，兼作注册互锁
    bool canceled_{false};           // 取消已生效：完成路径抢到完成权后置位，IsCanceled() 读它
    std::string error_text_;
    std::vector<std::pair<int, google::protobuf::Closure*>> cancel_callbacks_;   // token + 回调
    int next_cancel_token_{0};                                            // 递增分配，不复用
    std::optional<std::chrono::milliseconds> timeout_;
    std::optional<TimePoint> deadline_;
};

}  // namespace nebula::rpc
