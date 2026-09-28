#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/callbacks.h"
#include "nebula/net/timer_id.h"
#include "nebula/rpc/rpc_call.h"
#include "nebula/rpc/rpc_codec.h"
#include "nebula/rpc/rpc_metrics.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/service.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace nebula::net
{
class Buffer;
class EventLoop;
class TcpClient;
}  // namespace nebula::net

namespace nebula::rpc
{

// 单连接 RPC 通道：连接不可用时当场失败并给出原因，不排队、不重发
class RpcChannel final : public google::protobuf::RpcChannel,
                         private base::Noncopyable
{
public:
    RpcChannel(net::EventLoop* loop, std::string ip, std::uint16_t port);
    ~RpcChannel() override;

    // 属主 loop：完成回调与协程恢复都跑在它的线程上
    [[nodiscard]] net::EventLoop* Loop() const noexcept
    {
        return loop_;
    }

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

    // 过载闸门，0 表示不限制
    void SetMaxPendingCalls(std::size_t max_pending_calls) noexcept;

    // 设了钩子即启用降级：过载时先问钩子，返回 false 回落为直接失败
    // 返回 true 表示钩子已自行完成该次调用（填了 response 或调了 SetFailed）
    using DegradeHandler = std::function<bool(google::protobuf::RpcController*,
                                              google::protobuf::Message*,
                                              const std::string&)>;
    void SetDegradeHandler(DegradeHandler handler);

    // 周期输出 QPS 与分位数；interval 为 0 表示关闭
    void StartMetricsReport(std::chrono::milliseconds interval);
    void StopMetricsReport();

    [[nodiscard]] std::size_t PendingCallCount() const noexcept;
    [[nodiscard]] std::uint64_t OverloadRejectCount() const noexcept;

private:
    using TimePoint = std::chrono::steady_clock::time_point;

    struct PendingCall
    {
        google::protobuf::Message* response{};
        google::protobuf::RpcController* controller{};
        google::protobuf::Closure* done{};
        // 描述符指向生成代码里的静态对象，进程内长存，存指针不额外分配
        const google::protobuf::MethodDescriptor* method{};
        std::optional<TimePoint> deadline;
        TimePoint sent_at{};
        net::TimerId timeout_timer;
        int cancel_token{-1};                                                           // 注册在 RpcController 上的取消回调编号，-1 表示未注册
        RpcCall call;                                                                   // 完成权仲裁：response/timeout/cancel/disconnect 只能赢一个
    };

    // 前两种都拒发，区别只在原因能不能自愈：kConnecting 由 Connector 重连，kDisconnected 永久废
    enum class ConnectionState
    {
        kConnecting,
        kConnected,
        kDisconnected,
    };

    void RegisterAndSend(std::uint64_t request_id, std::string bytes, PendingCall pending_call);
    void RejectOverloaded(google::protobuf::RpcController* controller,
                          google::protobuf::Message* response,
                          google::protobuf::Closure* done,
                          const std::string& reason);
    void OnConnection(const net::TcpConnectionPtr& conn);
    void OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer);
    void OnTimeout(std::uint64_t request_id);
    void HandleFrame(RpcFrame frame);

    void ScheduleHeartbeatInLoop();
    void CancelHeartbeatInLoop();
    void OnHeartbeatTimer();
    void SendHeartbeatInLoop();

    void ReportMetricsInLoop();

    // 存活守卫：先 lock 再 Acquire；是 atomic 因为写与读不在同一线程
    struct AliveGuard
    {
        // 返回 nullptr 即 channel 已析构，调用方必须放弃
        RpcChannel* Acquire() const
        {
            return channel.load(std::memory_order_acquire);
        }

        std::atomic<RpcChannel*> channel{nullptr};
    };

    std::optional<PendingCall> TakePendingCall(std::uint64_t request_id);
    void CompleteCallWithFrame(std::uint64_t request_id, RpcFrame frame);
    void CompleteCallWithFailure(std::uint64_t request_id,
                                 const std::string& reason,
                                 RpcCallState state);
    void CompleteCallWithCancel(std::uint64_t request_id);

    // 返回实测延迟，日志与指标共用同一份数字
    [[nodiscard]] std::chrono::microseconds RecordMetrics(const PendingCall& pending_call,
                                                          RpcCallState state);

    // 完成日志的固定字段：request_id / service / method / latency_ms / error
    void LogCompletion(std::uint64_t request_id,
                       const PendingCall& pending_call,
                       std::string_view outcome,
                       std::chrono::microseconds latency,
                       std::string_view error);

    void FailAllPending(const std::string& reason);
    void FailAllPendingNow(const std::string& reason);

    net::EventLoop* loop_;
    std::unique_ptr<net::TcpClient> client_;
    net::TcpConnectionPtr connection_;
    std::shared_ptr<AliveGuard> alive_guard_;

    // 只允许 EventLoop owner thread 访问，因此 response/timeout 天然串行竞争。
    std::unordered_map<std::uint64_t, PendingCall> pending_calls_;

    ConnectionState connection_state_{ConnectionState::kConnecting};
    std::string connection_error_;   // 只有 kDisconnected 读它：这条路永久废掉的原因

    std::size_t max_pending_calls_{0};
    DegradeHandler degrade_handler_;
    std::atomic_uint64_t overload_reject_count_{0};

    RpcMetrics metrics_;
    TimePoint metrics_window_start_{};
    std::chrono::milliseconds metrics_interval_{0};
    net::TimerId metrics_timer_;

    int heartbeat_missed_{0};
    net::TimerId heartbeat_timer_;

    inline static std::atomic_uint64_t next_request_id_{1};
};

}  // namespace nebula::rpc
