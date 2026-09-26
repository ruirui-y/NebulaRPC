#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/callbacks.h"
#include "nebula/net/timer_id.h"
#include "nebula/rpc/rpc_call.h"
#include "nebula/rpc/rpc_codec.h"

#include <google/protobuf/service.h>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace nebula::net
{
class Buffer;
class EventLoop;
class TcpClient;
}  // namespace nebula::net

namespace nebula::rpc
{

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

    // 过载闸门，0 表示不限制：在途调用数 / 未建连时的排队写数
    void SetMaxPendingCalls(std::size_t max_pending_calls) noexcept;
    void SetMaxPendingWrites(std::size_t max_pending_writes) noexcept;

    // 设了钩子即启用降级：过载时先问钩子，返回 false 回落为直接失败
    // 返回 true 表示钩子已自行完成该次调用（填了 response 或调了 SetFailed）
    using DegradeHandler = std::function<bool(google::protobuf::RpcController*,
                                              google::protobuf::Message*,
                                              const std::string&)>;
    void SetDegradeHandler(DegradeHandler handler);

    [[nodiscard]] std::size_t PendingCallCount() const noexcept;
    [[nodiscard]] std::size_t PendingWriteCount() const noexcept;
    [[nodiscard]] std::uint64_t OverloadRejectCount() const noexcept;

private:
    using TimePoint = std::chrono::steady_clock::time_point;

    struct PendingCall
    {
        google::protobuf::Message* response{};
        google::protobuf::RpcController* controller{};
        google::protobuf::Closure* done{};
        std::optional<TimePoint> deadline;
        net::TimerId timeout_timer;
        int cancel_token{-1};   // 注册在 RpcController 上的取消回调编号，-1 表示未注册
        RpcCall call;           // 完成权仲裁：response/timeout/cancel/disconnect 只能赢一个
    };

    struct PendingWrite
    {
        std::uint64_t request_id{};
        std::string bytes;
    };

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
    void OnConnectError(const std::string& reason);
    void OnTimeout(std::uint64_t request_id);
    void HandleFrame(RpcFrame frame);

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
    void RemovePendingWrite(std::uint64_t request_id);

    void FailAllPending(const std::string& reason);
    void FailAllPendingNow(const std::string& reason);

    net::EventLoop* loop_;
    std::unique_ptr<net::TcpClient> client_;
    net::TcpConnectionPtr connection_;
    std::shared_ptr<AliveGuard> alive_guard_;

    // 只允许 EventLoop owner thread 访问，因此 response/timeout 天然串行竞争。
    std::unordered_map<std::uint64_t, PendingCall> pending_calls_;
    std::deque<PendingWrite> pending_writes_;

    ConnectionState connection_state_{ConnectionState::kConnecting};
    std::string connection_error_;

    std::size_t max_pending_calls_{0};
    std::size_t max_pending_writes_{0};
    DegradeHandler degrade_handler_;
    std::atomic_uint64_t overload_reject_count_{0};

    inline static std::atomic_uint64_t next_request_id_{1};
};

}  // namespace nebula::rpc
