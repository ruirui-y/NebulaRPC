#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/callbacks.h"
#include "nebula/net/timer_id.h"
#include "nebula/rpc/rpc_codec.h"

#include <google/protobuf/service.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
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

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    using TimePoint = std::chrono::steady_clock::time_point;

    struct PendingCall
    {
        google::protobuf::Message* response{};
        google::protobuf::RpcController* controller{};
        google::protobuf::Closure* done{};
        std::optional<TimePoint> deadline;
        net::TimerId timeout_timer;
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

    void RegisterAndSend(std::uint64_t request_id,
                         std::string bytes,
                         PendingCall pending_call);
    void OnConnection(const net::TcpConnectionPtr& conn);
    void OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer);
    void OnConnectError(const std::string& reason);
    void OnTimeout(std::uint64_t request_id);
    void HandleFrame(RpcFrame frame);

    std::optional<PendingCall> TakePendingCall(std::uint64_t request_id);
    void CompleteCallWithFrame(std::uint64_t request_id, RpcFrame frame);
    void CompleteCallWithFailure(std::uint64_t request_id,
                                 const std::string& reason);
    void RemovePendingWrite(std::uint64_t request_id);

    void FailAllPending(const std::string& reason);
    void FailAllPendingNow(const std::string& reason);

    net::EventLoop* loop_;
    std::unique_ptr<net::TcpClient> client_;
    net::TcpConnectionPtr connection_;

    // 只允许 EventLoop owner thread 访问，因此 response/timeout 天然串行竞争。
    std::unordered_map<std::uint64_t, PendingCall> pending_calls_;
    std::deque<PendingWrite> pending_writes_;

    ConnectionState connection_state_{ConnectionState::kConnecting};
    std::string connection_error_;

    inline static std::atomic_uint64_t next_request_id_{1};
};

}  // namespace nebula::rpc
