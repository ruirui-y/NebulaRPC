#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/tcp_server.h"
#include "nebula/net/timer_id.h"
#include "nebula/rpc/rpc_codec.h"

#include <google/protobuf/service.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace nebula::net
{
class EventLoop;
}  // namespace nebula::net

namespace nebula::rpc
{

class RpcServer final : private base::Noncopyable
{
public:
    RpcServer(net::EventLoop* loop, std::string ip, std::uint16_t port);

    void RegisterService(google::protobuf::Service* service);
    void Start(std::size_t io_thread_count = 0);

    // 优雅关闭：停 accept -> 等在途请求清零（上限 10s）-> 逐个 Shutdown 连接 -> 回调
    // 可在任意线程调用；重复调用只生效一次
    void Stop();
    void SetShutdownCompleteCallback(std::function<void()> callback);

    // 转发到底层 TcpServer，建连时逐条应用到新连接
    void SetHighWatermarkCallback(net::HighWatermarkCallback cb, std::size_t high_watermark)
    {
        server_.SetHighWatermarkCallback(std::move(cb), high_watermark);
    }
    void SetMaxOutputBufferBytes(std::size_t limit) noexcept
    {
        server_.SetMaxOutputBufferBytes(limit);
    }
    void SetMaxInputBufferBytes(std::size_t limit) noexcept
    {
        server_.SetMaxInputBufferBytes(limit);
    }

private:
    struct ServerCall;

    void OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer);
    void HandleRequest(const net::TcpConnectionPtr& conn, const RpcFrame& frame);
    void SendResponse(const net::TcpConnectionPtr& conn,
                      std::uint64_t request_id,
                      const std::string& trace_id,
                      const google::protobuf::Message& response);
    void SendError(const net::TcpConnectionPtr& conn,
                   std::uint64_t request_id,
                   const std::string& trace_id,
                   int error_code,
                   std::string error_text);
    void SendHeartbeatEcho(const net::TcpConnectionPtr& conn, const proto::RpcMeta& request_meta);

    void StopInLoop();
    void CloseConnectionsInLoop();
    void FinishShutdownInLoop(std::string_view reason);
    void OnCallFinished();

    net::EventLoop* loop_;
    net::TcpServer server_;
    std::unordered_map<std::string, google::protobuf::Service*> services_;

    // 在途请求计数：done 回调可能在业务线程跑，用 atomic 让关闭决策看到最新值
    std::atomic_uint64_t in_flight_{0};
    std::atomic_bool stopping_{false};
    bool connections_closed_{false};
    bool shutdown_finished_{false};
    net::TimerId grace_timer_;
    std::function<void()> shutdown_complete_callback_;
};

}  // namespace nebula::rpc
