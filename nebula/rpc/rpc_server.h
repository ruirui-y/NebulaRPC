#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/tcp_server.h"
#include "nebula/rpc/rpc_codec.h"

#include <google/protobuf/service.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace nebula::rpc
{

class RpcServer final : private base::Noncopyable
{
public:
    RpcServer(net::EventLoop* loop, std::string ip, std::uint16_t port);

    void RegisterService(google::protobuf::Service* service);
    void Start(std::size_t io_thread_count = 0);

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
                      const google::protobuf::Message& response);
    void SendError(const net::TcpConnectionPtr& conn,
                   std::uint64_t request_id,
                   int error_code,
                   std::string error_text);

    net::TcpServer server_;
    std::unordered_map<std::string, google::protobuf::Service*> services_;
};

}  // namespace nebula::rpc
