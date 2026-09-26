#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/callbacks.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace nebula::net
{

class Connector;
class EventLoop;

class TcpClient final : private base::Noncopyable
{
public:
    using ConnectErrorCallback = std::function<void(const std::string&)>;

    TcpClient(EventLoop* loop, std::string ip, std::uint16_t port);
    ~TcpClient();

    void SetConnectionCallback(ConnectionCallback cb)
    {
        connection_callback_ = std::move(cb);
    }
    void SetMessageCallback(MessageCallback cb)
    {
        message_callback_ = std::move(cb);
    }
    void SetWriteCompleteCallback(WriteCompleteCallback cb)
    {
        write_complete_callback_ = std::move(cb);
    }
    void SetConnectErrorCallback(ConnectErrorCallback cb)
    {
        connect_error_callback_ = std::move(cb);
    }

    // 以下三个配置在建连成功时逐条应用到新连接
    void SetHighWatermarkCallback(HighWatermarkCallback cb, std::size_t high_watermark)
    {
        high_watermark_callback_ = std::move(cb);
        high_watermark_ = high_watermark;
    }
    void SetMaxOutputBufferBytes(std::size_t limit) noexcept
    {
        max_output_buffer_bytes_ = limit;
    }
    void SetMaxInputBufferBytes(std::size_t limit) noexcept
    {
        max_input_buffer_bytes_ = limit;
    }

    void Connect();
    void Disconnect();
    void Stop();

    [[nodiscard]] const TcpConnectionPtr& Connection() const noexcept
    {
        return connection_;
    }

private:
    void NewConnection(int socket_fd);
    void RemoveConnection(const TcpConnectionPtr& conn);
    void HandleConnectError(const std::string& reason);

    EventLoop* loop_;
    std::shared_ptr<Connector> connector_;

    ConnectionCallback connection_callback_;
    MessageCallback message_callback_;
    WriteCompleteCallback write_complete_callback_;
    ConnectErrorCallback connect_error_callback_;

    HighWatermarkCallback high_watermark_callback_;
    std::size_t high_watermark_{0};
    std::size_t max_output_buffer_bytes_{0};
    std::size_t max_input_buffer_bytes_{0};

    TcpConnectionPtr connection_;
    bool connect_{false};
    std::uint64_t next_connection_id_{1};
};

}  // namespace nebula::net
