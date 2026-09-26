#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/buffer.h"
#include "nebula/net/callbacks.h"
#include "nebula/net/socket.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace nebula::net
{

class Channel;
class EventLoop;

class TcpConnection final : public std::enable_shared_from_this<TcpConnection>,
                            private base::Noncopyable {
public:
    TcpConnection(EventLoop* loop, int socket_fd, std::string name);
    ~TcpConnection();

    [[nodiscard]] EventLoop* Loop() const noexcept
    {
        return loop_;
    }
    [[nodiscard]] const std::string& Name() const noexcept
    {
        return name_;
    }
    [[nodiscard]] bool Connected() const noexcept;

    void Send(std::string_view data);
    void Shutdown();

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
    void SetCloseCallback(CloseCallback cb)
    {
        close_callback_ = std::move(cb);
    }

    // 软水位：待发字节首次越线通知一次，恢复靠 WriteCompleteCallback
    void SetHighWatermarkCallback(HighWatermarkCallback cb, std::size_t high_watermark);
    // 硬上限：待发字节越线即断连并丢弃待发数据，0 表示不限制
    void SetMaxOutputBufferBytes(std::size_t limit) noexcept;
    // 输入水位：单次读事件后残留的可读字节仍越线即断连，0 表示不限制
    void SetMaxInputBufferBytes(std::size_t limit) noexcept;

    void ForceClose();

    // 只能在所属 loop 线程读
    [[nodiscard]] std::size_t PendingOutputBytes() const noexcept;
    [[nodiscard]] std::uint64_t HighWatermarkCount() const noexcept;
    [[nodiscard]] std::uint64_t OverloadCloseCount() const noexcept;

    void ConnectEstablished();
    void ConnectDestroyed();

private:
    enum class State
    {
        kConnecting,
        kConnected,
        kDisconnecting,
        kDisconnected
    };

    void SetState(State state) noexcept
    {
        state_.store(state);
    }
    void HandleRead();
    void HandleWrite();
    void HandleClose();
    void HandleError();
    void SendInLoop(std::string data);
    void ShutdownInLoop();
    void ForceCloseInLoop();
    void NotifyHighWatermark(std::size_t before, std::size_t after);

    EventLoop* loop_;
    const std::string name_;
    std::atomic<State> state_{State::kConnecting};
    Socket socket_;
    std::unique_ptr<Channel> channel_;

    Buffer input_buffer_;
    Buffer output_buffer_;

    ConnectionCallback connection_callback_;
    MessageCallback message_callback_;
    WriteCompleteCallback write_complete_callback_;
    CloseCallback close_callback_;
    HighWatermarkCallback high_watermark_callback_;

    std::size_t high_watermark_{0};
    std::size_t max_output_buffer_bytes_{0};
    std::size_t max_input_buffer_bytes_{0};
    std::atomic_uint64_t high_watermark_count_{0};
    std::atomic_uint64_t overload_close_count_{0};
};

}  // namespace nebula::net
