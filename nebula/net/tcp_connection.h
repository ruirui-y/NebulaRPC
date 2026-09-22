#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/buffer.h"
#include "nebula/net/callbacks.h"
#include "nebula/net/socket.h"

#include <atomic>
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
};

}  // namespace nebula::net
