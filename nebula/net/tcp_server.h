#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/acceptor.h"
#include "nebula/net/callbacks.h"
#include "nebula/net/event_loop_thread_pool.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

namespace nebula::net
{

class EventLoop;

class TcpServer final : private base::Noncopyable
{
public:
    TcpServer(EventLoop* loop, std::string ip, std::uint16_t port, bool reuse_port = true);
    ~TcpServer();

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

    void Start(std::size_t io_thread_count = 0);

private:
    void NewConnection(int socket_fd);
    void RemoveConnection(const TcpConnectionPtr& conn);
    void RemoveConnectionInLoop(const TcpConnectionPtr& conn);

    EventLoop* loop_;
    std::unique_ptr<Acceptor> acceptor_;
    std::unique_ptr<EventLoopThreadPool> thread_pool_;
    ConnectionCallback connection_callback_;
    MessageCallback message_callback_;
    WriteCompleteCallback write_complete_callback_;
    bool started_{false};
    std::uint64_t next_connection_id_{1};
    std::unordered_map<std::string, TcpConnectionPtr> connections_;
};

}  // namespace nebula::net
