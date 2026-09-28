#include "nebula/net/tcp_server.h"

#include "nebula/base/logger.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"

#include <sstream>
#include <utility>

namespace nebula::net
{

TcpServer::TcpServer(EventLoop* loop, std::string ip, std::uint16_t port, bool reuse_port)
    : loop_(loop),
      acceptor_(std::make_unique<Acceptor>(loop, std::move(ip), port, reuse_port)),
      thread_pool_(std::make_unique<EventLoopThreadPool>(loop))
{
    acceptor_->SetNewConnectionCallback([this](int socket_fd)
        {
            NewConnection(socket_fd);
        });
}

TcpServer::~TcpServer()
{
    for (auto& [name, connection] : connections_)
    {
        (void)name;
        auto conn = connection;
        conn->Loop()->RunInLoop([conn]
            {
                conn->ConnectDestroyed();
            });
    }
}

void TcpServer::Start(std::size_t io_thread_count)
{
    if (started_)
    {
        return;
    }
    started_ = true;

    thread_pool_->Start(io_thread_count);
    loop_->RunInLoop([this]
        {
            if (!acceptor_->Listening())
            {
                acceptor_->Listen();
            }
        });
}

void TcpServer::Stop()
{
    loop_->RunInLoop([this]
        {
            StopInLoop();
        });
}

void TcpServer::StopInLoop()
{
    loop_->AssertInLoopThread();

    if (stopping_)
    {
        return;
    }
    stopping_ = true;

    NLOG_INFO("tcp server stop accepting connections={}", connections_.size());

    acceptor_->Stop();
}

void TcpServer::CloseAllConnections(std::function<void()> on_all_closed)
{
    loop_->RunInLoop([this, callback = std::move(on_all_closed)]() mutable
        {
            CloseAllConnectionsInLoop(std::move(callback));
        });
}

void TcpServer::CloseAllConnectionsInLoop(std::function<void()> on_all_closed)
{
    loop_->AssertInLoopThread();

    if (closing_)
    {
        return;
    }
    closing_ = true;
    all_closed_callback_ = std::move(on_all_closed);

    // 连接分散在各自的 io loop 上，Shutdown 自己会投递过去
    for (auto& [name, connection] : connections_)
    {
        (void)name;
        connection->Shutdown();
    }

    CheckAllClosedInLoop();
}

void TcpServer::CheckAllClosedInLoop()
{
    loop_->AssertInLoopThread();

    if (!closing_ || !connections_.empty())
    {
        return;
    }

    closing_ = false;

    if (all_closed_callback_)
    {
        // 先换出来再调用：回调里可能又发起一次关闭
        auto callback = std::move(all_closed_callback_);
        all_closed_callback_ = {};
        callback();
    }
}

void TcpServer::NewConnection(int socket_fd)
{
    loop_->AssertInLoopThread();
    EventLoop* io_loop = thread_pool_->GetNextLoop();

    std::ostringstream name;
    name << "conn-" << next_connection_id_++;
    auto connection = std::make_shared<TcpConnection>(io_loop, socket_fd, name.str());
    connections_.emplace(connection->Name(), connection);

    connection->SetConnectionCallback(connection_callback_);
    connection->SetMessageCallback(message_callback_);
    connection->SetWriteCompleteCallback(write_complete_callback_);
    connection->SetHighWatermarkCallback(high_watermark_callback_, high_watermark_);
    connection->SetMaxOutputBufferBytes(max_output_buffer_bytes_);
    connection->SetMaxInputBufferBytes(max_input_buffer_bytes_);
    connection->SetCloseCallback([this](const TcpConnectionPtr& conn)
        {
            RemoveConnection(conn);
        });

    io_loop->RunInLoop([connection]
        {
            connection->ConnectEstablished();
        });
}

void TcpServer::RemoveConnection(const TcpConnectionPtr& conn)
{
    loop_->RunInLoop([this, conn]
        {
            RemoveConnectionInLoop(conn);
        });
}

void TcpServer::RemoveConnectionInLoop(const TcpConnectionPtr& conn)
{
    loop_->AssertInLoopThread();
    connections_.erase(conn->Name());

    EventLoop* io_loop = conn->Loop();
    io_loop->QueueInLoop([conn]
        {
            conn->ConnectDestroyed();
        });

    // 最后一条连接退场即通知关闭完成，不必靠轮询
    CheckAllClosedInLoop();
}

}  // namespace nebula::net
