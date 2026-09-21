#include "nebula/net/tcp_server.h"

#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"

#include <sstream>

namespace nebula::net {

TcpServer::TcpServer(EventLoop* loop, std::string ip, std::uint16_t port, bool reuse_port)
    : loop_(loop),
      acceptor_(std::make_unique<Acceptor>(loop, std::move(ip), port, reuse_port)),
      thread_pool_(std::make_unique<EventLoopThreadPool>(loop)) {
    acceptor_->SetNewConnectionCallback([this](int socket_fd) { NewConnection(socket_fd); });
}

TcpServer::~TcpServer() {
    for (auto& [name, connection] : connections_) {
        (void)name;
        auto conn = connection;
        conn->Loop()->RunInLoop([conn] { conn->ConnectDestroyed(); });
    }
}

void TcpServer::Start(std::size_t io_thread_count) {
    if (started_) {
        return;
    }
    started_ = true;

    thread_pool_->Start(io_thread_count);
    loop_->RunInLoop([this] {
        if (!acceptor_->Listening()) {
            acceptor_->Listen();
        }
    });
}

void TcpServer::NewConnection(int socket_fd) {
    loop_->AssertInLoopThread();
    EventLoop* io_loop = thread_pool_->GetNextLoop();

    std::ostringstream name;
    name << "conn-" << next_connection_id_++;
    auto connection = std::make_shared<TcpConnection>(io_loop, socket_fd, name.str());
    connections_.emplace(connection->Name(), connection);

    connection->SetConnectionCallback(connection_callback_);
    connection->SetMessageCallback(message_callback_);
    connection->SetWriteCompleteCallback(write_complete_callback_);
    connection->SetCloseCallback([this](const TcpConnectionPtr& conn) { RemoveConnection(conn); });

    io_loop->RunInLoop([connection] { connection->ConnectEstablished(); });
}

void TcpServer::RemoveConnection(const TcpConnectionPtr& conn) {
    loop_->RunInLoop([this, conn] { RemoveConnectionInLoop(conn); });
}

void TcpServer::RemoveConnectionInLoop(const TcpConnectionPtr& conn) {
    loop_->AssertInLoopThread();
    connections_.erase(conn->Name());
    EventLoop* io_loop = conn->Loop();
    io_loop->QueueInLoop([conn] { conn->ConnectDestroyed(); });
}

}  // namespace nebula::net
