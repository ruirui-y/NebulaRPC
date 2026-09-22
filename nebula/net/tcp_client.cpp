#include "nebula/net/tcp_client.h"

#include "nebula/net/connector.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"

#include <sstream>
#include <unistd.h>

namespace nebula::net {

TcpClient::TcpClient(EventLoop* loop, std::string ip, std::uint16_t port)
    : loop_(loop),
      connector_(std::make_shared<Connector>(loop, std::move(ip), port)) {
    connector_->SetNewConnectionCallback([this](int socket_fd) {
        NewConnection(socket_fd);
    });
    connector_->SetErrorCallback([this](const std::string& reason) {
        HandleConnectError(reason);
    });
}

TcpClient::~TcpClient() {
    loop_->AssertInLoopThread();

    connector_->SetNewConnectionCallback({});
    connector_->SetErrorCallback({});
    connector_->Stop();

    if (!connection_) {
        return;
    }

    auto conn = connection_;
    connection_.reset();

    // 析构时先切断上层回调，再移除底层 Channel。
    conn->SetConnectionCallback({});
    conn->SetMessageCallback({});
    conn->SetWriteCompleteCallback({});
    conn->SetCloseCallback({});
    conn->ConnectDestroyed();
}

void TcpClient::Connect() {
    connect_ = true;
    connector_->Start();
}

void TcpClient::Disconnect() {
    connect_ = false;
    auto conn = connection_;
    if (conn) {
        conn->Shutdown();
    }
}

void TcpClient::Stop() {
    connect_ = false;
    connector_->Stop();
}

void TcpClient::NewConnection(int socket_fd) {
    loop_->AssertInLoopThread();

    if (!connect_) {
        ::close(socket_fd);
        return;
    }

    std::ostringstream name;
    name << "client-conn-" << next_connection_id_++;

    auto connection = std::make_shared<TcpConnection>(loop_, socket_fd, name.str());
    connection_ = connection;

    connection->SetConnectionCallback(connection_callback_);
    connection->SetMessageCallback(message_callback_);
    connection->SetWriteCompleteCallback(write_complete_callback_);
    connection->SetCloseCallback([this](const TcpConnectionPtr& conn) {
        RemoveConnection(conn);
    });

    connection->ConnectEstablished();
}

void TcpClient::RemoveConnection(const TcpConnectionPtr& conn) {
    loop_->AssertInLoopThread();

    if (connection_ == conn) {
        connection_.reset();
    }

    conn->SetCloseCallback({});
    loop_->QueueInLoop([conn] { conn->ConnectDestroyed(); });
}

void TcpClient::HandleConnectError(const std::string& reason) {
    loop_->AssertInLoopThread();
    connect_ = false;
    if (connect_error_callback_) {
        connect_error_callback_(reason);
    }
}

}  // namespace nebula::net
