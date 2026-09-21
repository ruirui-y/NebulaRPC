#include "nebula/net/acceptor.h"

#include "nebula/net/event_loop.h"

#include <cerrno>
#include <unistd.h>

namespace nebula::net {

Acceptor::Acceptor(EventLoop* loop, std::string ip, std::uint16_t port, bool reuse_port)
    : loop_(loop),
      accept_socket_(Socket::CreateNonblocking()),
      accept_channel_(loop, accept_socket_.Fd()) {
    accept_socket_.SetReuseAddr(true);
    accept_socket_.SetReusePort(reuse_port);
    accept_socket_.BindAddress(ip, port);
    accept_channel_.SetReadCallback([this] { HandleRead(); });
}

void Acceptor::Listen() {
    loop_->AssertInLoopThread();
    listening_ = true;
    accept_socket_.Listen();
    accept_channel_.EnableReading();
}

void Acceptor::HandleRead() {
    loop_->AssertInLoopThread();

    while (true) {
        const int conn_fd = accept_socket_.Accept();
        if (conn_fd >= 0) {
            if (new_connection_callback_) {
                new_connection_callback_(conn_fd);
            } else {
                ::close(conn_fd);
            }
            continue;
        }

        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        break;
    }
}

}  // namespace nebula::net
