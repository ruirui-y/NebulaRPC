#include "nebula/net/tcp_connection.h"

#include "nebula/net/channel.h"

namespace nebula::net
{

TcpConnection::TcpConnection(EventLoop* loop, int fd)
    : loop_(loop),
      fd_(fd),
      channel_(std::make_unique<Channel>(loop, fd))
{
}

TcpConnection::~TcpConnection() = default;

void TcpConnection::HandleRead()
{
    // TODO(NRPC-S1-01):
    // nonblocking read -> input state -> message callback
}

} // namespace nebula::net
