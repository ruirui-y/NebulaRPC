#pragma once

#include <memory>

namespace nebula::net
{

class Channel;
class EventLoop;

class TcpConnection
{
public:
    TcpConnection(EventLoop* loop, int fd);
    ~TcpConnection();

    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;

    // Stage 1 / NRPC-S1-01：后续把 Channel read callback 接到这里。
    void HandleRead();

private:
    EventLoop* loop_{nullptr};
    int fd_{-1};
    std::unique_ptr<Channel> channel_;
};

} // namespace nebula::net
