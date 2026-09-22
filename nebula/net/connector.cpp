#include "nebula/net/connector.h"

#include "nebula/net/channel.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/socket.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <exception>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace nebula::net
{

Connector::Connector(EventLoop* loop, std::string ip, std::uint16_t port)
    : loop_(loop), ip_(std::move(ip)), port_(port)
{
}

Connector::~Connector()
{
    loop_->AssertInLoopThread();

    if (channel_)
    {
        channel_->DisableAll();
        channel_->Remove();
        channel_.reset();
    }

    if (socket_fd_ >= 0)
    {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }
}

void Connector::Start()
{
    connect_ = true;
    auto self = shared_from_this();
    loop_->RunInLoop([self]
        {
            self->StartInLoop();
        });
}

void Connector::Stop()
{
    connect_ = false;
    auto self = shared_from_this();
    loop_->RunInLoop([self]
        {
            self->StopInLoop();
        });
}

void Connector::StartInLoop()
{
    loop_->AssertInLoopThread();
    if (connect_.load() && state_ == State::kDisconnected)
    {
        ConnectInLoop();
    }
}

void Connector::StopInLoop()
{
    loop_->AssertInLoopThread();
    if (state_ != State::kConnecting)
    {
        return;
    }

    const int socket_fd = RemoveAndResetChannel();
    socket_fd_ = -1;
    state_ = State::kDisconnected;
    ::close(socket_fd);
}

void Connector::ConnectInLoop()
{
    loop_->AssertInLoopThread();

    int socket_fd = -1;
    try
    {
        socket_fd = Socket::CreateNonblocking();
    }
    catch (const std::exception& ex)
    {
        ReportError(ex.what());
        return;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);

    if (::inet_pton(AF_INET, ip_.c_str(), &address.sin_addr) != 1)
    {
        ::close(socket_fd);
        ReportError("invalid IPv4 address: " + ip_);
        return;
    }

    const int result = ::connect(socket_fd,
                                 reinterpret_cast<sockaddr*>(&address),
                                 sizeof(address));
    const int saved_errno = result == 0 ? 0 : errno;

    if (result == 0 || saved_errno == EISCONN)
    {
        state_ = State::kConnected;
        if (!connect_.load())
        {
            ::close(socket_fd);
            state_ = State::kDisconnected;
            return;
        }

        if (new_connection_callback_)
        {
            new_connection_callback_(socket_fd);
        }
        else
        {
            ::close(socket_fd);
            state_ = State::kDisconnected;
        }
        return;
    }

    if (saved_errno == EINPROGRESS || saved_errno == EINTR)
    {
        Connecting(socket_fd);
        return;
    }

    const std::string reason = std::strerror(saved_errno);
    ::close(socket_fd);
    ReportError("connect failed: " + reason);
}

void Connector::Connecting(int socket_fd)
{
    loop_->AssertInLoopThread();

    state_ = State::kConnecting;
    socket_fd_ = socket_fd;
    channel_ = std::make_unique<Channel>(loop_, socket_fd_);

    // weak_ptr 避免 Connector -> Channel callback -> Connector 引用环。
    std::weak_ptr<Connector> weak_self = shared_from_this();

    channel_->SetWriteCallback([weak_self]
        {
            if (auto self = weak_self.lock())
            {
                self->HandleWrite();
            }
        });

    channel_->SetErrorCallback([weak_self]
        {
            if (auto self = weak_self.lock())
            {
                self->HandleError();
            }
        });

    channel_->EnableWriting();
}

void Connector::HandleWrite()
{
    loop_->AssertInLoopThread();
    if (state_ != State::kConnecting)
    {
        return;
    }

    // EPOLLOUT 只表示 connect 已结束，最终结果看 SO_ERROR。
    const int socket_error = GetSocketError(socket_fd_);
    const int socket_fd = RemoveAndResetChannel();
    socket_fd_ = -1;

    if (socket_error != 0)
    {
        state_ = State::kDisconnected;
        ::close(socket_fd);
        ReportError("connect failed: " + std::string(std::strerror(socket_error)));
        return;
    }

    if (!connect_.load())
    {
        state_ = State::kDisconnected;
        ::close(socket_fd);
        return;
    }

    state_ = State::kConnected;
    if (new_connection_callback_)
    {
        new_connection_callback_(socket_fd);
    }
    else
    {
        ::close(socket_fd);
        state_ = State::kDisconnected;
    }
}

void Connector::HandleError()
{
    loop_->AssertInLoopThread();
    if (state_ != State::kConnecting)
    {
        return;
    }

    const int socket_error = GetSocketError(socket_fd_);
    const int socket_fd = RemoveAndResetChannel();
    socket_fd_ = -1;
    state_ = State::kDisconnected;
    ::close(socket_fd);

    const int error = socket_error != 0 ? socket_error : ECONNREFUSED;
    ReportError("connect failed: " + std::string(std::strerror(error)));
}

int Connector::RemoveAndResetChannel()
{
    loop_->AssertInLoopThread();

    const int socket_fd = channel_->Fd();
    channel_->DisableAll();
    channel_->Remove();

    // 当前可能仍在 Channel::HandleEvent 调用栈中，延迟释放。
    auto self = shared_from_this();
    loop_->QueueInLoop([self]
        {
            self->ResetChannel();
        });
    return socket_fd;
}

void Connector::ResetChannel()
{
    loop_->AssertInLoopThread();
    channel_.reset();
}

void Connector::ReportError(const std::string& reason)
{
    state_ = State::kDisconnected;
    if (error_callback_)
    {
        error_callback_(reason);
    }
}

int Connector::GetSocketError(int socket_fd)
{
    int error = 0;
    socklen_t length = sizeof(error);
    if (::getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0)
    {
        return errno;
    }
    return error;
}

}  // namespace nebula::net
