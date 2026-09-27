#include "nebula/net/connector.h"

#include "nebula/net/channel.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/socket.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <exception>
#include <memory>
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
    CancelReconnect();

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

void Connector::Restart()
{
    auto self = shared_from_this();
    loop_->RunInLoop([self]
        {
            self->RestartInLoop();
        });
}

void Connector::StartInLoop()
{
    loop_->AssertInLoopThread();
    if (connect_.load() && state_ == State::kDisconnected)
    {
        CancelReconnect();
        ConnectInLoop();
    }
}

void Connector::StopInLoop()
{
    loop_->AssertInLoopThread();

    // 等待重连期间 state_ 就是 kDisconnected，取消必须排在守卫之前
    CancelReconnect();

    if (state_ != State::kConnecting)
    {
        return;
    }

    const int socket_fd = RemoveAndResetChannel();
    socket_fd_ = -1;
    state_ = State::kDisconnected;
    ::close(socket_fd);
}

void Connector::RestartInLoop()
{
    loop_->AssertInLoopThread();

    // 只有外部报丧能把 kConnected 带回起点；从没连上过时退避那条路自己在管
    if (state_ != State::kConnected)
    {
        return;
    }

    state_ = State::kDisconnected;
    StartInLoop();
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
        next_backoff_ = {};                          // 连上了，退避清零
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
    next_backoff_ = {};                              // 连上了，退避清零
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

    ScheduleReconnect();
}

void Connector::ScheduleReconnect()
{
    loop_->AssertInLoopThread();

    if (!connect_.load() || state_ != State::kDisconnected)
    {
        return;
    }

    const std::chrono::milliseconds delay =
        next_backoff_.count() == 0 ? reconnect_policy_.initial : next_backoff_;
    const std::chrono::milliseconds next = delay * reconnect_policy_.multiplier;
    next_backoff_ = std::min(next, reconnect_policy_.max);
    reconnect_attempt_count_.fetch_add(1);

    // weak_ptr：退避期间外部可能已经放弃这个 Connector
    std::weak_ptr<Connector> weak_self = shared_from_this();
    retry_timer_ = loop_->RunAfter(delay, [weak_self]
        {
            if (auto self = weak_self.lock())
            {
                self->StartInLoop();
            }
        });
}

void Connector::CancelReconnect()
{
    if (!retry_timer_.Valid())
    {
        return;
    }

    loop_->CancelTimer(retry_timer_);
    retry_timer_ = {};
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
