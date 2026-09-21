#include "nebula/net/tcp_connection.h"

#include "nebula/net/channel.h"
#include "nebula/net/event_loop.h"

#include <cerrno>
#include <cstring>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

namespace nebula::net {

TcpConnection::TcpConnection(EventLoop* loop, int socket_fd, std::string name)
    : loop_(loop),
      name_(std::move(name)),
      socket_(socket_fd),
      channel_(std::make_unique<Channel>(loop, socket_fd)) {
    channel_->SetReadCallback([this] { HandleRead(); });
    channel_->SetWriteCallback([this] { HandleWrite(); });
    channel_->SetCloseCallback([this] { HandleClose(); });
    channel_->SetErrorCallback([this] { HandleError(); });
    socket_.SetTcpNoDelay(true);
}

TcpConnection::~TcpConnection() = default;

bool TcpConnection::Connected() const noexcept {
    return state_.load() == State::kConnected;
}

void TcpConnection::Send(std::string_view data) {
    if (!Connected()) {
        return;
    }

    std::string owned_data(data);
    if (loop_->IsInLoopThread()) {
        SendInLoop(std::move(owned_data));
        return;
    }

    auto self = shared_from_this();
    loop_->RunInLoop([self, data = std::move(owned_data)]() mutable {
        self->SendInLoop(std::move(data));
    });
}

void TcpConnection::Shutdown() {
    State expected = State::kConnected;
    if (state_.compare_exchange_strong(expected, State::kDisconnecting)) {
        auto self = shared_from_this();
        loop_->RunInLoop([self] { self->ShutdownInLoop(); });
    }
}

void TcpConnection::ConnectEstablished() {
    loop_->AssertInLoopThread();
    SetState(State::kConnected);
    channel_->Tie(shared_from_this());
    channel_->EnableReading();
    if (connection_callback_) {
        connection_callback_(shared_from_this());
    }
}

void TcpConnection::ConnectDestroyed() {
    loop_->AssertInLoopThread();
    if (state_.load() == State::kConnected) {
        SetState(State::kDisconnected);
        channel_->DisableAll();
        if (connection_callback_) {
            connection_callback_(shared_from_this());
        }
    }
    channel_->Remove();
}

void TcpConnection::HandleRead() {
    int saved_errno = 0;
    const ssize_t n = input_buffer_.ReadFd(socket_.Fd(), &saved_errno);
    if (n > 0) {
        if (message_callback_) {
            message_callback_(shared_from_this(), &input_buffer_);
        }
    } else if (n == 0) {
        HandleClose();
    } else if (saved_errno != EAGAIN && saved_errno != EWOULDBLOCK) {
        errno = saved_errno;
        HandleError();
    }
}

void TcpConnection::HandleWrite() {
    if (!channel_->IsWriting()) {
        return;
    }

    const ssize_t n = ::write(socket_.Fd(), output_buffer_.Peek(), output_buffer_.ReadableBytes());
    if (n > 0) {
        output_buffer_.Retrieve(static_cast<std::size_t>(n));
        if (output_buffer_.ReadableBytes() == 0U) {
            channel_->DisableWriting();
            if (write_complete_callback_) {
                auto self = shared_from_this();
                loop_->QueueInLoop([self, cb = write_complete_callback_] { cb(self); });
            }
            if (state_.load() == State::kDisconnecting) {
                ShutdownInLoop();
            }
        }
    } else if (n < 0 && errno != EWOULDBLOCK && errno != EAGAIN) {
        HandleError();
    }
}

void TcpConnection::HandleClose() {
    loop_->AssertInLoopThread();
    const State current = state_.load();
    if (current == State::kDisconnected) {
        return;
    }

    SetState(State::kDisconnected);
    channel_->DisableAll();
    auto guard = shared_from_this();
    if (connection_callback_) {
        connection_callback_(guard);
    }
    if (close_callback_) {
        close_callback_(guard);
    }
}

void TcpConnection::HandleError() {
    int error = 0;
    socklen_t length = sizeof(error);
    if (::getsockopt(socket_.Fd(), SOL_SOCKET, SO_ERROR, &error, &length) < 0) {
        error = errno;
    }
    std::cerr << "TcpConnection(" << name_ << ") socket error: "
              << std::strerror(error) << '\n';
}

void TcpConnection::SendInLoop(std::string data) {
    loop_->AssertInLoopThread();
    if (state_.load() == State::kDisconnected) {
        return;
    }

    std::size_t remaining = data.size();
    ssize_t written = 0;

    if (!channel_->IsWriting() && output_buffer_.ReadableBytes() == 0U) {
        written = ::write(socket_.Fd(), data.data(), data.size());
        if (written >= 0) {
            remaining -= static_cast<std::size_t>(written);
            if (remaining == 0U && write_complete_callback_) {
                auto self = shared_from_this();
                loop_->QueueInLoop([self, cb = write_complete_callback_] { cb(self); });
            }
        } else {
            written = 0;
            if (errno != EWOULDBLOCK && errno != EAGAIN) {
                HandleError();
                return;
            }
        }
    }

    if (remaining > 0U) {
        output_buffer_.Append(data.data() + written, remaining);
        if (!channel_->IsWriting()) {
            channel_->EnableWriting();
        }
    }
}

void TcpConnection::ShutdownInLoop() {
    loop_->AssertInLoopThread();
    if (!channel_->IsWriting()) {
        socket_.ShutdownWrite();
    }
}

}  // namespace nebula::net
