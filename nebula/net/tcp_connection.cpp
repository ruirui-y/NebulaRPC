#include "nebula/net/tcp_connection.h"

#include "nebula/net/channel.h"
#include "nebula/net/event_loop.h"

#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

namespace nebula::net
{
namespace
{

// 对端断电/断网时既无 FIN 也无 RST，只能靠探测耗尽把错误暴露给 read/write
constexpr int kKeepAliveIdleSeconds = 10;
constexpr int kKeepAliveIntervalSeconds = 3;
constexpr int kKeepAliveProbeCount = 3;

}  // namespace

TcpConnection::TcpConnection(EventLoop* loop, int socket_fd, std::string name)
    : loop_(loop),
      name_(std::move(name)),
      socket_(socket_fd),
      channel_(std::make_unique<Channel>(loop, socket_fd))
{
    channel_->SetReadCallback([this]
        {
            HandleRead();
        });
    channel_->SetWriteCallback([this]
        {
            HandleWrite();
        });
    channel_->SetCloseCallback([this]
        {
            HandleClose();
        });
    channel_->SetErrorCallback([this]
        {
            HandleError();
        });
    socket_.SetTcpNoDelay(true);

    // 最坏 10 + 3 * 3 = 19s 发现对端不可达，随后 HandleError 关连接、TcpClient 重启连接
    socket_.SetKeepAlive(true,
                         kKeepAliveIdleSeconds,
                         kKeepAliveIntervalSeconds,
                         kKeepAliveProbeCount);
}

TcpConnection::~TcpConnection() = default;

bool TcpConnection::Connected() const noexcept
{
    return state_.load() == State::kConnected;
}

void TcpConnection::SetHighWatermarkCallback(HighWatermarkCallback cb,
                                             std::size_t high_watermark)
{
    high_watermark_callback_ = std::move(cb);
    high_watermark_ = high_watermark;
}

void TcpConnection::SetMaxOutputBufferBytes(std::size_t limit) noexcept
{
    max_output_buffer_bytes_ = limit;
}

void TcpConnection::SetMaxInputBufferBytes(std::size_t limit) noexcept
{
    max_input_buffer_bytes_ = limit;
}

std::size_t TcpConnection::PendingOutputBytes() const noexcept
{
    return output_buffer_.ReadableBytes();
}

std::uint64_t TcpConnection::HighWatermarkCount() const noexcept
{
    return high_watermark_count_.load();
}

std::uint64_t TcpConnection::OverloadCloseCount() const noexcept
{
    return overload_close_count_.load();
}

void TcpConnection::ForceClose()
{
    auto self = shared_from_this();
    loop_->RunInLoop([self]
        {
            self->ForceCloseInLoop();
        });
}

void TcpConnection::Write(std::string data)
{
    if (!Connected() || data.empty())
    {
        return;
    }

    if (loop_->IsInLoopThread())
    {
        WriteInLoop(std::move(data));
        return;
    }

    auto self = shared_from_this();
    loop_->QueueInLoop([self, data = std::move(data)]() mutable
        {
            self->WriteInLoop(std::move(data));
        });
}

void TcpConnection::Flush()
{
    if (!Connected())
    {
        return;
    }

    if (loop_->IsInLoopThread())
    {
        FlushInLoop();
        return;
    }

    auto self = shared_from_this();
    loop_->QueueInLoop([self]
        {
            self->FlushInLoop();
        });
}

void TcpConnection::Shutdown()
{
    State expected = State::kConnected;
    if (state_.compare_exchange_strong(expected, State::kDisconnecting))
    {
        auto self = shared_from_this();
        loop_->RunInLoop([self]
            {
                self->ShutdownInLoop();
            });
    }
}

void TcpConnection::ConnectEstablished()
{
    loop_->AssertInLoopThread();
    SetState(State::kConnected);
    channel_->Tie(shared_from_this());
    channel_->EnableReading();
    if (connection_callback_)
    {
        connection_callback_(shared_from_this());
    }
}

void TcpConnection::ConnectDestroyed()
{
    loop_->AssertInLoopThread();
    if (state_.load() == State::kConnected)
    {
        SetState(State::kDisconnected);
        channel_->DisableAll();
        if (connection_callback_)
        {
            connection_callback_(shared_from_this());
        }
    }
    channel_->Remove();
}

void TcpConnection::HandleRead()
{
    int saved_errno = 0;
    const ssize_t n = input_buffer_.ReadFd(socket_.Fd(), &saved_errno);
    if (n > 0)
    {
        if (message_callback_)
        {
            message_callback_(shared_from_this(), &input_buffer_);
        }

        // 检查放在回调之后：合法帧会被消费掉，残留越线说明对端在灌无效流量
        if (max_input_buffer_bytes_ > 0U &&
            input_buffer_.ReadableBytes() > max_input_buffer_bytes_)
        {
            overload_close_count_.fetch_add(1);
            ForceCloseInLoop();
        }
    }
    else if (n == 0)
    {
        HandleClose();
    }
    else if (saved_errno != EAGAIN && saved_errno != EWOULDBLOCK)
    {
        errno = saved_errno;
        HandleError();
    }
}

void TcpConnection::HandleWrite()
{
    if (!channel_->IsWriting())
    {
        return;
    }

    WriteOutputBufferInLoop();
}

void TcpConnection::HandleClose()
{
    loop_->AssertInLoopThread();
    const State current = state_.load();
    if (current == State::kDisconnected)
    {
        return;
    }

    SetState(State::kDisconnected);
    channel_->DisableAll();
    auto guard = shared_from_this();
    if (connection_callback_)
    {
        connection_callback_(guard);
    }
    if (close_callback_)
    {
        close_callback_(guard);
    }
}

void TcpConnection::HandleError()
{
    // 必须读走 SO_ERROR：水平触发下不消费待处理错误，epoll 会反复报同一条连接
    [[maybe_unused]] int error = 0;
    socklen_t length = sizeof(error);

    if (::getsockopt(socket_.Fd(), SOL_SOCKET, SO_ERROR, &error, &length) < 0)
    {
        error = errno;
    }

    ForceCloseInLoop();
}

void TcpConnection::WriteInLoop(std::string data)
{
    loop_->AssertInLoopThread();
    if (state_.load() == State::kDisconnected || data.empty())
    {
        return;
    }

    const std::size_t before = output_buffer_.ReadableBytes();
    const std::size_t pending = before + data.size();

    // 硬上限在真正入队前检查；Write 本身绝不触发 send。
    if (max_output_buffer_bytes_ > 0U && pending > max_output_buffer_bytes_)
    {
        overload_close_count_.fetch_add(1);
        ForceCloseInLoop();
        return;
    }

    NotifyHighWatermark(before, pending);
    output_buffer_.Append(data.data(), data.size());
}

void TcpConnection::FlushInLoop()
{
    loop_->AssertInLoopThread();
    if (state_.load() == State::kDisconnected || output_buffer_.ReadableBytes() == 0U)
    {
        return;
    }

    // 已经因为 partial write / EAGAIN 监听 EPOLLOUT 时，不主动重复 send。
    if (channel_->IsWriting())
    {
        return;
    }

    WriteOutputBufferInLoop();
}

void TcpConnection::WriteOutputBufferInLoop()
{
    loop_->AssertInLoopThread();
    if (output_buffer_.ReadableBytes() == 0U)
    {
        if (channel_->IsWriting())
        {
            channel_->DisableWriting();
        }
        return;
    }

    // Flush 与 EPOLLOUT 续写共用同一条真正的 syscall 路径。
    const ssize_t n = ::send(socket_.Fd(),
                             output_buffer_.Peek(),
                             output_buffer_.ReadableBytes(),
                             MSG_NOSIGNAL);

    if (n > 0)
    {
        output_buffer_.Retrieve(static_cast<std::size_t>(n));

        if (output_buffer_.ReadableBytes() == 0U)
        {
            if (channel_->IsWriting())
            {
                channel_->DisableWriting();
            }

            if (write_complete_callback_)
            {
                auto self = shared_from_this();
                loop_->QueueInLoop([self, cb = write_complete_callback_]
                    {
                        cb(self);
                    });
            }

            if (state_.load() == State::kDisconnecting)
            {
                ShutdownInLoop();
            }
            return;
        }

        if (!channel_->IsWriting())
        {
            channel_->EnableWriting();
        }
        return;
    }

    if (n < 0 && errno != EWOULDBLOCK && errno != EAGAIN)
    {
        HandleError();
        return;
    }

    // EAGAIN：留在 output_buffer_，之后只靠 EPOLLOUT 继续排空。
    if (!channel_->IsWriting())
    {
        channel_->EnableWriting();
    }
}

void TcpConnection::ShutdownInLoop()
{
    loop_->AssertInLoopThread();

    // 显式 Flush 尚未发生时也不能让优雅关闭丢掉已入队数据。
    // 有待发数据就交给 Flush / EPOLLOUT；真正清空后由写路径再次进入这里发送 FIN。
    if (output_buffer_.ReadableBytes() > 0U)
    {
        if (!channel_->IsWriting())
        {
            FlushInLoop();
        }
        return;
    }

    if (!channel_->IsWriting())
    {
        socket_.ShutdownWrite();
    }
}

void TcpConnection::ForceCloseInLoop()
{
    loop_->AssertInLoopThread();
    if (state_.load() == State::kDisconnected)
    {
        return;
    }

    // 丢弃待发数据：对端已经不读了，留着只是占内存
    output_buffer_.RetrieveAll();
    channel_->DisableWriting();
    HandleClose();
}

void TcpConnection::NotifyHighWatermark(std::size_t before, std::size_t after)
{
    if (high_watermark_ == 0U || high_watermark_callback_ == nullptr)
    {
        return;
    }

    // before < 水位 <= after：只在越线那一刻通知，天然防抖，不需要额外迟滞状态
    if (before >= high_watermark_ || after < high_watermark_)
    {
        return;
    }

    high_watermark_count_.fetch_add(1);

    // 走队列：回调里业务可能 Shutdown，不能在写路径中间改连接状态
    auto self = shared_from_this();
    loop_->QueueInLoop([self, cb = high_watermark_callback_, after]
        {
            cb(self, after);
        });
}

}  // namespace nebula::net
