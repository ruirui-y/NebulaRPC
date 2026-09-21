#include "nebula/net/channel.h"

#include <utility>

namespace nebula::net
{

Channel::Channel(EventLoop* loop, int fd) noexcept
    : loop_(loop),
      fd_(fd)
{
}

void Channel::HandleEvent()
{
    // TODO(NRPC-S1-01):
    // 根据 epoll 返回的 revents 决定是否调用 read_callback_。
}

void Channel::SetReadCallback(EventCallback callback)
{
    read_callback_ = std::move(callback);
}

} // namespace nebula::net
