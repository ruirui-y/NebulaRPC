#include "nebula/net/poller.h"

#include "nebula/net/channel.h"
#include "nebula/net/epoll_poller.h"

#include <memory>

namespace nebula::net {

bool Poller::HasChannel(Channel* channel) const {
    const auto it = channels_.find(channel->Fd());
    return it != channels_.end() && it->second == channel;
}

std::unique_ptr<Poller> Poller::NewDefaultPoller(EventLoop* loop) {
    return std::make_unique<EPollPoller>(loop);
}

}  // namespace nebula::net
