#include "nebula/net/epoll_poller.h"

#include "nebula/net/channel.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <unistd.h>

namespace nebula::net
{

EPollPoller::EPollPoller(EventLoop* loop)
    : Poller(loop),
      epoll_fd_(::epoll_create1(EPOLL_CLOEXEC)),
      events_(kInitialEventListSize)
{
    if (epoll_fd_ < 0)
    {
        throw std::runtime_error("epoll_create1 failed: " + std::string(std::strerror(errno)));
    }
}

EPollPoller::~EPollPoller()
{
    ::close(epoll_fd_);
}

void EPollPoller::Poll(int timeout_ms, ChannelList* active_channels)
{
    const int event_count = ::epoll_wait(
        epoll_fd_, events_.data(), static_cast<int>(events_.size()), timeout_ms);

    if (event_count > 0)
    {
        FillActiveChannels(event_count, active_channels);
        if (event_count == static_cast<int>(events_.size()))
        {
            events_.resize(events_.size() * 2U);
        }
        return;
    }

    if (event_count < 0 && errno != EINTR)
    {
    }
}

void EPollPoller::UpdateChannel(Channel* channel)
{
    const int index = channel->Index();
    const int fd = channel->Fd();

    if (index == kNew || index == kDeleted)
    {
        if (index == kNew)
        {
            channels_[fd] = channel;
        }
        channel->SetIndex(kAdded);
        Update(EPOLL_CTL_ADD, channel);
        return;
    }

    if (channel->IsNoneEvent())
    {
        Update(EPOLL_CTL_DEL, channel);
        channel->SetIndex(kDeleted);
    }
    else
    {
        Update(EPOLL_CTL_MOD, channel);
    }
}

void EPollPoller::RemoveChannel(Channel* channel)
{
    const int fd = channel->Fd();
    channels_.erase(fd);

    if (channel->Index() == kAdded)
    {
        Update(EPOLL_CTL_DEL, channel);
    }
    channel->SetIndex(kNew);
}

void EPollPoller::FillActiveChannels(int event_count, ChannelList* active_channels) const
{
    for (int i = 0; i < event_count; ++i)
    {
        auto* channel = static_cast<Channel*>(events_[static_cast<std::size_t>(i)].data.ptr);
        channel->SetRevents(events_[static_cast<std::size_t>(i)].events);
        active_channels->push_back(channel);
    }
}

void EPollPoller::Update(int operation, Channel* channel)
{
    epoll_event event{};
    event.events = channel->Events();
    event.data.ptr = channel;

    if (::epoll_ctl(epoll_fd_, operation, channel->Fd(), &event) < 0)
    {
        if (operation == EPOLL_CTL_DEL && errno == ENOENT)
        {
            return;
        }
        throw std::runtime_error("epoll_ctl failed: " + std::string(std::strerror(errno)));
    }
}

}  // namespace nebula::net
