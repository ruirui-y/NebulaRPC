#pragma once

#include "nebula/net/poller.h"

#include <sys/epoll.h>

#include <vector>

namespace nebula::net
{

class EPollPoller final : public Poller
{
public:
    explicit EPollPoller(EventLoop* loop);
    ~EPollPoller() override;

    void Poll(int timeout_ms, ChannelList* active_channels) override;
    void UpdateChannel(Channel* channel) override;
    void RemoveChannel(Channel* channel) override;

private:
    static constexpr int kNew = -1;
    static constexpr int kAdded = 1;
    static constexpr int kDeleted = 2;
    static constexpr int kInitialEventListSize = 16;

    void FillActiveChannels(int event_count, ChannelList* active_channels) const;
    void Update(int operation, Channel* channel);

    int epoll_fd_;
    std::vector<epoll_event> events_;
};

}  // namespace nebula::net
