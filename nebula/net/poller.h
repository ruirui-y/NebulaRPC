#pragma once

#include "nebula/base/noncopyable.h"

#include <memory>
#include <unordered_map>
#include <vector>

namespace nebula::net
{

class Channel;
class EventLoop;

class Poller : private base::Noncopyable
{
public:
    using ChannelList = std::vector<Channel*>;

    explicit Poller(EventLoop* owner_loop) : owner_loop_(owner_loop)
    {
    }
    virtual ~Poller() = default;

    virtual void Poll(int timeout_ms, ChannelList* active_channels) = 0;
    virtual void UpdateChannel(Channel* channel) = 0;
    virtual void RemoveChannel(Channel* channel) = 0;

    [[nodiscard]] bool HasChannel(Channel* channel) const;
    static std::unique_ptr<Poller> NewDefaultPoller(EventLoop* loop);

protected:
    using ChannelMap = std::unordered_map<int, Channel*>;
    ChannelMap channels_;
    EventLoop* owner_loop_;
};

}  // namespace nebula::net
