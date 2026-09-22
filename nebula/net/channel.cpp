#include "nebula/net/channel.h"

#include "nebula/net/event_loop.h"

#include <sys/epoll.h>

namespace nebula::net
{

const std::uint32_t Channel::kReadEvent = EPOLLIN | EPOLLPRI | EPOLLRDHUP;
const std::uint32_t Channel::kWriteEvent = EPOLLOUT;

Channel::Channel(EventLoop* loop, int fd) : loop_(loop), fd_(fd)
{
}

void Channel::Tie(const std::shared_ptr<void>& owner)
{
    tie_ = owner;
    tied_ = true;
}

void Channel::HandleEvent()
{
    if (!tied_)
    {
        HandleEventWithGuard();
        return;
    }

    if (auto guard = tie_.lock())
    {
        HandleEventWithGuard();
    }
}

void Channel::HandleEventWithGuard()
{
    if ((revents_ & EPOLLHUP) != 0U && (revents_ & EPOLLIN) == 0U)
    {
        if (close_callback_)
        {
            close_callback_();
        }
    }

    if ((revents_ & EPOLLERR) != 0U)
    {
        if (error_callback_)
        {
            error_callback_();
        }
    }

    if ((revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) != 0U)
    {
        if (read_callback_)
        {
            read_callback_();
        }
    }

    if ((revents_ & EPOLLOUT) != 0U)
    {
        if (write_callback_)
        {
            write_callback_();
        }
    }
}

void Channel::EnableReading()
{
    events_ |= kReadEvent;
    Update();
}

void Channel::DisableReading()
{
    events_ &= ~kReadEvent;
    Update();
}

void Channel::EnableWriting()
{
    events_ |= kWriteEvent;
    Update();
}

void Channel::DisableWriting()
{
    events_ &= ~kWriteEvent;
    Update();
}

void Channel::DisableAll()
{
    events_ = kNoneEvent;
    Update();
}

void Channel::Remove()
{
    loop_->RemoveChannel(this);
}

void Channel::Update()
{
    loop_->UpdateChannel(this);
}

}  // namespace nebula::net
