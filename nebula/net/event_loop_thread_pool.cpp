#include "nebula/net/event_loop_thread_pool.h"

#include "nebula/net/event_loop.h"
#include "nebula/net/event_loop_thread.h"

#include <stdexcept>
#include <utility>

namespace nebula::net
{

EventLoopThreadPool::EventLoopThreadPool(EventLoop* base_loop) : base_loop_(base_loop)
{
}

EventLoopThreadPool::~EventLoopThreadPool() = default;

void EventLoopThreadPool::Start(std::size_t thread_count, EventLoopInitCallback init_callback)
{
    base_loop_->AssertInLoopThread();
    if (started_)
    {
        throw std::logic_error("EventLoopThreadPool already started");
    }

    if (thread_count == 0)
    {
        if (init_callback)
        {
            init_callback(base_loop_);
        }
        started_ = true;
        return;
    }

    std::vector<std::unique_ptr<EventLoopThread>> new_threads;
    std::vector<EventLoop*> new_loops;
    new_threads.reserve(thread_count);
    new_loops.reserve(thread_count);

    try
    {
        for (std::size_t i = 0; i < thread_count; ++i)
        {
            auto thread = std::make_unique<EventLoopThread>();
            EventLoop* loop = thread->StartLoop(init_callback);
            new_loops.push_back(loop);
            new_threads.push_back(std::move(thread));
        }
    }
    catch (...)
    {
        new_threads.clear();
        throw;
    }

    threads_ = std::move(new_threads);
    loops_ = std::move(new_loops);
    started_ = true;
}

EventLoop* EventLoopThreadPool::GetNextLoop()
{
    base_loop_->AssertInLoopThread();
    if (loops_.empty())
    {
        return base_loop_;
    }

    EventLoop* loop = loops_[next_];
    next_ = (next_ + 1U) % loops_.size();
    return loop;
}

std::vector<EventLoop*> EventLoopThreadPool::GetAllLoops() const
{
    if (loops_.empty())
    {
        return {base_loop_};
    }
    return loops_;
}

}  // namespace nebula::net
