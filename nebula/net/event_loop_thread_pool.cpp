#include "nebula/net/event_loop_thread_pool.h"

#include "nebula/net/event_loop.h"
#include "nebula/net/event_loop_thread.h"

#include <stdexcept>

namespace nebula::net {

EventLoopThreadPool::EventLoopThreadPool(EventLoop* base_loop) : base_loop_(base_loop) {}

EventLoopThreadPool::~EventLoopThreadPool() = default;

void EventLoopThreadPool::Start(std::size_t thread_count) {
    base_loop_->AssertInLoopThread();
    if (started_) {
        throw std::logic_error("EventLoopThreadPool already started");
    }
    started_ = true;

    for (std::size_t i = 0; i < thread_count; ++i) {
        auto thread = std::make_unique<EventLoopThread>();
        EventLoop* loop = thread->StartLoop();
        loops_.push_back(loop);
        threads_.push_back(std::move(thread));
    }
}

EventLoop* EventLoopThreadPool::GetNextLoop() {
    base_loop_->AssertInLoopThread();
    if (loops_.empty()) {
        return base_loop_;
    }

    EventLoop* loop = loops_[next_];
    next_ = (next_ + 1U) % loops_.size();
    return loop;
}

std::vector<EventLoop*> EventLoopThreadPool::GetAllLoops() const {
    if (loops_.empty()) {
        return {base_loop_};
    }
    return loops_;
}

}  // namespace nebula::net
