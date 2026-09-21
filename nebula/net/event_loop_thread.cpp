#include "nebula/net/event_loop_thread.h"

#include "nebula/net/event_loop.h"

namespace nebula::net {

EventLoopThread::~EventLoopThread() {
    exiting_ = true;
    EventLoop* loop = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop = loop_;
    }
    if (loop != nullptr) {
        loop->Quit();
    }
    if (thread_.joinable()) {
        thread_.join();
    }
}

EventLoop* EventLoopThread::StartLoop() {
    thread_ = std::thread([this] { ThreadFunc(); });

    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [this] { return loop_ != nullptr; });
    return loop_;
}

void EventLoopThread::ThreadFunc() {
    EventLoop loop;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = &loop;
        condition_.notify_one();
    }

    loop.Loop();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = nullptr;
    }
}

}  // namespace nebula::net
