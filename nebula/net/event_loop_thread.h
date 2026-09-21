#pragma once

#include "nebula/base/noncopyable.h"

#include <condition_variable>
#include <mutex>
#include <thread>

namespace nebula::net {

class EventLoop;

class EventLoopThread final : private base::Noncopyable {
public:
    EventLoopThread() = default;
    ~EventLoopThread();

    EventLoop* StartLoop();

private:
    void ThreadFunc();

    EventLoop* loop_{nullptr};
    bool exiting_{false};
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable condition_;
};

}  // namespace nebula::net
