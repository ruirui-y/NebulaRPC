#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/callbacks.h"

#include <condition_variable>
#include <exception>
#include <mutex>
#include <thread>

namespace nebula::net
{

class EventLoop;

class EventLoopThread final : private base::Noncopyable
{
public:
    EventLoopThread() = default;
    ~EventLoopThread();

    EventLoop* StartLoop(EventLoopInitCallback init_callback = {});  // 初始化完成并开始运行后，才把 Loop 返回给调用者

private:
    void ThreadFunc(EventLoopInitCallback init_callback);

    EventLoop* loop_{nullptr};
    bool started_{false};
    bool startup_finished_{false};
    std::exception_ptr startup_error_;
    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable condition_;
};

}  // namespace nebula::net
