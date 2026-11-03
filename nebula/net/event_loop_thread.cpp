#include "nebula/net/event_loop_thread.h"

#include "nebula/net/event_loop.h"

#include <chrono>
#include <memory>
#include <stdexcept>

namespace nebula::net
{

EventLoopThread::~EventLoopThread()
{
    EventLoop* loop = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop = loop_;
    }
    if (loop != nullptr)
    {
        loop->Quit();
    }
    if (thread_.joinable())
    {
        thread_.join();
    }
}

EventLoop* EventLoopThread::StartLoop(EventLoopInitCallback init_callback)
{
    if (started_)
    {
        throw std::logic_error("EventLoopThread already started");
    }
    started_ = true;
    try
    {
        thread_ = std::thread([this, callback = std::move(init_callback)]() mutable
            {
                ThreadFunc(std::move(callback));
            });
    }
    catch (...)
    {
        started_ = false;
        throw;
    }

    std::unique_lock<std::mutex> lock(mutex_);
    condition_.wait(lock, [this]
        {
            return startup_finished_;
        });

    if (startup_error_)
    {
        const std::exception_ptr error = startup_error_;
        lock.unlock();
        if (thread_.joinable())
        {
            thread_.join();
        }
        std::rethrow_exception(error);
    }

    return loop_;
}

void EventLoopThread::ThreadFunc(EventLoopInitCallback init_callback)
{
    std::unique_ptr<EventLoop> loop;
    try
    {
        loop = std::make_unique<EventLoop>();
        if (init_callback)
        {
            init_callback(loop.get());
        }

        EventLoop* loop_pointer = loop.get();
        loop->RunAfter(std::chrono::milliseconds(0), [this, loop_pointer]
            {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    loop_ = loop_pointer;
                    startup_finished_ = true;
                }
                condition_.notify_one();
            });
    }
    catch (...)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            startup_error_ = std::current_exception();
            startup_finished_ = true;
        }
        condition_.notify_one();
        return;
    }

    loop->Loop();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        loop_ = nullptr;
    }
}

}  // namespace nebula::net
