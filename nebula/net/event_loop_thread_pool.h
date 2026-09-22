#pragma once

#include "nebula/base/noncopyable.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace nebula::net
{

class EventLoop;
class EventLoopThread;

class EventLoopThreadPool final : private base::Noncopyable
{
public:
    explicit EventLoopThreadPool(EventLoop* base_loop);
    ~EventLoopThreadPool();

    void Start(std::size_t thread_count);
    [[nodiscard]] EventLoop* GetNextLoop();
    [[nodiscard]] std::vector<EventLoop*> GetAllLoops() const;

private:
    EventLoop* base_loop_;
    bool started_{false};
    std::size_t next_{0};
    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;
};

}  // namespace nebula::net
