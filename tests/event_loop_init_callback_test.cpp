#include "nebula/net/event_loop.h"
#include "nebula/net/event_loop_thread_pool.h"

#include <atomic>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace
{

// 记录这个 Loop 的资源是不是在它自己的线程里释放。
struct LoopContext
{
    LoopContext(nebula::net::EventLoop* owner_loop,
                std::atomic_size_t& owner_destroy_count,
                std::atomic_size_t& other_destroy_count)
        : owner(owner_loop),
          destroyed_on_owner(owner_destroy_count),
          destroyed_on_other(other_destroy_count)
    {
    }

    ~LoopContext()
    {
        if (owner->IsInLoopThread())
        {
            destroyed_on_owner.fetch_add(1);
        }
        else
        {
            destroyed_on_other.fetch_add(1);
        }
    }

    nebula::net::EventLoop* owner;
    std::atomic_size_t& destroyed_on_owner;
    std::atomic_size_t& destroyed_on_other;
};

}  // namespace

int main()
{
    std::atomic_size_t initialized{0};
    std::atomic_size_t destroyed_on_owner{0};
    std::atomic_size_t destroyed_on_other{0};

    {
        nebula::net::EventLoop base_loop;
        nebula::net::EventLoopThreadPool pool(&base_loop);
        pool.Start(4, [&](nebula::net::EventLoop* loop)
            {
                if (!loop->IsInLoopThread())
                {
                    throw std::runtime_error("initializer did not run in the loop thread");
                }

                loop->SetContext(std::make_unique<LoopContext>(
                    loop, destroyed_on_owner, destroyed_on_other));
                if (loop->GetContext<LoopContext>() == nullptr)
                {
                    throw std::runtime_error("loop context was not stored");
                }
                initialized.fetch_add(1);
            });

        if (initialized.load() != 4 || pool.GetAllLoops().size() != 4)
        {
            std::cerr << "not every IO loop was initialized before Start returned\n";
            return 1;
        }
    }

    if (destroyed_on_owner.load() != 4 || destroyed_on_other.load() != 0)
    {
        std::cerr << "IO loop resources were not destroyed by their owner threads\n";
        return 1;
    }

    {
        nebula::net::EventLoop base_loop;
        nebula::net::EventLoopThreadPool pool(&base_loop);
        std::atomic_size_t callback_count{0};
        bool startup_failed = false;

        try
        {
            pool.Start(2, [&](nebula::net::EventLoop* loop)
                {
                    if (callback_count.fetch_add(1) == 1)
                    {
                        throw std::runtime_error("expected initialization failure");
                    }
                    loop->SetContext(std::make_unique<LoopContext>(
                        loop, destroyed_on_owner, destroyed_on_other));
                });
        }
        catch (const std::runtime_error&)
        {
            startup_failed = true;
        }

        if (!startup_failed || destroyed_on_owner.load() != 5 || destroyed_on_other.load() != 0)
        {
            std::cerr << "failed IO loop startup did not clean up previously started loops\n";
            return 1;
        }

        pool.Start(1, [&](nebula::net::EventLoop* loop)
            {
                loop->SetContext(std::make_unique<LoopContext>(
                    loop, destroyed_on_owner, destroyed_on_other));
            });
    }

    if (destroyed_on_owner.load() != 6 || destroyed_on_other.load() != 0)
    {
        std::cerr << "IO loop pool could not restart after an initialization failure\n";
        return 1;
    }

    {
        nebula::net::EventLoop base_loop;
        nebula::net::EventLoopThreadPool pool(&base_loop);
        pool.Start(0, [&](nebula::net::EventLoop* loop)
            {
                if (loop != &base_loop || !loop->IsInLoopThread())
                {
                    throw std::runtime_error("base loop initializer ran on the wrong thread");
                }
                loop->SetContext(std::make_unique<LoopContext>(
                    loop, destroyed_on_owner, destroyed_on_other));
                initialized.fetch_add(1);
            });

        if (pool.GetNextLoop() != &base_loop || initialized.load() != 5)
        {
            std::cerr << "base loop was not initialized when IO thread count is zero\n";
            return 1;
        }
    }

    if (destroyed_on_owner.load() != 7 || destroyed_on_other.load() != 0)
    {
        std::cerr << "base loop resource was not destroyed by its owner thread: owner="
                  << destroyed_on_owner.load() << " other=" << destroyed_on_other.load() << '\n';
        return 1;
    }

    std::cout << "EventLoop init callback and context lifetime checks passed\n";
    return 0;
}
