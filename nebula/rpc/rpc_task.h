#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <stdexcept>
#include <utility>

namespace nebula::rpc
{

// 惰性任务：Start() 才跑；帧由 Task 持有并销毁
// 不实现 await_*（co_await 一个 Task 编不过），分析见 notes/knowledge/01_C++20_Coroutines.md 13.4~13.6
template <typename T>
class Task
{
public:
    struct promise_type
    {
        std::optional<T> value{};
        std::exception_ptr exception{};

        Task get_return_object() noexcept
        {
            return Task(std::coroutine_handle<promise_type>::from_promise(*this));
        }

        std::suspend_always initial_suspend() noexcept
        {
            return {};
        }

        // 必须挂住：换成 suspend_never，编译器当场释放帧，Result() 读野内存 + ~Task 双重释放
        std::suspend_always final_suspend() noexcept
        {
            return {};
        }

        void return_value(T result)
        {
            value.emplace(std::move(result));
        }

        void unhandled_exception() noexcept
        {
            exception = std::current_exception();
        }
    };

    Task() = default;

    explicit Task(std::coroutine_handle<promise_type> handle)
        : handle_(handle)
    {
    }

    ~Task()
    {
        if (handle_ != nullptr)
        {
            handle_.destroy();
        }
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : handle_(std::exchange(other.handle_, nullptr)),
          started_(std::exchange(other.started_, false))
    {
    }

    Task& operator=(Task&& other) noexcept
    {
        if (this != &other)
        {
            if (handle_ != nullptr)
            {
                handle_.destroy();
            }

            handle_ = std::exchange(other.handle_, nullptr);
            started_ = std::exchange(other.started_, false);
        }

        return *this;
    }

    // 唯一驱动入口：重复 resume 会让协程从挂起点提前蹿出去
    void Start()
    {
        if (handle_ == nullptr || started_ || handle_.done())
        {
            return;
        }

        started_ = true;
        handle_.resume();
    }

    [[nodiscard]] bool Done() const noexcept
    {
        return handle_ == nullptr || handle_.done();
    }

    // 顶层取结果的唯一出口：协程内抛的异常在这里重抛
    T Result()
    {
        if (handle_ == nullptr)
        {
            throw std::logic_error("Task::Result called on an empty task");
        }

        if (!handle_.done())
        {
            throw std::logic_error("Task::Result called before the task finished");
        }

        return TakeValue();
    }

private:
    T TakeValue()
    {
        if (handle_.promise().exception != nullptr)
        {
            std::rethrow_exception(handle_.promise().exception);
        }

        return std::move(*handle_.promise().value);
    }

    std::coroutine_handle<promise_type> handle_{};
    bool started_{false};
};

}  // namespace nebula::rpc
