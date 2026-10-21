#pragma once

#include <concepts>
#include <coroutine>
#include <exception>
#include <optional>
#include <stdexcept>
#include <utility>

namespace nebula::coro
{
namespace detail
{

class TaskPromiseBase
{
public:
    // Lazy Task：创建协程后先挂起，由 Start() 或 co_await 再启动。
    std::suspend_always initial_suspend() noexcept
    {
        return {};
    }

    struct FinalAwaiter
    {
        bool await_ready() const noexcept
        {
            return false;
        }

        template <typename Promise>
        std::coroutine_handle<> await_suspend(
            std::coroutine_handle<Promise> handle) const noexcept
        {
            // continuation 保存等待当前子协程完成的父协程句柄。
            const std::coroutine_handle<> continuation = handle.promise().continuation_;

            // 子协程结束后直接对称转移回父协程；顶层 Task 没有父协程则什么都不做。
            return continuation ? continuation : std::noop_coroutine();
        }

        void await_resume() const noexcept
        {
        }
    };

    FinalAwaiter final_suspend() noexcept
    {
        return {};
    }

    void unhandled_exception() noexcept
    {
        exception_ = std::current_exception();
    }

    void SetContinuation(std::coroutine_handle<> continuation) noexcept
    {
        continuation_ = continuation;
    }

    void RethrowIfNeeded()
    {
        if (exception_ != nullptr)
        {
            std::rethrow_exception(exception_);
        }
    }

private:
    std::coroutine_handle<> continuation_{};
    std::exception_ptr exception_{};
};


template <typename Promise>
class TaskAwaiter final
{
public:
    using Handle = std::coroutine_handle<Promise>;

    explicit TaskAwaiter(Handle handle) noexcept
        : handle_(handle)
    {
    }

    ~TaskAwaiter()
    {
        // co_await 完成后由 Awaiter 释放接管的子协程 frame。
        if (handle_ != nullptr)
        {
            handle_.destroy();
        }
    }

    TaskAwaiter(const TaskAwaiter&) = delete;
    TaskAwaiter& operator=(const TaskAwaiter&) = delete;
    TaskAwaiter(TaskAwaiter&&) = delete;
    TaskAwaiter& operator=(TaskAwaiter&&) = delete;

    bool await_ready() const noexcept
    {
        return handle_ == nullptr || handle_.done();
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<> continuation) noexcept
    {
        // 保存父协程句柄，子协程 final_suspend 时才能知道要恢复谁。
        handle_.promise().SetContinuation(continuation);

        // 返回子协程句柄，通过 Symmetric Transfer 直接切换到子协程执行。
        return handle_;
    }

    decltype(auto) await_resume()
    {
        if (handle_ == nullptr)
        {
            throw std::logic_error("co_await on an empty Task");
        }

        return handle_.promise().FinalizeResult();
    }

private:
    Handle handle_{};
};

} // namespace detail


template <typename T>
class Task
{
public:
    struct promise_type final : detail::TaskPromiseBase
    {
        Task get_return_object() noexcept
        {
            return Task(
                std::coroutine_handle<promise_type>::from_promise(*this));
        }

        template <typename U>
        requires std::convertible_to<U, T>
        void return_value(U&& value)
        {
            value_.emplace(std::forward<U>(value));
        }

        T FinalizeResult()
        {
            this->RethrowIfNeeded();

            if (!value_.has_value())
            {
                throw std::logic_error(
                    "Task result is unavailable or already consumed");
            }

            // Task<T> 的返回值只允许消费一次。
            T result = std::move(*value_);
            value_.reset();

            return result;
        }

    private:
        std::optional<T> value_{};
    };


    using Handle = std::coroutine_handle<promise_type>;

    Task() = default;

    explicit Task(Handle handle) noexcept
        : handle_(handle)
    {
    }

    ~Task()
    {
        Reset();
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
            Reset();

            handle_ = std::exchange(other.handle_, nullptr);
            started_ = std::exchange(other.started_, false);
        }

        return *this;
    }

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

    T Result()
    {
        if (handle_ == nullptr)
        {
            throw std::logic_error(
                "Task::Result called on an empty task");
        }

        if (!handle_.done())
        {
            throw std::logic_error(
                "Task::Result called before the task finished");
        }

        return handle_.promise().FinalizeResult();
    }


    using Awaiter = detail::TaskAwaiter<promise_type>;

    Awaiter operator co_await() &&
    {
        if (started_)
        {
            throw std::logic_error(
                "cannot co_await a Task that was already started");
        }

        started_ = true;

        // co_await 后 frame 所有权从 Task 转移给 Awaiter。
        return Awaiter(std::exchange(handle_, nullptr));
    }

private:
    void Reset() noexcept
    {
        if (handle_ != nullptr)
        {
            handle_.destroy();
            handle_ = nullptr;
        }

        started_ = false;
    }

    Handle handle_{};
    bool started_{false};
};


template <>
class Task<void>
{
public:
    struct promise_type final : detail::TaskPromiseBase
    {
        Task get_return_object() noexcept
        {
            return Task(
                std::coroutine_handle<promise_type>::from_promise(*this));
        }

        void return_void() noexcept
        {
        }

        void FinalizeResult()
        {
            this->RethrowIfNeeded();
        }
    };


    using Handle = std::coroutine_handle<promise_type>;

    Task() = default;

    explicit Task(Handle handle) noexcept
        : handle_(handle)
    {
    }

    ~Task()
    {
        Reset();
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
            Reset();

            handle_ = std::exchange(other.handle_, nullptr);
            started_ = std::exchange(other.started_, false);
        }

        return *this;
    }

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

    void Result()
    {
        if (handle_ == nullptr)
        {
            throw std::logic_error(
                "Task::Result called on an empty task");
        }

        if (!handle_.done())
        {
            throw std::logic_error(
                "Task::Result called before the task finished");
        }

        handle_.promise().FinalizeResult();
    }


    using Awaiter = detail::TaskAwaiter<promise_type>;

    Awaiter operator co_await() &&
    {
        if (started_)
        {
            throw std::logic_error(
                "cannot co_await a Task that was already started");
        }

        started_ = true;

        // co_await 后 frame 所有权从 Task 转移给 Awaiter。
        return Awaiter(std::exchange(handle_, nullptr));
    }

private:
    void Reset() noexcept
    {
        if (handle_ != nullptr)
        {
            handle_.destroy();
            handle_ = nullptr;
        }

        started_ = false;
    }

    Handle handle_{};
    bool started_{false};
};


// Fire-and-forget Task：立即执行，执行到结束后自动销毁 coroutine frame。
class DetachedTask final
{
public:
    struct promise_type
    {
        DetachedTask get_return_object() noexcept
        {
            return {};
        }

        std::suspend_never initial_suspend() noexcept
        {
            return {};
        }

        std::suspend_never final_suspend() noexcept
        {
            return {};
        }

        void return_void() noexcept
        {
        }

        void unhandled_exception() noexcept
        {
            std::terminate();
        }
    };
};

} // namespace nebula::coro