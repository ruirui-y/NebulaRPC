// 探针 01：co_await 到底在哪一刻交出控制权
//
// 要证伪的那个直觉：
//   「await_ready 返回 false -> 执行 await_suspend -> await_suspend 执行完
//     -> 接着执行 co_await 的下一行」
//
// 实际发生的：
//   await_suspend 返回 void 后，控制权回到【resumer】（这里是 main 的 Start() 之后），
//   co_await 的下一行必须等【别人再次 resume】才会执行。
//
// 跑一遍看输出顺序即可，不需要调试器；想单步就在 VS 里对本目录 F5。

#include <coroutine>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <utility>

// 对齐 nebula/rpc/rpc_task.h 的骨架：初始挂起 + final 挂起 + 手工 Start
struct Task
{
    struct promise_type
    {
        Task get_return_object() noexcept
        {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept
        {
            return {};
        }

        std::suspend_always final_suspend() noexcept
        {
            return {};
        }

        void return_void() noexcept
        {
        }

        void unhandled_exception()
        {
            std::terminate();
        }
    };

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept : handle_(handle)
    {
    }

    Task(Task&& other) noexcept : handle_(std::exchange(other.handle_, {}))
    {
    }

    Task& operator=(Task&&) = delete;

    ~Task()
    {
        if (handle_)
        {
            handle_.destroy();
        }
    }

    void Start() const
    {
        handle_.resume();
    }

    void Resume() const
    {
        handle_.resume();
    }

    bool Done() const
    {
        return handle_.done();
    }

    std::coroutine_handle<promise_type> handle_{};
};

// 探针 awaiter：三个 await 函数一被调用就打印，另外把「协程此刻是否已挂起」也标出来
struct Probe
{
    const char* tag;

    bool await_ready() const noexcept
    {
        std::printf("        [%s] await_ready()   -> false\n", tag);
        return false;
    }

    void await_suspend(std::coroutine_handle<> handle) const noexcept
    {
        (void)handle;
        std::printf("        [%s] await_suspend() 进入   <- 协程【此刻已经被视为挂起】\n", tag);
        std::printf("        [%s] await_suspend() 返回 void = 保持挂起\n", tag);
    }

    void await_resume() const noexcept
    {
        std::printf("        [%s] await_resume()  <- co_await 表达式在这里取到值\n", tag);
    }
};

Task Coro()
{
    std::printf("      <Coro> 函数体【开始】执行 —— 说明协程真被 resume 了\n");
    std::printf("      <Coro> 马上 co_await\n");
    co_await Probe{"A"};
    std::printf("      <Coro> co_await 的【下一行】   <== 必须等再次 resume 才到这里\n");
    co_return;
}

int main()
{
#ifdef _WIN32
    // Windows 控制台默认 GBK(936)，源码按 UTF-8 编译，不切代码页中文会乱码
    std::system("chcp 65001 > nul");
#endif

    std::printf("--- 步骤 1：构造协程对象（initial_suspend 挂住，函数体一行都没跑）---\n");
    Task task = Coro();

    std::printf("--- 步骤 2：构造完成。上面没有 <Coro> 字样 => 函数体确实没执行 ---\n\n");

    std::printf("--- 步骤 3：Start() -> resume，跑进 Coro 直到 co_await ---\n");
    task.Start();

    std::printf("--- 步骤 4：Start() 已经返回了！ <== 线程回到 main，不是 co_await 的下一行 ---\n");
    std::printf("                Done()=%d（false = 协程还挂着）\n\n",
                static_cast<int>(task.Done()));

    std::printf("--- 步骤 5：手动 resume（现实中这里换成 IO 就绪 / 超时定时器 / done 回调）---\n");
    task.Resume();

    std::printf("--- 步骤 6：resume 返回，Done()=%d（true = 协程跑完，停在 final suspend）---\n",
                static_cast<int>(task.Done()));
    return 0;
}
