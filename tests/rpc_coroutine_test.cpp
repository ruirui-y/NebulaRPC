#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_awaiter.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_error.h"
#include "nebula/rpc/rpc_server.h"
#include "nebula/rpc/rpc_task.h"
#include "echo.pb.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <string>

namespace
{

using nebula::example::EchoRequest;
using nebula::example::EchoResponse;

// 验证四件事：co_await 拿到响应、超时抛 RpcError、丢弃 Task 不崩、之后通道仍可用
class DelayedEchoService final : public nebula::example::EchoService
{
public:
    explicit DelayedEchoService(nebula::net::EventLoop* loop)
        : loop_(loop)
    {
    }

    void Echo(::google::protobuf::RpcController* controller,
              const ::nebula::example::EchoRequest* request,
              ::nebula::example::EchoResponse* response,
              ::google::protobuf::Closure* done) override
    {
        (void)controller;

        if (request->text() == "slow")
        {
            slow_call_count_.fetch_add(1);
        }

        const std::chrono::milliseconds delay = request->text() == "slow"
            ? std::chrono::milliseconds(250)
            : std::chrono::milliseconds(10);

        const std::string text = request->text();

        // 真正延迟回包：slow 请求 250ms 后才响应，晚于客户端 100ms 超时
        loop_->RunAfter(delay, [this, text, response, done]
            {
                timer_fired_count_.fetch_add(1);
                response->set_text(text);
                response->set_server_sequence(next_sequence_.fetch_add(1));

                if (done != nullptr)
                {
                    done->Run();
                }
            });
    }

    [[nodiscard]] int SlowCallCount() const
    {
        return slow_call_count_.load();
    }

    [[nodiscard]] int TimerFiredCount() const
    {
        return timer_fired_count_.load();
    }

private:
    nebula::net::EventLoop* loop_;
    std::atomic_uint64_t next_sequence_{1};
    std::atomic_int slow_call_count_{0};
    std::atomic_int timer_fired_count_{0};
};

struct Outcome
{
    bool fast_ok{false};
    bool timeout_ok{false};
    bool reuse_ok{false};
    std::string fast_text;
    std::string timeout_reason;
    nebula::rpc::RpcCallState timeout_state{nebula::rpc::RpcCallState::Pending};
};

const google::protobuf::MethodDescriptor* EchoMethod()
{
    return nebula::rpc::FindMethod(nebula::example::EchoService::descriptor(), "Echo");
}

// 主体协程：正常调用 → 超时调用 → 取消/超时之后通道仍可用
nebula::rpc::Task<int> RunCases(nebula::rpc::RpcChannel& channel, Outcome& outcome)
{
    EchoRequest fast;
    fast.set_text("fast");

    try
    {
        EchoResponse response = co_await nebula::rpc::RpcAwaiter<EchoRequest, EchoResponse>(
            &channel, EchoMethod(), fast, std::chrono::milliseconds(500));

        outcome.fast_text = response.text();
        outcome.fast_ok = (response.text() == "fast");
    }
    catch (const nebula::rpc::RpcError& error)
    {
        outcome.fast_text = std::string("threw: ") + error.what();
    }

    EchoRequest slow;
    slow.set_text("slow");

    try
    {
        EchoResponse response = co_await nebula::rpc::RpcAwaiter<EchoRequest, EchoResponse>(
            &channel, EchoMethod(), slow, std::chrono::milliseconds(100));

        outcome.timeout_reason = "no throw, got: " + response.text();
    }
    catch (const nebula::rpc::RpcError& error)
    {
        outcome.timeout_state = error.State();
        outcome.timeout_reason = error.what();
        outcome.timeout_ok = (error.State() == nebula::rpc::RpcCallState::Timeout);
    }

    EchoRequest again;
    again.set_text("fast");

    try
    {
        EchoResponse response = co_await nebula::rpc::RpcAwaiter<EchoRequest, EchoResponse>(
            &channel, EchoMethod(), again, std::chrono::milliseconds(500));

        outcome.reuse_ok = (response.text() == "fast");
    }
    catch (const nebula::rpc::RpcError&)
    {
        outcome.reuse_ok = false;
    }

    co_return (outcome.fast_ok && outcome.timeout_ok && outcome.reuse_ok) ? 0 : 1;
}

// 发了就丢：Task 析构 → 销毁协程帧 → awaiter 析构 → 取消在途调用
nebula::rpc::Task<int> RunDropped(nebula::rpc::RpcChannel& channel)
{
    EchoRequest slow;
    slow.set_text("slow");

    [[maybe_unused]] const EchoResponse response =
        co_await nebula::rpc::RpcAwaiter<EchoRequest, EchoResponse>(
            &channel, EchoMethod(), slow, std::chrono::milliseconds(5000));

    co_return 0;
}

}  // namespace

int main()
{
    using namespace std::chrono_literals;

    constexpr std::uint16_t kPort = 39002;

    nebula::net::EventLoop loop;
    nebula::rpc::RpcServer server(&loop, "127.0.0.1", kPort);
    DelayedEchoService service(&loop);

    server.RegisterService(&service);
    server.Start(0);

    nebula::rpc::RpcChannel channel(&loop, "127.0.0.1", kPort);

    Outcome outcome;

    // 中途丢弃 Task：若取消没生效，250ms 后的迟到响应会打进已释放的协程帧
    {
        nebula::rpc::Task<int> dropped = RunDropped(channel);
        dropped.Start();
    }

    nebula::rpc::Task<int> cases = RunCases(channel, outcome);
    cases.Start();

    bool finished = false;

    loop.RunAfter(3000ms, [&]
        {
            finished = cases.Done();
            loop.Quit();
        });

    loop.Loop();

    bool passed = false;

    if (finished)
    {
        try
        {
            passed = (cases.Result() == 0);
        }
        catch (const std::exception& error)
        {
            std::cerr << "coroutine task threw: " << error.what() << '\n';
        }
    }

    std::cout << "---------- RPC coroutine test detail ----------\n";
    std::cout << "[case1] await response ok=" << outcome.fast_ok
              << ", text=\"" << outcome.fast_text << "\"\n";
    std::cout << "[case2] timeout ok=" << outcome.timeout_ok
              << ", state=" << static_cast<int>(outcome.timeout_state)
              << ", reason=\"" << outcome.timeout_reason << "\"\n";
    std::cout << "[case3] channel reusable after timeout ok=" << outcome.reuse_ok << "\n";
    std::cout << "[case4] dropped task destroyed, slow_call_count="
              << service.SlowCallCount()
              << ", timer_fired_count=" << service.TimerFiredCount() << "\n";
    std::cout << "-----------------------------------------------\n";

    if (!passed)
    {
        std::cerr << "RPC coroutine test failed (task finished=" << finished << ")\n";
        return 1;
    }

    std::cout << "RPC coroutine test passed\n";
    return 0;
}
