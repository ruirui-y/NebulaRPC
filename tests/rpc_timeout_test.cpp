#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"
#include "nebula/rpc/rpc_server.h"
#include "echo.pb.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>

namespace
{

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

        call_count_.fetch_add(1);

        if (request->text() == "fast")
        {
            fast_call_count_.fetch_add(1);
        }
        else if (request->text() == "slow")
        {
            slow_call_count_.fetch_add(1);
        }

        const std::chrono::milliseconds delay =
            request->text() == "slow"
            ? std::chrono::milliseconds(250)
            : std::chrono::milliseconds(10);

        const std::string text = request->text();

        loop_->RunInLoop([this, text, response, done]
            {
                timer_fired_count_.fetch_add(1);

                if (text == "fast")
                {
                    fast_timer_fired_count_.fetch_add(1);
                }
                else if (text == "slow")
                {
                    slow_timer_fired_count_.fetch_add(1);
                }

                response->set_text(text);
                response->set_server_sequence(next_sequence_.fetch_add(1));

                if (done != nullptr)
                {
done_run_count_.fetch_add(1);
                    done->Run();
                }
            });
    }

    [[nodiscard]] int CallCount() const
    {
        return call_count_.load();
    }

    [[nodiscard]] int FastCallCount() const
    {
        return fast_call_count_.load();
    }

    [[nodiscard]] int SlowCallCount() const
    {
        return slow_call_count_.load();
    }

    [[nodiscard]] int TimerFiredCount() const
    {
        return timer_fired_count_.load();
    }

    [[nodiscard]] int FastTimerFiredCount() const
    {
        return fast_timer_fired_count_.load();
    }

    [[nodiscard]] int SlowTimerFiredCount() const
    {
        return slow_timer_fired_count_.load();
    }

    [[nodiscard]] int DoneRunCount() const
    {
        return done_run_count_.load();
    }

private:
    nebula::net::EventLoop* loop_;
    std::atomic_uint64_t next_sequence_{1};
    std::atomic_int call_count_{0};
    std::atomic_int fast_call_count_{0};
    std::atomic_int slow_call_count_{0};
    std::atomic_int timer_fired_count_{0};
    std::atomic_int fast_timer_fired_count_{0};
    std::atomic_int slow_timer_fired_count_{0};
    std::atomic_int done_run_count_{0};
};

struct CallContext
{
    nebula::example::EchoRequest request;
    nebula::example::EchoResponse response;
    nebula::rpc::RpcController controller;
};

}  // namespace

int main()
{
    using namespace std::chrono_literals;

    constexpr std::uint16_t kPort = 39001;

    nebula::net::EventLoop loop;
    nebula::rpc::RpcServer server(&loop, "127.0.0.1", kPort);
    DelayedEchoService service(&loop);

    server.RegisterService(&service);
    server.Start(0);

    nebula::rpc::RpcChannel channel(&loop, "127.0.0.1", kPort);
    nebula::example::EchoService_Stub stub(&channel);

    auto fast = std::make_shared<CallContext>();
    fast->request.set_text("fast");
    fast->controller.SetTimeout(500ms);

    auto slow = std::make_shared<CallContext>();
    slow->request.set_text("slow");
    slow->controller.SetTimeout(100ms);

    auto fast_done_count = std::make_shared<std::atomic_int>(0);
    auto slow_done_count = std::make_shared<std::atomic_int>(0);

    auto* fast_done = new nebula::rpc::RpcClosure(
        [fast, fast_done_count]
        {
            std::cout << "Fast done\n";
            fast_done_count->fetch_add(1);
        });

    auto* slow_done = new nebula::rpc::RpcClosure(
        [slow, slow_done_count]
        {
            std::cout << "Slow done\n";
            slow_done_count->fetch_add(1);
        });

    stub.Echo(&fast->controller,
              &fast->request,
              &fast->response,
              fast_done);

    stub.Echo(&slow->controller,
              &slow->request,
              &slow->response,
              slow_done);

    bool passed = false;

    loop.RunAfter(5000ms, [&]
        {
            std::cout << "RPC timeout test timeout\n";

const bool fast_ok =
                fast_done_count->load() == 1 &&
                !fast->controller.Failed() &&
                fast->response.text() == "fast";

            const bool slow_ok =
                slow_done_count->load() == 1 &&
                !slow->controller.Failed() &&
                slow->response.text() == "slow";

// ===== 新增打印日志 =====
            std::cout << "---------- RPC timeout test detail ----------\n";
            std::cout << "[fast] done_count=" << fast_done_count->load()
                << ", failed=" << fast->controller.Failed()
                << ", error_text=\"" << fast->controller.ErrorText() << "\""
                << ", response_text=\"" << fast->response.text() << "\""
                << "\n";
            std::cout << "[slow] done_count=" << slow_done_count->load()
                << ", failed=" << slow->controller.Failed()
                << ", error_text=\"" << slow->controller.ErrorText() << "\""
                << ", response_text=\"" << slow->response.text() << "\""
                << "\n";
std::cout << "---------------------------------------------\n";
            // ========================

            passed = fast_ok && slow_ok;
            loop.Quit();
        });

    loop.Loop();

    if (!passed)
    {
        std::cerr
            << "RPC timeout test failed\n"
            << "fast_done_count=" << fast_done_count->load()
            << ", fast_failed=" << fast->controller.Failed()
            << ", fast_error=" << fast->controller.ErrorText()
            << ", fast_response=" << fast->response.text()
            << ", fast_sequence=" << fast->response.server_sequence()
            << "\n"
            << "slow_done_count=" << slow_done_count->load()
            << ", slow_failed=" << slow->controller.Failed()
            << ", slow_error=" << slow->controller.ErrorText()
            << ", slow_response=" << slow->response.text()
            << ", slow_sequence=" << slow->response.server_sequence()
            << "\n"
            << "service_call_count=" << service.CallCount()
            << ", fast_call_count=" << service.FastCallCount()
            << ", slow_call_count=" << service.SlowCallCount()
            << "\n"
            << "timer_fired_count=" << service.TimerFiredCount()
            << ", fast_timer_fired_count=" << service.FastTimerFiredCount()
            << ", slow_timer_fired_count=" << service.SlowTimerFiredCount()
            << ", done_run_count=" << service.DoneRunCount()
            << "\n";

        return 1;
    }

    std::cout << "RPC timeout exactly-once test passed\n";
    return 0;
}
