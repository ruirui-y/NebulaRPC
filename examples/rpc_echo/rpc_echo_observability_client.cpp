#include "nebula/base/logger.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/timer_id.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"
#include "echo.pb.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

namespace
{

constexpr auto kSendInterval = std::chrono::milliseconds(200);
constexpr auto kMetricsInterval = std::chrono::seconds(1);
constexpr auto kRunDuration = std::chrono::seconds(4);

// 请求/响应/控制器必须活到异步 done 执行，一个在途调用一套
struct CallContext
{
    nebula::example::EchoRequest request;
    nebula::example::EchoResponse response;
    nebula::rpc::RpcController controller;
};

// 持续打流并周期输出指标：让「发起 -> 完成」的日志与 QPS/分位数同时可见
class ObservabilityClient
{
public:
    ObservabilityClient(nebula::net::EventLoop* loop, std::string ip, std::uint16_t port)
        : loop_(loop),
          channel_(loop, std::move(ip), port),
          stub_(&channel_)
    {
    }

    void Run()
    {
        channel_.StartMetricsReport(kMetricsInterval);

        send_timer_ = loop_->RunAfter(kSendInterval, [this]
            {
                SendOne();
            });

        loop_->RunAfter(kRunDuration, [this]
            {
                Finish();
            });
    }

private:
    void SendOne()
    {
        auto context = std::make_shared<CallContext>();
        context->request.set_text("observability probe #" + std::to_string(sequence_++));

        auto* done = new nebula::rpc::RpcClosure([this, context]
            {
                OnDone(context);
            });

        stub_.Echo(&context->controller, &context->request, &context->response, done);

        send_timer_ = loop_->RunAfter(kSendInterval, [this]
            {
                SendOne();
            });
    }

    void OnDone(const std::shared_ptr<CallContext>& context)
    {
        // 失败不打屏：它已经进了 failed 计数与 error 字段，指标里看得到
        if (!context->controller.Failed())
        {
            ++completed_;
        }
    }

    void Finish()
    {
        if (send_timer_.Valid())
        {
            loop_->CancelTimer(send_timer_);
            send_timer_ = {};
        }

        channel_.StopMetricsReport();

        std::cout << "completed=" << completed_
                  << " in_flight=" << channel_.PendingCallCount() << '\n';

        loop_->Quit();
    }

    nebula::net::EventLoop* loop_;
    nebula::rpc::RpcChannel channel_;
    nebula::example::EchoService_Stub stub_;
    nebula::net::TimerId send_timer_;
    std::uint64_t sequence_{1};
    std::uint64_t completed_{0};
};

}  // namespace

int main(int argc, char** argv)
{
    const std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    const std::uint16_t port = argc > 2
        ? static_cast<std::uint16_t>(std::atoi(argv[2]))
        : 9000;

    nebula::base::InitLogger();

    nebula::net::EventLoop loop;
    ObservabilityClient client(&loop, ip, port);
    client.Run();

    loop.Loop();
    return 0;
}
