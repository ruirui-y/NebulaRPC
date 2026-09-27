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

constexpr std::uint16_t kPort = 39006;
constexpr int kRejectedCount = 3;          // 掉线窗口里补发的请求数，全部应被当场拒绝
constexpr int kExpectedTotal = 1 + kRejectedCount + 1;
constexpr int kMaxPollTicks = 250;

constexpr auto kConnectGrace = std::chrono::milliseconds(200);   // 给首次建连留的窗口
constexpr auto kKillDelay = std::chrono::milliseconds(120);      // 等首发响应回来再掐服务端
constexpr auto kFinSettle = std::chrono::milliseconds(120);      // 等 FIN 走完一轮 epoll
constexpr auto kReconnectWait = std::chrono::milliseconds(400);  // 服务端重起后等退避把通道拉回来
constexpr auto kPollInterval = std::chrono::milliseconds(20);

class EchoServiceImpl final : public nebula::example::EchoService
{
public:
    void Echo(::google::protobuf::RpcController* controller,
              const ::nebula::example::EchoRequest* request,
              ::nebula::example::EchoResponse* response,
              ::google::protobuf::Closure* done) override
    {
        (void)controller;
        response->set_text(request->text());
        response->set_server_sequence(next_sequence_.fetch_add(1));

        if (done != nullptr)
        {
            done->Run();
        }
    }

private:
    std::atomic_uint64_t next_sequence_{1};
};

std::unique_ptr<nebula::rpc::RpcServer> MakeServer(nebula::net::EventLoop* loop,
                                                   EchoServiceImpl* service)
{
    auto server = std::make_unique<nebula::rpc::RpcServer>(loop, "127.0.0.1", kPort);
    server->RegisterService(service);
    server->Start(0);
    return server;
}

enum class Phase
{
    kInitial,         // 阶段 1：建连完成后发的第一发
    kDuringOutage,    // 阶段 2：连接已掉，应被当场拒绝
    kAfterReconnect,  // 阶段 3：自愈完成后发的第一发
};

struct CallContext
{
    nebula::example::EchoRequest request;
    nebula::example::EchoResponse response;
    nebula::rpc::RpcController controller;
    Phase phase{Phase::kInitial};
};

struct Stats
{
    int ok{0};
    int failed{0};
    int completed{0};
    int ok_initial{0};
    int failed_during_outage{0};
    int ok_after_reconnect{0};
};

class ReconnectTest
{
public:
    explicit ReconnectTest(nebula::net::EventLoop* loop)
        : loop_(loop),
          server_(MakeServer(loop, &service_)),   // 先起监听，channel 首次才能直接连上
          channel_(loop, "127.0.0.1", kPort),
          stub_(&channel_)
    {
    }

    void Run()
    {
        loop_->RunAfter(kConnectGrace, [this] { CallBeforeKill(); });
    }

    bool Report()
    {
        PrintDetail();

        Check(stats_.ok_initial == 1, "phase1: 初始 RPC 成功");
        Check(stats_.failed_during_outage == kRejectedCount, "phase2: 掉线窗口里的请求全部被拒");
        Check(stats_.ok == 2, "phase2: 被拒的请求一次都没发出去（ok 只该有首尾两发）");
        Check(stats_.ok_after_reconnect == 1, "phase3: 重连后新请求成功（自愈生效）");
        Check(stats_.completed == kExpectedTotal, "全部 done 都被执行（无挂单）");
        Check(!timed_out_, "未超时");

        std::cout << (pass_ ? "RPC reconnect test passed\n" : "RPC reconnect test failed\n");
        return pass_;
    }

private:
    // 阶段 1：建连完成后发第一发
    void CallBeforeKill()
    {
        Issue(1, Phase::kInitial);
        loop_->RunAfter(kKillDelay, [this] { KillServer(); });
    }

    void KillServer()
    {
        // 等价于进程被杀：内核代发 FIN，客户端走 HandleClose
        server_.reset();

        // 必须等 FIN 落下来：通道回到 kConnecting 之后，下一步的「当场拒绝」才真实
        loop_->RunAfter(kFinSettle, [this] { CallDuringOutage(); });
    }

    // 阶段 2：连接已掉，这批请求应被当场拒绝、一次都不发出
    void CallDuringOutage()
    {
        for (int i = 0; i < kRejectedCount; ++i)
        {
            Issue(i + 2, Phase::kDuringOutage);
        }

        server_ = MakeServer(loop_, &service_);
        loop_->RunAfter(kReconnectWait, [this] { CallAfterReconnect(); });
    }

    // 阶段 3：退避重连应已把通道拉回 kConnected
    void CallAfterReconnect()
    {
        Issue(2 + kRejectedCount, Phase::kAfterReconnect);
        SchedulePoll();
    }

    void Issue(int index, Phase phase)
    {
        auto context = std::make_shared<CallContext>();
        context->request.set_text("req#" + std::to_string(index));
        context->phase = phase;

        // RpcClosure 由框架在调用结束后 delete，这里只能 new
        auto* done = new nebula::rpc::RpcClosure([this, context] { OnCallDone(context); });
        stub_.Echo(&context->controller, &context->request, &context->response, done);
    }

    void OnCallDone(const std::shared_ptr<CallContext>& context)
    {
        stats_.completed += 1;

        if (context->controller.Failed())
        {
            stats_.failed += 1;

            if (first_error_.empty())
            {
                first_error_ = context->controller.ErrorText();
            }

            if (context->phase == Phase::kDuringOutage)
            {
                stats_.failed_during_outage += 1;
            }
            return;
        }

        stats_.ok += 1;

        if (context->phase == Phase::kInitial)
        {
            stats_.ok_initial += 1;
        }
        else if (context->phase == Phase::kAfterReconnect)
        {
            stats_.ok_after_reconnect += 1;
        }
    }

    void SchedulePoll()
    {
        loop_->RunAfter(kPollInterval, [this] { Poll(); });
    }

    void Poll()
    {
        if (stats_.completed >= kExpectedTotal)
        {
            Finish();
            return;
        }

        if (++poll_ticks_ > kMaxPollTicks)
        {
            timed_out_ = true;
            Finish();
            return;
        }

        SchedulePoll();
    }

    void Finish()
    {
        if (!finished_)
        {
            finished_ = true;
            loop_->Quit();
        }
    }

    void PrintDetail() const
    {
        std::cout << "---------- rpc reconnect detail ----------\n";
        std::cout << "[phase1] ok_initial=" << stats_.ok_initial << '\n';
        std::cout << "[phase2] failed_during_outage=" << stats_.failed_during_outage << '\n';
        std::cout << "[phase3] ok_after_reconnect=" << stats_.ok_after_reconnect << '\n';
        std::cout << "[final]  completed=" << stats_.completed
                  << ", ok=" << stats_.ok
                  << ", failed=" << stats_.failed
                  << ", timed_out=" << (timed_out_ ? 1 : 0) << '\n';

        if (!first_error_.empty())
        {
            std::cout << "[final]  first_error=\"" << first_error_ << "\"\n";
        }

        std::cout << "-----------------------------------------\n";
    }

    void Check(bool condition, const char* name)
    {
        std::cout << (condition ? "[ok]   " : "[FAIL] ") << name << '\n';

        if (!condition)
        {
            pass_ = false;
        }
    }

    nebula::net::EventLoop* loop_;
    EchoServiceImpl service_;
    std::unique_ptr<nebula::rpc::RpcServer> server_;
    nebula::rpc::RpcChannel channel_;
    nebula::example::EchoService_Stub stub_;

    Stats stats_;
    std::string first_error_;
    int poll_ticks_{0};
    bool finished_{false};
    bool timed_out_{false};
    bool pass_{true};
};

}  // namespace

int main()
{
    nebula::net::EventLoop loop;
    ReconnectTest test(&loop);

    test.Run();
    loop.Loop();

    return test.Report() ? 0 : 1;
}
