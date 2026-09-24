#include "nebula/net/event_loop.h"
#include "nebula/net/event_loop_thread.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"
#include "nebula/rpc/rpc_server.h"
#include "echo.pb.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>

namespace
{

// 立即回包的 echo 服务：让响应尽快到达，与取消形成竞争
class ImmediateEchoService final : public nebula::example::EchoService
{
public:
    void Echo(::google::protobuf::RpcController* controller,
              const ::nebula::example::EchoRequest* request,
              ::nebula::example::EchoResponse* response,
              ::google::protobuf::Closure* done) override
    {
        (void)controller;

        response->set_text(request->text());

        if (done != nullptr)
        {
            done->Run();
        }
    }
};

struct CallContext
{
    nebula::example::EchoRequest request;
    nebula::example::EchoResponse response;
    nebula::rpc::RpcController controller;

    std::mutex mutex;
    std::condition_variable condition;
    std::atomic_int done_count{0};
};

}  // namespace

int main()
{
    using namespace std::chrono_literals;

    constexpr std::uint16_t kPort = 39002;
    constexpr int kRounds = 1000;

    // ---- 服务端与客户端分属不同线程的 EventLoop ----
    nebula::net::EventLoopThread server_thread;
    nebula::net::EventLoop* server_loop = server_thread.StartLoop();

    nebula::rpc::RpcServer server(server_loop, "127.0.0.1", kPort);
    ImmediateEchoService service;
    server.RegisterService(&service);
    server.Start(0);

    nebula::net::EventLoopThread client_thread;
    nebula::net::EventLoop* client_loop = client_thread.StartLoop();

    nebula::rpc::RpcChannel channel(client_loop, "127.0.0.1", kPort);
    nebula::example::EchoService_Stub stub(&channel);

    // ---- 第一轮：先等连接建立，避免把连接竞争混入取消竞争 ----
    {
        auto warmup = std::make_shared<CallContext>();
        warmup->request.set_text("warmup");

        auto* warmup_done = new nebula::rpc::RpcClosure(
            [warmup]
            {
                std::lock_guard<std::mutex> lock(warmup->mutex);
                warmup->done_count.fetch_add(1);
                warmup->condition.notify_all();
            });

        stub.Echo(&warmup->controller,
                  &warmup->request,
                  &warmup->response,
                  warmup_done);

        std::unique_lock<std::mutex> lock(warmup->mutex);

        if (!warmup->condition.wait_for(lock, 3s,
                [&warmup]
                {
                    return warmup->done_count.load() == 1;
                }))
        {
            std::cerr << "warmup RPC did not complete\n";
            return 1;
        }

        if (warmup->controller.Failed())
        {
            std::cerr << "warmup RPC failed: "
                      << warmup->controller.ErrorText() << "\n";
            return 1;
        }
    }

    // ---- 竞争主循环：主线程发起 RPC 后立即 StartCancel，与 loop 线程的响应完成竞争，done 恰好一次 ----
    for (int round = 0; round < kRounds; ++round)
    {
        auto context = std::make_shared<CallContext>();
        context->request.set_text("race");

        auto* done = new nebula::rpc::RpcClosure(
            [context]
            {
                std::lock_guard<std::mutex> lock(context->mutex);
                context->done_count.fetch_add(1);
                context->condition.notify_all();
            });

        stub.Echo(&context->controller,
                  &context->request,
                  &context->response,
                  done);

        // 业务线程随时取消：可能早于注册、早于响应、晚于响应
        context->controller.StartCancel();

        std::unique_lock<std::mutex> lock(context->mutex);
        const bool completed = context->condition.wait_for(lock, 3s,
            [&context]
            {
                return context->done_count.load() >= 1;
            });
        lock.unlock();

        if (!completed)
        {
            std::cerr << "round " << round << ": done never ran\n";
            return 1;
        }

        // 多等一拍，确认没有第二次回调
        std::this_thread::sleep_for(1ms);

        const int done_count = context->done_count.load();

        if (done_count != 1)
        {
            std::cerr << "round " << round
                      << ": done ran " << done_count << " times\n";
            return 1;
        }

        // ---- 终态自洽：要么拿到响应，要么响应为空（取消赢），二者不并存 ----
        const bool got_response = context->response.text() == "race";
        const bool got_cancel = context->response.text().empty();

        if (got_response == got_cancel)
        {
            std::cerr << "round " << round
                      << ": inconsistent final state, text=\""
                      << context->response.text() << "\"\n";
            return 1;
        }

        // IsCanceled() 是终态：取消赢 ⟺ true；MarkCanceled 排在 done 之前，读到的是终值
        if (context->controller.IsCanceled() != got_cancel)
        {
            std::cerr << "round " << round << ": IsCanceled="
                      << context->controller.IsCanceled()
                      << " but cancel_won=" << got_cancel << "\n";
            return 1;
        }

        // 取消不算失败：任何一轮出现 Failed 都说明路径错误
        if (context->controller.Failed())
        {
            std::cerr << "round " << round
                      << ": unexpected failure: "
                      << context->controller.ErrorText() << "\n";
            return 1;
        }
    }

    std::cout << "rpc cancel race test passed: "
              << kRounds << " rounds, done exactly once each\n";
    return 0;
}
