#include "nebula/base/logger.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/rpc/rpc_server.h"
#include "echo.pb.h"

#include <atomic>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>

namespace
{

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

// 信号处理函数只能碰全局对象：闭包捕获是信号上下文里的未定义行为
nebula::net::EventLoop* g_loop = nullptr;

}  // namespace

extern "C" void HandleSigterm(int)
{
    // 只置位 + 写 eventfd，真正的 Stop 回到 loop 线程执行
    if (g_loop != nullptr)
    {
        g_loop->NotifyFromSignal();
    }
}

int main(int argc, char** argv)
{
    const std::uint16_t port = argc > 1 ? static_cast<std::uint16_t>(std::atoi(argv[1])) : 9000;

    nebula::base::InitLogger();

    nebula::net::EventLoop loop;
    nebula::rpc::RpcServer server(&loop, "0.0.0.0", port);
    EchoServiceImpl echo_service;
    server.RegisterService(&echo_service);

    // 软水位只做可观测；越过硬上限才真的踢连接，那是内存的最终闸门
    server.SetHighWatermarkCallback(
        [](const nebula::net::TcpConnectionPtr& conn, std::size_t pending_bytes)
        {
            std::cout << "[backpressure] slow consumer on " << conn->Name()
                      << ", pending=" << pending_bytes << " bytes\n";
        },
        4U * 1024U * 1024U);

    server.SetMaxOutputBufferBytes(16U * 1024U * 1024U);

    // SIGTERM -> 停 accept -> 等在途请求清零（上限 10s）-> 逐个 Shutdown -> 退出
    g_loop = &loop;
    server.SetShutdownCompleteCallback([&loop]
        {
            loop.Quit();
        });
    loop.SetSignalCallback([&server]
        {
            server.Stop();
        });
    std::signal(SIGTERM, HandleSigterm);

    server.Start(0);

    std::cout << "NebulaRPC RPC echo server listening on 0.0.0.0:" << port
              << " (SIGTERM triggers graceful shutdown)\n";
    loop.Loop();
    return 0;
}
