#include "nebula/base/logger.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/rpc/rpc_coroutine.h"
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

    nebula::net::EventLoop* g_loop = nullptr;

}  // namespace

extern "C" void HandleSigterm(int)
{
    if (g_loop != nullptr)
    {
        g_loop->NotifyFromSignal();
    }
}

int main(int argc, char** argv)
{
    const std::uint16_t port = argc > 1
        ? static_cast<std::uint16_t>(std::atoi(argv[1]))
        : 9000;

    nebula::base::InitLogger();

    nebula::net::EventLoop loop;
    nebula::rpc::RpcServer server(&loop, "0.0.0.0", port);

    std::atomic_uint64_t next_sequence{ 1 };

    nebula::rpc::CoroutineService echo_service(nebula::example::EchoService::descriptor());

    echo_service.Register<
        nebula::example::EchoRequest,
        nebula::example::EchoResponse>(
            "Echo",
            [&next_sequence](nebula::rpc::RpcServerContext&,
                const nebula::example::EchoRequest& request)
            -> nebula::coro::Task<nebula::example::EchoResponse>
            {
                nebula::example::EchoResponse response;
                response.set_text(request.text());
                response.set_server_sequence(next_sequence.fetch_add(1));
                co_return response;
            });

    server.RegisterCoroutineService(&echo_service);

    server.SetHighWatermarkCallback(
        [](const nebula::net::TcpConnectionPtr& conn,
            std::size_t pending_bytes)
        {
            std::cout << "[backpressure] slow consumer on "
                << conn->Name()
                << ", pending=" << pending_bytes
                << " bytes\n";
        },
        4U * 1024U * 1024U);

    server.SetMaxOutputBufferBytes(16U * 1024U * 1024U);

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

    std::cout << "NebulaRPC coroutine echo server listening on 0.0.0.0:"
        << port
        << " (SIGTERM triggers graceful shutdown)\n";

    loop.Loop();
    return 0;
}