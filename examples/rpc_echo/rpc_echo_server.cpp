#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/rpc/rpc_server.h"
#include "echo.pb.h"

#include <atomic>
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

}  // namespace

int main(int argc, char** argv)
{
    const std::uint16_t port = argc > 1 ? static_cast<std::uint16_t>(std::atoi(argv[1])) : 9000;

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

    server.Start(0);

    std::cout << "NebulaRPC RPC echo server listening on 0.0.0.0:" << port << '\n';
    loop.Loop();
    return 0;
}
