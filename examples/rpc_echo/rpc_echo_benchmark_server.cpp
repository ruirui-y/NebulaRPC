// Minimal echo server for performance runs. Same RpcServer and EchoService as
// the examples, but no per-request debug logging or console output.
#include "nebula/base/logger.h"
#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_server.h"
#include "echo.pb.h"

#include <spdlog/spdlog.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>

namespace
{
    class EchoServiceImpl final : public nebula::example::EchoService
    {
    public:
        void Echo(::google::protobuf::RpcController*,
                  const nebula::example::EchoRequest* request,
                  nebula::example::EchoResponse* response,
                  ::google::protobuf::Closure* done) override
        {
            response->set_text(request->text());
            response->set_server_sequence(seq_.fetch_add(1, std::memory_order_relaxed));

            if (done)
            {
                done->Run();
            }
        }

    private:
        std::atomic<std::uint64_t> seq_{ 1 };
    };
}  // namespace

int main(int argc, char** argv)
{
    const unsigned long port = argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 9000UL;
    const unsigned long io_threads = argc > 2 ? std::strtoul(argv[2], nullptr, 10) : 0UL;

    if (port == 0 || port > 65535 || io_threads > 128)
    {
        std::cerr << "Usage: " << argv[0] << " [port=9000] [io_threads=0]\n";
        return 2;
    }

    nebula::base::InitLogger(spdlog::level::warn);

    nebula::net::EventLoop loop;
    nebula::rpc::RpcServer server(&loop, "0.0.0.0", static_cast<std::uint16_t>(port));

    EchoServiceImpl service;
    server.RegisterService(&service);
    server.Start(static_cast<std::size_t>(io_threads));

    std::cout << "NebulaRPC benchmark Echo server listening at 0.0.0.0:" << port
              << " IO threads=" << io_threads << '\n';

    loop.Loop();
    return 0;
}
