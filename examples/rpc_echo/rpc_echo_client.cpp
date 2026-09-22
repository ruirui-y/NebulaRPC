#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"
#include "echo.pb.h"

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

namespace
{

struct CallContext
{
    nebula::example::EchoRequest request;
    nebula::example::EchoResponse response;
    nebula::rpc::RpcController controller;
};

}  // namespace

int main(int argc, char** argv)
{
    const std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    const std::uint16_t port = argc > 2
        ? static_cast<std::uint16_t>(std::atoi(argv[2]))
        : 9000;

    nebula::net::EventLoop loop;
    nebula::rpc::RpcChannel channel(&loop, ip, port);
    nebula::example::EchoService_Stub stub(&channel);

    constexpr int kRequestCount = 3;
    auto completed = std::make_shared<std::atomic_int>(0);

    for (int i = 0; i < kRequestCount; ++i)
    {
        // response/controller 必须活到异步 done 执行。
        auto context = std::make_shared<CallContext>();
        context->request.set_text("hello NebulaRPC #" + std::to_string(i + 1));

        auto* done = new nebula::rpc::RpcClosure([context, completed, &loop]
            {
                if (context->controller.Failed())
                {
                    std::cerr << "RPC failed: " << context->controller.ErrorText() << '\n';
                }
                else
                {
                    std::cout << "response=" << context->response.text()
                              << ", server_sequence=" << context->response.server_sequence()
                              << '\n';
                }

                const int count = completed->fetch_add(1) + 1;

                if (count == kRequestCount)
                {
                    loop.Quit();
                }
            });

        stub.Echo(&context->controller,
                  &context->request,
                  &context->response,
                  done);

        std::cout << "CallMethod returned immediately for request #"
                  << i + 1 << '\n';
    }

    loop.Loop();
    return 0;
}
