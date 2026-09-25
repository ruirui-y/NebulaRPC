#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_awaiter.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_error.h"
#include "nebula/rpc/rpc_task.h"
#include "echo.pb.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>

namespace
{

using nebula::example::EchoRequest;
using nebula::example::EchoResponse;

// 协程体：写起来像同步代码，底下仍是「发出去 → 挂起 → 完成路径恢复」
nebula::rpc::Task<int> RunEcho(nebula::rpc::RpcChannel& channel,
                               nebula::net::EventLoop& loop)
{
    const google::protobuf::MethodDescriptor* echo =
        nebula::rpc::FindMethod(nebula::example::EchoService::descriptor(), "Echo");

    EchoRequest request;
    request.set_text("hello coroutine");

    int exit_code = 0;

    try
    {
        EchoResponse response = co_await nebula::rpc::RpcAwaiter<EchoRequest, EchoResponse>(
            &channel, echo, request, std::chrono::milliseconds(2000));

        std::cout << "[coroutine] response=" << response.text()
                  << ", server_sequence=" << response.server_sequence()
                  << '\n';
    }
    catch (const nebula::rpc::RpcError& error)
    {
        std::cerr << "[coroutine] RPC failed: state="
                  << static_cast<int>(error.State())
                  << ", reason=" << error.what() << '\n';
        exit_code = 1;
    }

    loop.Quit();
    co_return exit_code;
}

}  // namespace

int main(int argc, char** argv)
{
    const std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    const std::uint16_t port = argc > 2
        ? static_cast<std::uint16_t>(std::atoi(argv[2]))
        : 9000;

    nebula::net::EventLoop loop;
    nebula::rpc::RpcChannel channel(&loop, ip, port);

    nebula::rpc::Task<int> task = RunEcho(channel, loop);
    task.Start();

    loop.Loop();

    if (!task.Done())
    {
        std::cerr << "[coroutine] task did not finish\n";
        return 1;
    }

    try
    {
        return task.Result();
    }
    catch (const std::exception& error)
    {
        std::cerr << "[coroutine] task threw: " << error.what() << '\n';
        return 1;
    }
}
