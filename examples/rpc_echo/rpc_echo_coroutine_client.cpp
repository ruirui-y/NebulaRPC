#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_coroutine.h"
#include "echo.pb.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <utility>

namespace
{
    using namespace std::chrono_literals;
    using EchoRequest = nebula::example::EchoRequest;
    using EchoResponse = nebula::example::EchoResponse;
    using EchoCoroutineStub = nebula::rpc::CoroutineStub<nebula::example::EchoService>;

    nebula::coro::Task<int> RunEcho(
        EchoCoroutineStub& stub,
        nebula::rpc::RpcChannel& channel,
        nebula::net::EventLoop& loop)
    {
        const auto connect_deadline = std::chrono::steady_clock::now() + 2s;

        // RpcChannel 建连本身是异步的，先用协程等待连接建立。
        while (!channel.Connected())
        {
            if (std::chrono::steady_clock::now() >= connect_deadline)
            {
                std::cerr << "[coroutine] connect timeout\n";
                loop.Quit();
                co_return 1;
            }

            co_await nebula::net::SleepFor(&loop, 10ms);
        }

        const google::protobuf::MethodDescriptor* echo_method = stub.ResolveMethod("Echo");

        EchoRequest request;
        request.set_text("hello coroutine");

        nebula::rpc::RpcCallOptions options;
        options.timeout = 2s;

        auto result = co_await stub.Call<EchoRequest, EchoResponse>(echo_method, std::move(request), options);

        if (!result)
        {
            std::cerr << "[coroutine] RPC failed: state=" << static_cast<int>(result.State())
                << ", reason=" << result.ErrorText() << '\n';

            loop.Quit();
            co_return 1;
        }

        const EchoResponse& response = result.ResponseValue();

        std::cout << "[coroutine] response=" << response.text()
            << ", server_sequence=" << response.server_sequence()
            << ", request_id=" << result.RequestId()
            << ", trace_id=" << result.TraceId()
            << ", latency_us=" << result.Latency().count()
            << '\n';

        loop.Quit();
        co_return 0;
    }

}  // namespace

int main(int argc, char** argv)
{
    const std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    const std::uint16_t port = argc > 2 ? static_cast<std::uint16_t>(std::atoi(argv[2])) : 9000;

    nebula::net::EventLoop loop;
    nebula::rpc::RpcChannel channel(&loop, ip, port);
    EchoCoroutineStub stub(&channel);

    nebula::coro::Task<int> task = RunEcho(stub, channel, loop);
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