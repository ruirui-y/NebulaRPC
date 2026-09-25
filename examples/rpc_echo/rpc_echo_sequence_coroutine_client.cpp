// 协程版：顺序发 3 个请求，成功就打印响应，失败就报错中止。
// 对照文件：rpc_echo_sequence_client.cpp（同一个任务，回调写法）
//
// 同样的任务，这个版本里没有 SequenceState，也没有 SendNext。
// 循环变量 i、请求、响应都是普通局部变量，它们住在堆上的协程帧里，活过了每一次挂起。

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

constexpr int kRequestCount = 3;

nebula::rpc::Task<int> RunSequence(nebula::rpc::RpcChannel& channel,
                                   nebula::net::EventLoop& loop)
{
    const auto* echo =
        nebula::rpc::FindMethod(nebula::example::EchoService::descriptor(), "Echo");

    int exit_code = 0;

    for (int i = 0; i < kRequestCount; ++i)   // 就是普通的 for
    {
        nebula::example::EchoRequest request;
        request.set_text("hello #" + std::to_string(i + 1));

        try
        {
            nebula::example::EchoResponse response =
                co_await nebula::rpc::RpcAwaiter<
                    nebula::example::EchoRequest, nebula::example::EchoResponse>(
                    &channel, echo, request, std::chrono::milliseconds(2000));

            std::cout << "response=" << response.text() << '\n';   // i 直接可用
        }
        catch (const nebula::rpc::RpcError& error)
        {
            std::cerr << "RPC failed: " << error.what() << '\n';
            exit_code = 1;
            break;   // 中止就是 break
        }
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

    nebula::rpc::Task<int> task = RunSequence(channel, loop);
    task.Start();

    loop.Loop();

    if (!task.Done())
    {
        std::cerr << "task did not finish\n";
        return 1;
    }

    try
    {
        return task.Result();
    }
    catch (const std::exception& error)
    {
        std::cerr << "task threw: " << error.what() << '\n';
        return 1;
    }
}
