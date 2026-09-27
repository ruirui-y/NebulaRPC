// 探针 03：已建连接掉线后客户端能否自己爬起来（起 server -> 连上 -> 停 server -> 重启 server）

#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_client.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/net/tcp_server.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>

namespace
{

constexpr std::uint16_t kPort = 59998;
const std::chrono::steady_clock::time_point kStart = std::chrono::steady_clock::now();

long long Since()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - kStart)
        .count();
}

}  // namespace

int main()
{
    nebula::net::EventLoop loop;

    auto server = std::make_unique<nebula::net::TcpServer>(&loop, "127.0.0.1", kPort);
    server->Start();

    nebula::net::TcpClient client(&loop, "127.0.0.1", kPort);

    client.SetConnectionCallback([](const nebula::net::TcpConnectionPtr& conn)
        {
            std::printf("t=%5lldms  client %s\n",
                        Since(),
                        conn->Connected() ? "CONNECTED" : "DISCONNECTED");
        });

    // 重连失败的次数和节奏都从这里看
    client.SetConnectErrorCallback([](const std::string& reason)
        {
            std::printf("t=%5lldms  %s\n", Since(), reason.c_str());
        });

    client.Connect();

    loop.RunAfter(std::chrono::milliseconds{400}, [&server]
        {
            server.reset();
        });

    loop.RunAfter(std::chrono::milliseconds{1200}, [&loop, &server]
        {
            server = std::make_unique<nebula::net::TcpServer>(&loop, "127.0.0.1", kPort);
            server->Start();
        });

    loop.RunAfter(std::chrono::milliseconds{3000}, [&loop]
        {
            loop.Quit();
        });

    loop.Loop();
    return 0;
}
