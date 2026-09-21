#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/net/tcp_server.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>

int main(int argc, char** argv) {
    const std::uint16_t port = argc > 1 ? static_cast<std::uint16_t>(std::atoi(argv[1])) : 9001;

    nebula::net::EventLoop loop;
    nebula::net::TcpServer server(&loop, "0.0.0.0", port);

    server.SetConnectionCallback([](const nebula::net::TcpConnectionPtr& conn) {
        std::cout << (conn->Connected() ? "connected: " : "disconnected: ")
                  << conn->Name() << '\n';
    });

    server.SetMessageCallback([](const nebula::net::TcpConnectionPtr& conn,
                                 nebula::net::Buffer* buffer) {
        const std::string message = buffer->RetrieveAllAsString();
        conn->Send(message);
    });

    server.Start(0);
    std::cout << "NebulaRPC echo server listening on 0.0.0.0:" << port << '\n';
    loop.Loop();
    return 0;
}
