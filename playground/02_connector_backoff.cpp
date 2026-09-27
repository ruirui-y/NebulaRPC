// 探针 02：Connector 连不上时的退避重连节奏（指向死端口，观察 error_callback_ 的时间戳）

#include "nebula/net/connector.h"
#include "nebula/net/event_loop.h"

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>

int main()
{
    nebula::net::EventLoop loop;

    // 死端口：连接必然失败，退避会一直跑，正好用来看节奏
    auto connector = std::make_shared<nebula::net::Connector>(&loop, "127.0.0.1", 59999);
    const auto start = std::chrono::steady_clock::now();

    connector->SetErrorCallback([start](const std::string& reason)
        {
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start);
            std::printf("t=%5lldms  %s\n",
                        static_cast<long long>(elapsed.count()),
                        reason.c_str());
        });

    connector->Start();

    loop.RunAfter(std::chrono::milliseconds{4000}, [&loop, connector]
        {
            std::printf("total reconnect attempts=%llu\n",
                        static_cast<unsigned long long>(connector->ReconnectAttemptCount()));
            loop.Quit();
        });

    loop.Loop();
    return 0;
}
