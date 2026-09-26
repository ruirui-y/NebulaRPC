#include "nebula/net/buffer.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/net/tcp_server.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_codec.h"
#include "nebula/rpc/rpc_controller.h"
#include "nebula/rpc/rpc_server.h"
#include "echo.pb.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

namespace
{

// 收到就不回包：让客户端的在途调用一直挂在 pending_calls_ 里
class HangingEchoService final : public nebula::example::EchoService
{
public:
    void Echo(::google::protobuf::RpcController* controller,
              const ::nebula::example::EchoRequest* request,
              ::nebula::example::EchoResponse* response,
              ::google::protobuf::Closure* done) override
    {
        (void)controller;
        (void)request;
        (void)response;
        (void)done;
        call_count_.fetch_add(1);
    }

    [[nodiscard]] int CallCount() const
    {
        return call_count_.load();
    }

private:
    std::atomic_int call_count_{0};
};

struct CallContext
{
    nebula::example::EchoRequest request;
    nebula::example::EchoResponse response;
    nebula::rpc::RpcController controller;
    std::atomic_int done_count{0};
};

int ConnectRaw(std::uint16_t port)
{
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0)
    {
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
    {
        ::close(fd);
        return -1;
    }

    // 测试线程就是 loop 线程，非阻塞才能保证灌数据时不会把 loop 卡死
    ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
    return fd;
}

// Case 1：在途调用数闸门 —— 超限立即失败、不进队、错误文本固定
bool RunPendingLimitCase()
{
    using namespace std::chrono_literals;

    constexpr std::uint16_t kPort = 39002;
    constexpr std::size_t kMaxPending = 3;
    constexpr int kTotalCalls = 6;

    nebula::net::EventLoop loop;
    nebula::rpc::RpcServer server(&loop, "127.0.0.1", kPort);
    HangingEchoService service;
    server.RegisterService(&service);
    server.Start(0);

    nebula::rpc::RpcChannel channel(&loop, "127.0.0.1", kPort);
    channel.SetMaxPendingCalls(kMaxPending);
    nebula::example::EchoService_Stub stub(&channel);

    auto contexts = std::make_shared<std::vector<std::shared_ptr<CallContext>>>();

    // 等建连完成再发，否则请求会落进「未连接排队」而不是在途闸门
    loop.RunAfter(200ms, [&, contexts]
        {
            for (int i = 0; i < kTotalCalls; ++i)
            {
                auto context = std::make_shared<CallContext>();
                context->request.set_text("hang-" + std::to_string(i));
                contexts->push_back(context);

                auto* done = new nebula::rpc::RpcClosure([context]
                    {
                        context->done_count.fetch_add(1);
                    });

                stub.Echo(&context->controller,
                          &context->request,
                          &context->response,
                          done);
            }
        });

    bool passed = false;

    loop.RunAfter(1500ms, [&, contexts]
        {
            int rejected_done = 0;
            int rejected_failed = 0;
            int error_text_ok = 0;
            int hanging_done = 0;

            for (const auto& context : *contexts)
            {
                if (context->controller.Failed())
                {
                    ++rejected_failed;

                    if (context->controller.ErrorText() == "too many pending rpcs")
                    {
                        ++error_text_ok;
                    }
                }
                else
                {
                    hanging_done += context->done_count.load();
                }

                if (context->done_count.load() == 1)
                {
                    ++rejected_done;
                }
            }

            const int rejected = kTotalCalls - static_cast<int>(kMaxPending);

            std::cout << "---------- backpressure pending limit detail ----------\n";
            std::cout << "[gate] server_call_count=" << service.CallCount()
                << ", pending_calls=" << channel.PendingCallCount()
                << ", pending_writes=" << channel.PendingWriteCount()
                << ", overload_reject=" << channel.OverloadRejectCount()
                << "\n";
            std::cout << "[calls] total=" << kTotalCalls
                << ", rejected_failed=" << rejected_failed
                << ", error_text_ok=" << error_text_ok
                << ", rejected_done=" << rejected_done
                << ", hanging_done=" << hanging_done
                << "\n";
            std::cout << "-------------------------------------------------------\n";

            passed =
                channel.PendingCallCount() == kMaxPending &&
                channel.OverloadRejectCount() == static_cast<std::uint64_t>(rejected) &&
                rejected_failed == rejected &&
                error_text_ok == rejected &&
                rejected_done == rejected &&
                hanging_done == 0;

            loop.Quit();
        });

    loop.Loop();
    return passed;
}

// Case 2：输出水位与硬上限 —— 慢客户端不读，服务端灌满后必须被硬闸门切断
bool RunOutputWatermarkCase()
{
    using namespace std::chrono_literals;

    constexpr std::uint16_t kPort = 39003;
    constexpr std::size_t kHighWatermark = 1U * 1024U * 1024U;
    constexpr std::size_t kHardLimit = 4U * 1024U * 1024U;

    nebula::net::EventLoop loop;
    nebula::net::TcpServer server(&loop, "127.0.0.1", kPort);

    auto established = std::make_shared<std::atomic_bool>(false);
    auto watermark_hits = std::make_shared<std::atomic_int>(0);
    auto conn_watermark_count = std::make_shared<std::atomic_int>(0);
    auto overload_closes = std::make_shared<std::atomic_int>(0);
    auto peak_pending = std::make_shared<std::atomic<std::size_t>>(0);

    server.SetConnectionCallback(
        [conn_watermark_count, overload_closes, established, peak_pending]
        (const nebula::net::TcpConnectionPtr& conn)
        {
            if (!conn->Connected())
            {
                overload_closes->store(static_cast<int>(conn->OverloadCloseCount()));
                conn_watermark_count->store(static_cast<int>(conn->HighWatermarkCount()));
                return;
            }

            established->store(true);

            // 灌 32MB：socket 缓冲写满后剩余部分堆进 output_buffer
            const std::string chunk(64U * 1024U, 'x');

            for (int i = 0; i < 512; ++i)
            {
                conn->Send(chunk);

                const std::size_t pending = conn->PendingOutputBytes();

                if (pending > peak_pending->load())
                {
                    peak_pending->store(pending);
                }
            }
        });

    server.SetHighWatermarkCallback(
        [watermark_hits](const nebula::net::TcpConnectionPtr& conn, std::size_t pending_bytes)
        {
            (void)conn;
            (void)pending_bytes;
            watermark_hits->fetch_add(1);
        },
        kHighWatermark);

    server.SetMaxOutputBufferBytes(kHardLimit);
    server.Start(0);

    auto raw_fd = std::make_shared<int>(-1);

    // 慢客户端：连上就什么都不做，不读
    loop.RunAfter(200ms, [&, raw_fd]
        {
            *raw_fd = ConnectRaw(kPort);
        });

    bool passed = false;

    loop.RunAfter(2000ms,
        [&, raw_fd, established, watermark_hits, conn_watermark_count, overload_closes, peak_pending]
        {
            std::cout << "---------- backpressure output watermark detail ----------\n";
            std::cout << "[gate] established=" << established->load()
                << ", high_watermark_hits=" << watermark_hits->load()
                << ", conn_watermark_count=" << conn_watermark_count->load()
                << ", overload_closes=" << overload_closes->load()
                << ", peak_pending_bytes=" << peak_pending->load()
                << "\n";
            std::cout << "----------------------------------------------------------\n";

            passed =
                established->load() &&
                watermark_hits->load() >= 1 &&
                conn_watermark_count->load() >= 1 &&
                overload_closes->load() >= 1 &&
                peak_pending->load() > 0;

            loop.Quit();
        });

    loop.Loop();

    if (*raw_fd >= 0)
    {
        ::close(*raw_fd);
    }

    return passed;
}

// Case 3：输入水位 —— 对端灌无效流量且上层不消费，残留越线后必须断连
bool RunInputWatermarkCase()
{
    using namespace std::chrono_literals;

    constexpr std::uint16_t kPort = 39004;
    constexpr std::size_t kInputLimit = 64U * 1024U;

    nebula::net::EventLoop loop;
    nebula::net::TcpServer server(&loop, "127.0.0.1", kPort);

    auto closed = std::make_shared<std::atomic_bool>(false);
    auto overload_closes = std::make_shared<std::atomic_int>(0);

    // 故意不消费：模拟上层既认不出帧也不排空缓冲
    server.SetMessageCallback(
        [](const nebula::net::TcpConnectionPtr& conn, nebula::net::Buffer* buffer)
        {
            (void)conn;
            (void)buffer;
        });

    server.SetConnectionCallback(
        [closed, overload_closes](const nebula::net::TcpConnectionPtr& conn)
        {
            if (!conn->Connected())
            {
                closed->store(true);
                overload_closes->store(static_cast<int>(conn->OverloadCloseCount()));
            }
        });

    server.SetMaxInputBufferBytes(kInputLimit);
    server.Start(0);

    auto raw_fd = std::make_shared<int>(-1);

    loop.RunAfter(200ms, [&, raw_fd]
        {
            *raw_fd = ConnectRaw(kPort);

            if (*raw_fd < 0)
            {
                return;
            }

            // 1MB 垃圾：一次 readv 就能吃进 64KB+，足以越过水位
            const std::string junk(64U * 1024U, 'z');

            for (int i = 0; i < 16; ++i)
            {
                const ssize_t written = ::send(*raw_fd,
                                               junk.data(),
                                               junk.size(),
                                               MSG_NOSIGNAL);

                if (written <= 0)
                {
                    break;
                }
            }
        });

    bool passed = false;

    loop.RunAfter(2000ms, [&, raw_fd, closed, overload_closes]
        {
            std::cout << "---------- backpressure input watermark detail ----------\n";
            std::cout << "[gate] connection_closed=" << closed->load()
                << ", overload_closes=" << overload_closes->load()
                << "\n";
            std::cout << "---------------------------------------------------------\n";

            passed = closed->load() && overload_closes->load() >= 1;
            loop.Quit();
        });

    loop.Loop();

    if (*raw_fd >= 0)
    {
        ::close(*raw_fd);
    }

    return passed;
}

// Case 4：服务端顶到硬上限踢掉连接后，该连接上的在途调用必须被失败
bool RunForcedCloseFailsPendingCase()
{
    using namespace std::chrono_literals;

    constexpr std::uint16_t kPort = 39005;
    constexpr std::size_t kHardLimit = 4U * 1024U * 1024U;
    constexpr std::size_t kChunkBytes = 64U * 1024U;
    constexpr int kChunkCount = 512;
    constexpr std::uint64_t kFillerRequestId = 0xFFFF'FFF0ULL;

    nebula::net::EventLoop loop;
    nebula::net::TcpServer server(&loop, "127.0.0.1", kPort);

    auto overload_closes = std::make_shared<std::atomic_int>(0);
    auto server_requests = std::make_shared<std::atomic_int>(0);

    // 必须发合法帧且 request_id 不撞在途调用，否则在途调用会被提前失败/完成
    nebula::rpc::proto::RpcMeta filler_meta;
    filler_meta.set_type(nebula::rpc::proto::RpcMeta::RESPONSE);
    filler_meta.set_request_id(kFillerRequestId);
    const std::string filler_frame =
        nebula::rpc::RpcCodec::Encode(filler_meta, std::string(kChunkBytes, 'x'));

    // 吃掉请求不回包（调用留在 pending_calls_），再把连接顶到硬上限
    server.SetMessageCallback(
        [overload_closes, server_requests, filler_frame](const nebula::net::TcpConnectionPtr& conn,
                                                         nebula::net::Buffer* buffer)
        {
            buffer->RetrieveAll();
            server_requests->fetch_add(1);

            if (!conn->Connected())
            {
                return;
            }

            for (int i = 0; i < kChunkCount; ++i)
            {
                conn->Send(filler_frame);
            }

            overload_closes->store(static_cast<int>(conn->OverloadCloseCount()));
        });

    server.SetMaxOutputBufferBytes(kHardLimit);
    server.Start(0);

    nebula::rpc::RpcChannel channel(&loop, "127.0.0.1", kPort);
    nebula::example::EchoService_Stub stub(&channel);

    auto context = std::make_shared<CallContext>();

    loop.RunAfter(200ms, [&, context]
        {
            context->request.set_text("cut-me");

            auto* done = new nebula::rpc::RpcClosure([context]
                {
                    context->done_count.fetch_add(1);
                });

            stub.Echo(&context->controller,
                      &context->request,
                      &context->response,
                      done);
        });

    bool passed = false;

    loop.RunAfter(1500ms, [&, context, overload_closes, server_requests]
        {
            std::cout << "---------- backpressure forced close detail ----------\n";
            std::cout << "[gate] server_requests=" << server_requests->load()
                << ", overload_closes=" << overload_closes->load()
                << "\n";
            std::cout << "[calls] done_count=" << context->done_count.load()
                << ", failed=" << context->controller.Failed()
                << ", error_text=\"" << context->controller.ErrorText() << "\""
                << "\n";
            std::cout << "-----------------------------------------------------\n";

            passed =
                server_requests->load() >= 1 &&
                overload_closes->load() >= 1 &&
                context->done_count.load() == 1 &&
                context->controller.Failed() &&
                context->controller.ErrorText() == "RPC connection closed";

            loop.Quit();
        });

    loop.Loop();
    return passed;
}

}  // namespace

int main()
{
    if (!RunPendingLimitCase())
    {
        std::cerr << "backpressure pending limit case failed\n";
        return 1;
    }

    if (!RunOutputWatermarkCase())
    {
        std::cerr << "backpressure output watermark case failed\n";
        return 1;
    }

    if (!RunInputWatermarkCase())
    {
        std::cerr << "backpressure input watermark case failed\n";
        return 1;
    }

    if (!RunForcedCloseFailsPendingCase())
    {
        std::cerr << "backpressure forced close case failed\n";
        return 1;
    }

    std::cout << "RPC backpressure test passed\n";
    return 0;
}
