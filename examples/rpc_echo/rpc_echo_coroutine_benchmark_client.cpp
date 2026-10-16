// NebulaRPC 协程压测客户端
// 每个 Worker：一个线程、一个 EventLoop、一个 RpcChannel。
// 每个 Worker 在同一条连接上运行 window 个协程，请求完成后协程继续发送下一次 RPC。

#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_awaiter.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"
#include "nebula/rpc/rpc_error.h"
#include "nebula/rpc/rpc_task.h"
#include "echo.pb.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{
using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

struct Options
{
    std::string host = "127.0.0.1";
    std::uint16_t port = 9000;
    std::size_t workers = 4;             // OS线程 / EventLoop / TCP连接数
    std::size_t window = 16;             // 每条连接上的协程数 / 最大在途 RPC
    std::uint64_t requests = 100000;     // 正式测试总请求数
    std::size_t payload_bytes = 128;     // EchoRequest.text 字节数
    std::size_t warmup_requests = 100;   // 每个 Worker 的预热次数
    int timeout_ms = 2000;               // 单次 RPC 超时
    int connect_timeout_ms = 10000;      // 连接探测超时
    int max_seconds = 180;               // 正式测试时间上限
    std::string csv;                     // 结果 CSV 路径
};

[[noreturn]] void Usage(const char* exe, const std::string& error = {})
{
    if (!error.empty())
    {
        std::cerr << "Invalid option: " << error << '\n';
    }

    std::cerr << "Usage: " << exe
        << " [--host IP] [--port N] [--workers N] [--window N]"
        << " [--requests N] [--payload-bytes N] [--warmup-requests N]"
        << " [--timeout-ms N] [--connect-timeout-ms N] [--max-seconds N]"
        << " [--csv output.csv]\n"
        << "  Recommended: --workers 4 --window 16 --requests 600000 --payload-bytes 128\n"
        << "  workers = threads = EventLoops = TCP connections\n"
        << "  window  = coroutines per connection = max inflight RPCs per connection\n";

    std::exit(error.empty() ? 0 : 2);
}

std::uint64_t ParseUnsigned(const std::string& s)
{
    if (s.empty() || s[0] == '-')
    {
        throw std::invalid_argument(s);
    }

    std::size_t used = 0;
    const auto value = std::stoull(s, &used, 10);

    if (used != s.size())
    {
        throw std::invalid_argument(s);
    }

    return value;
}

Options ParseOptions(int argc, char** argv)
{
    Options o;

    for (int i = 1; i < argc; ++i)
    {
        const std::string name(argv[i]);

        if (name == "--help" || name == "-h")
        {
            Usage(argv[0]);
        }

        if (i + 1 >= argc)
        {
            Usage(argv[0], name + " has no value");
        }

        const std::string value(argv[++i]);

        try
        {
            if (name == "--host")
            {
                o.host = value;
            }
            else if (name == "--port")
            {
                const auto n = ParseUnsigned(value);

                if (n > 65535)
                {
                    Usage(argv[0], "port exceeds 65535");
                }

                o.port = static_cast<std::uint16_t>(n);
            }
            else if (name == "--workers")
            {
                o.workers = ParseUnsigned(value);
            }
            else if (name == "--window")
            {
                o.window = ParseUnsigned(value);
            }
            else if (name == "--requests")
            {
                o.requests = ParseUnsigned(value);
            }
            else if (name == "--payload-bytes")
            {
                o.payload_bytes = ParseUnsigned(value);
            }
            else if (name == "--warmup-requests")
            {
                o.warmup_requests = ParseUnsigned(value);
            }
            else if (name == "--timeout-ms")
            {
                const auto n = ParseUnsigned(value);

                if (n > 120000)
                {
                    Usage(argv[0], "timeout-ms too large");
                }

                o.timeout_ms = static_cast<int>(n);
            }
            else if (name == "--connect-timeout-ms")
            {
                const auto n = ParseUnsigned(value);

                if (n > 120000)
                {
                    Usage(argv[0], "connect-timeout-ms too large");
                }

                o.connect_timeout_ms = static_cast<int>(n);
            }
            else if (name == "--max-seconds")
            {
                const auto n = ParseUnsigned(value);

                if (n > 86400)
                {
                    Usage(argv[0], "max-seconds too large");
                }

                o.max_seconds = static_cast<int>(n);
            }
            else if (name == "--csv")
            {
                o.csv = value;
            }
            else
            {
                Usage(argv[0], name);
            }
        }
        catch (const std::exception&)
        {
            Usage(argv[0], name + "=" + value);
        }
    }

    if (o.host.empty() || o.port == 0 || o.workers == 0 || o.workers > 128 ||
        o.window == 0 || o.window > 10000 || o.requests < o.workers ||
        o.requests > 100000000 || o.payload_bytes > 65536 ||
        o.warmup_requests > 1000000 || o.timeout_ms < 1 || o.timeout_ms > 120000 ||
        o.connect_timeout_ms < 1 || o.connect_timeout_ms > 120000 ||
        o.max_seconds < 1 || o.max_seconds > 86400)
    {
        Usage(argv[0], "configuration outside supported range");
    }

    return o;
}

struct SharedStart
{
    std::atomic<std::size_t> ready{0};
    std::atomic<bool> go{false};
};

struct Stats
{
    std::uint64_t issued = 0;
    std::uint64_t completed = 0;
    std::uint64_t succeeded = 0;
    std::uint64_t failed = 0;
    std::uint64_t timed_out = 0;
    std::uint64_t mismatched = 0;
    std::uint64_t unfinished = 0;
    std::size_t worker_id = 0;
    TimePoint started{};
    TimePoint stopped{};
    std::vector<std::int64_t> latency_ns;
    std::string error;
};

struct CallContext
{
    nebula::example::EchoRequest request;
    nebula::example::EchoResponse response;
    nebula::rpc::RpcController controller;
};

const google::protobuf::MethodDescriptor* EchoMethod()
{
    return nebula::rpc::FindMethod(
        nebula::example::EchoService::descriptor(),
        "Echo");
}

class Worker
{
public:
    Worker(const Options& opts,
           SharedStart& shared,
           std::size_t worker_id,
           std::uint64_t request_count)
        : opts_(opts),
          shared_(shared),
          target_(request_count),
          channel_(&loop_, opts.host, opts.port),
          stub_(&channel_),
          echo_method_(EchoMethod()),
          payload_(opts.payload_bytes, 'x')
    {
        if (echo_method_ == nullptr)
        {
            throw std::runtime_error("Echo method descriptor not found");
        }

        stats_.worker_id = worker_id;
        stats_.latency_ns.reserve(static_cast<std::size_t>(target_));
    }

    Stats Run()
    {
        connection_deadline_ = Clock::now() +
            std::chrono::milliseconds(opts_.connect_timeout_ms);

        loop_.RunAfter(std::chrono::milliseconds(100), [this]
            {
                Probe();
            });

        loop_.Loop();

        CollectTaskErrors();
        stats_.unfinished = stats_.issued - stats_.completed;
        return std::move(stats_);
    }

private:
    void Ready()
    {
        if (announced_ready_)
        {
            return;
        }

        announced_ready_ = true;
        shared_.ready.fetch_add(1, std::memory_order_release);
    }

    void Abort(const std::string& reason)
    {
        if (stats_.error.empty())
        {
            stats_.error = reason;
        }

        Ready();

        if (started_ && stats_.stopped == TimePoint{})
        {
            stats_.stopped = Clock::now();
        }

        loop_.Quit();
    }

    // 建连探测仍沿用回调版，避免把连接建立时间计入正式测量。
    void Probe()
    {
        if (Clock::now() > connection_deadline_)
        {
            Abort("RPC connection/probe failed before deadline");
            return;
        }

        auto c = std::make_shared<CallContext>();
        c->request.set_text("nebula-coroutine-benchmark-probe");
        c->controller.SetTimeout(std::chrono::milliseconds(500));

        auto* done = new nebula::rpc::RpcClosure([this, c]
            {
                loop_.QueueInLoop([this, c]
                    {
                        if (!c->controller.Failed() &&
                            c->response.text() == c->request.text())
                        {
                            Warmup();
                        }
                        else
                        {
                            loop_.RunAfter(std::chrono::milliseconds(100), [this]
                                {
                                    Probe();
                                });
                        }
                    });
            });

        stub_.Echo(&c->controller, &c->request, &c->response, done);
    }

    // 预热不计入正式统计，完成后等待所有 Worker 统一起跑。
    void Warmup()
    {
        if (warmed_ >= opts_.warmup_requests)
        {
            Ready();
            WaitForStart();
            return;
        }

        auto c = std::make_shared<CallContext>();
        c->request.set_text(payload_);
        c->controller.SetTimeout(std::chrono::milliseconds(opts_.timeout_ms));

        auto* done = new nebula::rpc::RpcClosure([this, c]
            {
                loop_.QueueInLoop([this, c]
                    {
                        if (c->controller.Failed() ||
                            c->response.text() != c->request.text())
                        {
                            Abort("RPC warmup failed: " + c->controller.ErrorText());
                            return;
                        }

                        ++warmed_;
                        Warmup();
                    });
            });

        stub_.Echo(&c->controller, &c->request, &c->response, done);
    }

    void WaitForStart()
    {
        if (!shared_.go.load(std::memory_order_acquire))
        {
            loop_.RunAfter(std::chrono::milliseconds(1), [this]
                {
                    WaitForStart();
                });

            return;
        }

        started_ = true;
        stats_.started = Clock::now();

        loop_.RunAfter(std::chrono::seconds(opts_.max_seconds), [this]
            {
                Abort("measured phase exceeded --max-seconds");
            });

        StartCoroutines();
    }

    // 一个协程代表一条顺序 RPC 链：一次只等待自己的一个 RPC。
    // 多个协程共享同一个 RpcChannel，因此同一 TCP 连接上会同时存在多个 request_id。
    nebula::rpc::Task<int> RunCoroutine(std::uint64_t request_count)
        {
            for (std::uint64_t i = 0; i < request_count; ++i)
            {
                if (!stats_.error.empty())
                {
                    OnCoroutineFinished();
                    co_return 1;
                }

                nebula::example::EchoRequest request;
                request.set_text(payload_);

                const auto sent_at = Clock::now();
                ++stats_.issued;
                ++in_flight_;

                try
                {
                    nebula::example::EchoResponse response =
                        co_await nebula::rpc::RpcAwaiter<
                            nebula::example::EchoRequest,
                            nebula::example::EchoResponse>(
                                &channel_,
                                echo_method_,
                                std::move(request),
                                std::chrono::milliseconds(opts_.timeout_ms));

                    const auto ended = Clock::now();
                    --in_flight_;
                    ++stats_.completed;

                    if (response.text() != payload_)
                    {
                        ++stats_.mismatched;
                    }
                    else
                    {
                        ++stats_.succeeded;
                        stats_.latency_ns.push_back(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                ended - sent_at).count());
                    }
                }
                catch (const nebula::rpc::RpcError& error)
                {
                    --in_flight_;
                    ++stats_.completed;
                    ++stats_.failed;

                    if (error.State() == nebula::rpc::RpcCallState::Timeout)
                    {
                        ++stats_.timed_out;
                    }
                }
                catch (const std::exception& error)
                {
                    --in_flight_;
                    ++stats_.completed;
                    ++stats_.failed;

                    if (stats_.error.empty())
                    {
                        stats_.error = error.what();
                    }

                    OnCoroutineFinished();
                    co_return 1;
                }
            }

            OnCoroutineFinished();
            co_return 0;
        }

    void StartCoroutines()
    {
        const std::size_t coroutine_count = std::min<std::uint64_t>(
            static_cast<std::uint64_t>(opts_.window),
            target_);

        tasks_.reserve(coroutine_count);
        active_coroutines_ = coroutine_count;

        for (std::size_t i = 0; i < coroutine_count; ++i)
        {
            const std::uint64_t count = target_ / coroutine_count +
                (i < target_ % coroutine_count ? 1 : 0);

            tasks_.emplace_back(RunCoroutine(count));
        }

        // 先创建完全部 Task，再逐个 Start。
        // 每个 Task 会运行到第一个 co_await 后挂起，最终形成 window 个同时在途 RPC。
        for (auto& task : tasks_)
        {
            task.Start();
        }
    }

    void OnCoroutineFinished()
    {
        if (active_coroutines_ == 0)
        {
            return;
        }

        --active_coroutines_;

        if (active_coroutines_ == 0)
        {
            stats_.stopped = Clock::now();
            loop_.Quit();
        }
    }

    void CollectTaskErrors()
    {
        for (auto& task : tasks_)
        {
            if (!task.Done())
            {
                continue;
            }

            try
            {
                const int result = task.Result();

                if (result != 0 && stats_.error.empty())
                {
                    stats_.error = "coroutine returned failure";
                }
            }
            catch (const std::exception& error)
            {
                if (stats_.error.empty())
                {
                    stats_.error = error.what();
                }
            }
        }
    }

    const Options& opts_;
    SharedStart& shared_;
    std::uint64_t target_;
    nebula::net::EventLoop loop_;
    nebula::rpc::RpcChannel channel_;
    nebula::example::EchoService_Stub stub_;
    const google::protobuf::MethodDescriptor* echo_method_{};
    std::string payload_;
    TimePoint connection_deadline_{};
    std::size_t warmed_ = 0;
    std::size_t in_flight_ = 0;
    std::size_t active_coroutines_ = 0;
    bool announced_ready_ = false;
    bool started_ = false;
    Stats stats_;

    // 必须比 channel_ 后声明：析构时 Task 先销毁，RpcAwaiter 可安全取消仍在途的调用。
    std::vector<nebula::rpc::Task<int>> tasks_;
};

std::int64_t Percentile(const std::vector<std::int64_t>& sorted, double percent)
{
    if (sorted.empty())
    {
        return 0;
    }

    const std::size_t index = std::min(
        sorted.size() - 1,
        static_cast<std::size_t>(
            std::ceil(percent * static_cast<double>(sorted.size()))) - 1);

    return sorted[index];
}

int Report(const Options& o, std::vector<Stats>& results)
{
    std::vector<std::int64_t> latencies;
    std::uint64_t issued = 0;
    std::uint64_t completed = 0;
    std::uint64_t success = 0;
    std::uint64_t failed = 0;
    std::uint64_t timeout = 0;
    std::uint64_t mismatch = 0;
    std::uint64_t unfinished = 0;
    TimePoint earliest{};
    TimePoint latest{};
    bool bad = false;

    for (const auto& s : results)
    {
        issued += s.issued;
        completed += s.completed;
        success += s.succeeded;
        failed += s.failed;
        timeout += s.timed_out;
        mismatch += s.mismatched;
        unfinished += s.unfinished;

        if (!s.error.empty())
        {
            std::cerr << "worker " << s.worker_id << ": " << s.error << '\n';
            bad = true;
        }

        if (s.started != TimePoint{} &&
            (earliest == TimePoint{} || s.started < earliest))
        {
            earliest = s.started;
        }

        if (s.stopped != TimePoint{} &&
            (latest == TimePoint{} || s.stopped > latest))
        {
            latest = s.stopped;
        }

        latencies.insert(
            latencies.end(),
            s.latency_ns.begin(),
            s.latency_ns.end());
    }

    std::sort(latencies.begin(), latencies.end());

    const double seconds = earliest != TimePoint{} && latest > earliest
        ? std::chrono::duration<double>(latest - earliest).count()
        : 0.0;

    const double qps = seconds > 0
        ? static_cast<double>(success) / seconds
        : 0.0;

    const double mean_us = success > 0
        ? (static_cast<double>(
            std::accumulate(latencies.begin(), latencies.end(), 0.0)) /
            static_cast<double>(success) / 1000.0)
        : 0.0;

    const auto us = [](std::int64_t ns)
        {
            return static_cast<double>(ns) / 1000.0;
        };

    const double p50 = us(Percentile(latencies, 0.50));
    const double p95 = us(Percentile(latencies, 0.95));
    const double p99 = us(Percentile(latencies, 0.99));
    const double p999 = us(Percentile(latencies, 0.999));

    std::cout << std::fixed << std::setprecision(3)
        << "mode=coroutine_multiplexed"
        << " host=" << o.host << ':' << o.port
        << " workers=" << o.workers
        << " connections=" << o.workers
        << " coroutines_per_connection=" << o.window
        << " total_coroutines=" << (o.workers * o.window)
        << " payload_text_bytes=" << o.payload_bytes << '\n'
        << "target=" << o.requests
        << " issued=" << issued
        << " completed=" << completed
        << " success=" << success
        << " failed=" << failed
        << " timeout=" << timeout
        << " mismatched=" << mismatch
        << " unfinished=" << unfinished << '\n'
        << "wall_seconds=" << seconds
        << " success_qps=" << qps << '\n'
        << "latency_success_only_us: avg=" << mean_us
        << " p50=" << p50
        << " p95=" << p95
        << " p99=" << p99
        << " p999=" << p999 << '\n';

    // 沿用旧 benchmark CSV 列结构：window 在本版本中表示每连接协程数。
    if (!o.csv.empty())
    {
        std::ifstream old(o.csv, std::ios::binary | std::ios::ate);
        const bool header = !old.good() || old.tellg() == std::streampos(0);
        std::ofstream out(o.csv, std::ios::app);

        if (!out)
        {
            throw std::runtime_error("unable to open csv: " + o.csv);
        }

        if (header)
        {
            out << "framework,host,port,workers,connections,window,payload_text_bytes,"
                << "target,issued,completed,success,failed,timeout,mismatch,unfinished,"
                << "wall_seconds,success_qps,avg_us,p50_us,p95_us,p99_us,p999_us\n";
        }

        out << "NebulaRPC-Coroutine," << o.host << ',' << o.port << ','
            << o.workers << ',' << o.workers << ',' << o.window << ','
            << o.payload_bytes << ',' << o.requests << ',' << issued << ','
            << completed << ',' << success << ',' << failed << ',' << timeout << ','
            << mismatch << ',' << unfinished << ',' << seconds << ',' << qps << ','
            << mean_us << ',' << p50 << ',' << p95 << ',' << p99 << ',' << p999 << '\n';
    }

    if (bad || issued != o.requests || completed != issued ||
        failed || mismatch || unfinished)
    {
        return 1;
    }

    return 0;
}
}

int main(int argc, char** argv)
{
    try
    {
        const Options o = ParseOptions(argc, argv);

        spdlog::set_level(spdlog::level::err);

        SharedStart shared;
        std::vector<Stats> results(o.workers);
        std::vector<std::thread> threads;
        threads.reserve(o.workers);

        for (std::size_t i = 0; i < o.workers; ++i)
        {
            const std::uint64_t count = o.requests / o.workers +
                (i < o.requests % o.workers ? 1 : 0);

            threads.emplace_back([&, i, count]
                {
                    try
                    {
                        Worker worker(o, shared, i, count);
                        results[i] = worker.Run();
                    }
                    catch (const std::exception& error)
                    {
                        results[i].worker_id = i;
                        results[i].error = error.what();
                        shared.ready.fetch_add(1, std::memory_order_release);
                    }
                });
        }

        const auto deadline = Clock::now() +
            std::chrono::milliseconds(
                o.connect_timeout_ms +
                static_cast<int>(o.warmup_requests * 10 + 10000));

        while (shared.ready.load(std::memory_order_acquire) < o.workers &&
               Clock::now() < deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }

        shared.go.store(true, std::memory_order_release);

        for (auto& thread : threads)
        {
            thread.join();
        }

        return Report(o, results);
    }
    catch (const std::exception& error)
    {
        std::cerr << "benchmark error: " << error.what() << '\n';
        return 2;
    }
}
