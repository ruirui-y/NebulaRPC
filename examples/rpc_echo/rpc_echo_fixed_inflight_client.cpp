// NebulaRPC Fixed-InFlight 协程压测客户端。
//
// 测试模型：
//   workers = OS Thread = EventLoop = RpcChannel = TCP Connection
//   window  = 每条 TCP 连接上同时维持的最大 in-flight RPC 数
//   total_inflight = workers * window
//
// 每个 Lane 是一条串行 RPC 链；同一 Worker 上的 window 个 Lane 共享一条 RpcChannel。
// 初始阶段先让全部 Lane 运行到第一个 co_await，把 window 个请求写入 output_buffer，
// 然后统一 Flush。后续响应到达时，RpcChannel 会先恢复本批协程，再统一 Flush 下一批请求。

#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_coroutine.h"
#include "echo.pb.h"

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
    using EchoRequest = nebula::example::EchoRequest;
    using EchoResponse = nebula::example::EchoResponse;
    using EchoCoroutineStub = nebula::rpc::CoroutineStub<nebula::example::EchoService>;

    struct Options
    {
        std::string host = "127.0.0.1";
        std::uint16_t port = 9000;

        std::size_t workers = 4;
        std::size_t window = 16;

        std::uint64_t requests = 1000000;
        std::size_t payload_bytes = 128;
        std::size_t warmup_requests = 100;

        int timeout_ms = 2000;
        int connect_timeout_ms = 10000;
        int max_seconds = 120;

        std::string csv;
    };

    [[noreturn]] void Usage(const char* exe, const std::string& error = {})
    {
        if (!error.empty())
        {
            std::cerr << "Invalid option: " << error << '\n';
        }

        std::cerr
            << "Usage: " << exe
            << " [--host IP] [--port N]"
            << " [--workers N] [--window N]"
            << " [--requests N] [--payload-bytes N]"
            << " [--warmup-requests N] [--timeout-ms N]"
            << " [--connect-timeout-ms N] [--max-seconds N]"
            << " [--csv output.csv]\n\n"
            << "Fixed-InFlight model:\n"
            << "  workers        = OS threads = EventLoops = RpcChannels = TCP connections\n"
            << "  window         = max in-flight RPCs per TCP connection\n"
            << "  total_inflight = workers * window\n\n"
            << "Recommended baseline:\n"
            << "  --workers 4 --window 16 --requests 1000000 --payload-bytes 128\n";

        std::exit(error.empty() ? 0 : 2);
    }

    std::uint64_t ParseUnsigned(const std::string& text)
    {
        if (text.empty() || text.front() == '-')
        {
            throw std::invalid_argument(text);
        }

        std::size_t used = 0;
        const std::uint64_t value = std::stoull(text, &used, 10);

        if (used != text.size())
        {
            throw std::invalid_argument(text);
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
                    if (n == 0 || n > 65535)
                    {
                        Usage(argv[0], "port must be in [1, 65535]");
                    }
                    o.port = static_cast<std::uint16_t>(n);
                }
                else if (name == "--workers")
                {
                    o.workers = static_cast<std::size_t>(ParseUnsigned(value));
                }
                else if (name == "--window")
                {
                    o.window = static_cast<std::size_t>(ParseUnsigned(value));
                }
                else if (name == "--requests")
                {
                    o.requests = ParseUnsigned(value);
                }
                else if (name == "--payload-bytes")
                {
                    o.payload_bytes = static_cast<std::size_t>(ParseUnsigned(value));
                }
                else if (name == "--warmup-requests")
                {
                    o.warmup_requests = static_cast<std::size_t>(ParseUnsigned(value));
                }
                else if (name == "--timeout-ms")
                {
                    o.timeout_ms = static_cast<int>(ParseUnsigned(value));
                }
                else if (name == "--connect-timeout-ms")
                {
                    o.connect_timeout_ms = static_cast<int>(ParseUnsigned(value));
                }
                else if (name == "--max-seconds")
                {
                    o.max_seconds = static_cast<int>(ParseUnsigned(value));
                }
                else if (name == "--csv")
                {
                    o.csv = value;
                }
                else
                {
                    Usage(argv[0], "unknown option: " + name);
                }
            }
            catch (const std::exception&)
            {
                Usage(argv[0], name + "=" + value);
            }
        }

        if (o.host.empty() ||
            o.workers == 0 || o.workers > 128 ||
            o.window == 0 || o.window > 65536 ||
            o.requests < o.workers || o.requests > 100000000ULL ||
            o.payload_bytes > 1024U * 1024U ||
            o.warmup_requests > 1000000U ||
            o.timeout_ms < 1 || o.timeout_ms > 120000 ||
            o.connect_timeout_ms < 1 || o.connect_timeout_ms > 120000 ||
            o.max_seconds < 1 || o.max_seconds > 86400)
        {
            Usage(argv[0], "configuration outside supported range");
        }

        return o;
    }

    struct SharedStart
    {
        std::atomic<std::size_t> ready{ 0 };
        std::atomic<bool> go{ false };
        std::atomic<bool> cancel{ false };
    };

    struct Stats
    {
        std::size_t worker_id = 0;

        std::uint64_t issued = 0;
        std::uint64_t completed = 0;
        std::uint64_t succeeded = 0;
        std::uint64_t failed = 0;
        std::uint64_t timed_out = 0;
        std::uint64_t cancelled = 0;
        std::uint64_t mismatched = 0;
        std::uint64_t unfinished = 0;

        TimePoint started{};
        TimePoint stopped{};

        std::vector<std::int64_t> latency_ns;
        std::string error;
    };

    class Worker
    {
    public:
        Worker(const Options& opts, SharedStart& shared, std::size_t worker_id, std::uint64_t request_count)
            : opts_(opts),
            shared_(shared),
            target_(request_count),
            channel_(&loop_, opts.host, opts.port),
            stub_(&channel_),
            echo_method_(stub_.ResolveMethod("Echo")),
            payload_(opts.payload_bytes, 'x')
        {
            stats_.worker_id = worker_id;
            stats_.latency_ns.reserve(static_cast<std::size_t>(target_));
        }

        Stats Run()
        {
            setup_task_ = Setup();
            setup_task_.Start();

            loop_.Loop();

            CollectTaskErrors();
            stats_.unfinished = stats_.issued - stats_.completed;
            return std::move(stats_);
        }

    private:
        void AnnounceReady()
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

            AnnounceReady();

            if (started_ && stats_.stopped == TimePoint{})
            {
                stats_.stopped = Clock::now();
            }

            loop_.Quit();
        }

        nebula::coro::Task<int> Setup()
        {
            const TimePoint connect_deadline = Clock::now() +
                std::chrono::milliseconds(opts_.connect_timeout_ms);

            while (!channel_.Connected())
            {
                if (shared_.cancel.load(std::memory_order_acquire))
                {
                    Abort("benchmark start cancelled while connecting");
                    co_return 1;
                }

                if (Clock::now() >= connect_deadline)
                {
                    Abort("RPC connection timeout");
                    co_return 1;
                }

                co_await nebula::net::SleepFor(&loop_, std::chrono::milliseconds(10));
            }

            // 预热使用普通单次 co_await：RpcCallOptions::flush 保持默认 true。
            // 预热不进入正式统计。
            for (std::size_t i = 0; i < opts_.warmup_requests; ++i)
            {
                if (shared_.cancel.load(std::memory_order_acquire))
                {
                    Abort("benchmark start cancelled during warmup");
                    co_return 1;
                }

                EchoRequest request;
                request.set_text(payload_);

                nebula::rpc::RpcCallOptions options;
                options.timeout = std::chrono::milliseconds(opts_.timeout_ms);

                auto result = co_await stub_.Call<EchoRequest, EchoResponse>(echo_method_, std::move(request), options);

                if (!result)
                {
                    Abort("RPC warmup failed: " + result.ErrorText());
                    co_return 1;
                }

                if (result.ResponseValue().text() != payload_)
                {
                    Abort("RPC warmup response mismatch");
                    co_return 1;
                }
            }

            AnnounceReady();

            while (!shared_.go.load(std::memory_order_acquire))
            {
                if (shared_.cancel.load(std::memory_order_acquire))
                {
                    Abort("benchmark start cancelled before measured phase");
                    co_return 1;
                }

                co_await nebula::net::SleepFor(&loop_, std::chrono::milliseconds(1));
            }

            if (shared_.cancel.load(std::memory_order_acquire))
            {
                Abort("benchmark start cancelled");
                co_return 1;
            }

            started_ = true;
            stats_.started = Clock::now();

            loop_.RunAfter(std::chrono::seconds(opts_.max_seconds), [this]
                {
                    if (active_lanes_ != 0)
                    {
                        Abort("measured phase exceeded --max-seconds");
                    }
                });

            StartLanes();
            co_return 0;
        }

        // 一个 Lane 始终最多只有 1 个 RPC 在途。
        // window 个 Lane 共享同一 RpcChannel，因此一条 TCP 上最多同时存在 window 个 request_id。
        nebula::coro::Task<int> RunLane()
        {
            while (stats_.error.empty())
            {
                if (next_request_ >= target_)
                {
                    break;
                }

                ++next_request_;

                EchoRequest request;
                request.set_text(payload_);

                nebula::rpc::RpcCallOptions options;
                options.timeout = std::chrono::milliseconds(opts_.timeout_ms);
                options.flush = false;

                const TimePoint sent_at = Clock::now();
                ++stats_.issued;
                ++in_flight_;

                auto result = co_await stub_.Call<EchoRequest, EchoResponse>(echo_method_, std::move(request), options);

                const TimePoint ended_at = Clock::now();

                --in_flight_;
                ++stats_.completed;

                if (!result)
                {
                    ++stats_.failed;

                    if (result.State() == nebula::rpc::RpcCallState::Timeout)
                    {
                        ++stats_.timed_out;
                    }
                    else if (result.State() == nebula::rpc::RpcCallState::Cancelled)
                    {
                        ++stats_.cancelled;
                    }

                    continue;
                }

                if (result.ResponseValue().text() != payload_)
                {
                    ++stats_.mismatched;
                    continue;
                }

                ++stats_.succeeded;
                stats_.latency_ns.push_back(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        ended_at - sent_at).count());
            }

            OnLaneFinished();
            co_return stats_.error.empty() ? 0 : 1;
        }

        void StartLanes()
        {
            const std::size_t lane_count = static_cast<std::size_t>(
                std::min<std::uint64_t>(
                    static_cast<std::uint64_t>(opts_.window),
                    target_));

            if (lane_count == 0)
            {
                stats_.stopped = Clock::now();
                loop_.Quit();
                return;
            }

            lane_tasks_.reserve(lane_count);
            active_lanes_ = lane_count;

            for (std::size_t i = 0; i < lane_count; ++i)
            {
                lane_tasks_.emplace_back(RunLane());
            }

            // 关键：先让全部 Lane 跑到第一个 co_await。
            // 此时 window 个 RPC 只进入同一连接的 output_buffer，不立即发送。
            for (auto& task : lane_tasks_)
            {
                task.Start();
            }

            // 初始窗口一次性提交。
            stub_.Flush();
        }

        void OnLaneFinished()
        {
            if (active_lanes_ == 0)
            {
                return;
            }

            --active_lanes_;

            if (active_lanes_ == 0)
            {
                stats_.stopped = Clock::now();
                loop_.Quit();
            }
        }

        void CollectTaskErrors()
        {
            if (setup_task_.Done())
            {
                try
                {
                    if (setup_task_.Result() != 0 && stats_.error.empty())
                    {
                        stats_.error = "setup coroutine returned failure";
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

            for (auto& task : lane_tasks_)
            {
                if (!task.Done())
                {
                    continue;
                }

                try
                {
                    if (task.Result() != 0 && stats_.error.empty())
                    {
                        stats_.error = "lane coroutine returned failure";
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
        std::uint64_t target_ = 0;

        nebula::net::EventLoop loop_;
        nebula::rpc::RpcChannel channel_;
        EchoCoroutineStub stub_;
        const google::protobuf::MethodDescriptor* echo_method_ = nullptr;

        std::string payload_;

        std::uint64_t next_request_ = 0;
        std::size_t in_flight_ = 0;
        std::size_t active_lanes_ = 0;

        bool announced_ready_ = false;
        bool started_ = false;

        Stats stats_;

        // Task 必须在 channel_ 之后声明：析构顺序相反，
        // 这样协程 frame 会先销毁，仍在途 Awaiter 可安全调用 channel_ 取消请求。
        nebula::coro::Task<int> setup_task_;
        std::vector<nebula::coro::Task<int>> lane_tasks_;
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
        std::uint64_t succeeded = 0;
        std::uint64_t failed = 0;
        std::uint64_t timed_out = 0;
        std::uint64_t cancelled = 0;
        std::uint64_t mismatched = 0;
        std::uint64_t unfinished = 0;

        TimePoint earliest_start{};
        TimePoint latest_stop{};
        bool bad = false;

        for (const auto& s : results)
        {
            issued += s.issued;
            completed += s.completed;
            succeeded += s.succeeded;
            failed += s.failed;
            timed_out += s.timed_out;
            cancelled += s.cancelled;
            mismatched += s.mismatched;
            unfinished += s.unfinished;

            if (!s.error.empty())
            {
                std::cerr << "worker " << s.worker_id << ": " << s.error << '\n';
                bad = true;
            }

            if (s.started != TimePoint{} &&
                (earliest_start == TimePoint{} || s.started < earliest_start))
            {
                earliest_start = s.started;
            }

            if (s.stopped != TimePoint{} &&
                (latest_stop == TimePoint{} || s.stopped > latest_stop))
            {
                latest_stop = s.stopped;
            }

            latencies.insert(
                latencies.end(),
                s.latency_ns.begin(),
                s.latency_ns.end());
        }

        std::sort(latencies.begin(), latencies.end());

        const double wall_seconds =
            earliest_start != TimePoint{} && latest_stop > earliest_start
            ? std::chrono::duration<double>(latest_stop - earliest_start).count()
            : 0.0;

        const double success_qps = wall_seconds > 0.0
            ? static_cast<double>(succeeded) / wall_seconds
            : 0.0;

        const double avg_us = !latencies.empty()
            ? std::accumulate(latencies.begin(), latencies.end(), 0.0) /
            static_cast<double>(latencies.size()) / 1000.0
            : 0.0;

        const auto ToUs = [](std::int64_t ns)
            {
                return static_cast<double>(ns) / 1000.0;
            };

        const double p50_us = ToUs(Percentile(latencies, 0.50));
        const double p95_us = ToUs(Percentile(latencies, 0.95));
        const double p99_us = ToUs(Percentile(latencies, 0.99));
        const double p999_us = ToUs(Percentile(latencies, 0.999));

        const std::uint64_t total_inflight =
            static_cast<std::uint64_t>(o.workers) *
            static_cast<std::uint64_t>(o.window);

        std::cout << std::fixed << std::setprecision(3)
            << "mode=closed_loop_fixed_inflight"
            << " framework=NebulaRPC"
            << " host=" << o.host << ':' << o.port
            << " workers=" << o.workers
            << " connections=" << o.workers
            << " window=" << o.window
            << " total_inflight=" << total_inflight
            << " payload_text_bytes=" << o.payload_bytes << '\n'
            << "target=" << o.requests
            << " issued=" << issued
            << " completed=" << completed
            << " success=" << succeeded
            << " failed=" << failed
            << " timeout=" << timed_out
            << " cancelled=" << cancelled
            << " mismatched=" << mismatched
            << " unfinished=" << unfinished << '\n'
            << "wall_seconds=" << wall_seconds
            << " success_qps=" << success_qps << '\n'
            << "latency_success_only_us: avg=" << avg_us
            << " p50=" << p50_us
            << " p95=" << p95_us
            << " p99=" << p99_us
            << " p999=" << p999_us << '\n';

        if (!o.csv.empty())
        {
            std::ifstream old(o.csv, std::ios::binary | std::ios::ate);
            const bool write_header = !old.good() || old.tellg() == std::streampos(0);

            std::ofstream out(o.csv, std::ios::app);
            if (!out)
            {
                throw std::runtime_error("unable to open csv: " + o.csv);
            }

            if (write_header)
            {
                out
                    << "framework,mode,host,port,workers,connections,window,total_inflight,"
                    << "payload_text_bytes,target,issued,completed,success,failed,timeout,"
                    << "cancelled,mismatch,unfinished,wall_seconds,success_qps,"
                    << "avg_us,p50_us,p95_us,p99_us,p999_us\n";
            }

            out << std::fixed << std::setprecision(6)
                << "NebulaRPC,closed_loop_fixed_inflight,"
                << o.host << ',' << o.port << ','
                << o.workers << ',' << o.workers << ','
                << o.window << ',' << total_inflight << ','
                << o.payload_bytes << ',' << o.requests << ','
                << issued << ',' << completed << ',' << succeeded << ','
                << failed << ',' << timed_out << ',' << cancelled << ','
                << mismatched << ',' << unfinished << ','
                << wall_seconds << ',' << success_qps << ','
                << avg_us << ',' << p50_us << ',' << p95_us << ','
                << p99_us << ',' << p999_us << '\n';
        }

        if (bad ||
            issued != o.requests ||
            completed != issued ||
            succeeded != o.requests ||
            failed != 0 ||
            timed_out != 0 ||
            cancelled != 0 ||
            mismatched != 0 ||
            unfinished != 0)
        {
            return 1;
        }

        return 0;
    }

}  // namespace

int main(int argc, char** argv)
{
    try
    {
        const Options options = ParseOptions(argc, argv);

        SharedStart shared;
        std::vector<Stats> results(options.workers);
        std::vector<std::thread> threads;
        threads.reserve(options.workers);

        for (std::size_t i = 0; i < options.workers; ++i)
        {
            const std::uint64_t count =
                options.requests / options.workers +
                (i < options.requests % options.workers ? 1ULL : 0ULL);

            threads.emplace_back([&, i, count]
                {
                    try
                    {
                        Worker worker(options, shared, i, count);
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

        const TimePoint ready_deadline = Clock::now() +
            std::chrono::milliseconds(options.connect_timeout_ms) +
            std::chrono::seconds(30);

        while (shared.ready.load(std::memory_order_acquire) < options.workers &&
            Clock::now() < ready_deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }

        if (shared.ready.load(std::memory_order_acquire) < options.workers)
        {
            std::cerr << "benchmark workers did not become ready before deadline\n";
            shared.cancel.store(true, std::memory_order_release);
        }

        shared.go.store(true, std::memory_order_release);

        for (auto& thread : threads)
        {
            thread.join();
        }

        return Report(options, results);
    }
    catch (const std::exception& error)
    {
        std::cerr << "benchmark error: " << error.what() << '\n';
        return 2;
    }
}