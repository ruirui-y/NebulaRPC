// NebulaRPC 异步回调压测客户端
// 每个 Worker：一个线程、一个 EventLoop、一个 RpcChannel。
// 最大在途 RPC 数 = workers * window。

#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"
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
#include <limits>
#include <memory>
#include <numeric>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    // 01. 测试参数：未传入的参数使用默认值。
    struct Options
    {
        std::string host = "127.0.0.1";
        std::uint16_t port = 9000;
        std::size_t workers = 1;             // Worker / 连接数
        std::size_t window = 1;              // 每连接最大在途 RPC
        std::uint64_t requests = 100000;     // 正式测试的总请求数
        std::size_t payload_bytes = 64;      // EchoRequest.text 字节数
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

        std::cerr << "Usage: " << exe << " [--host IP] [--port N] [--workers N] [--window N]"
            << " [--requests N] [--payload-bytes N] [--warmup-requests N]"
            << " [--timeout-ms N] [--connect-timeout-ms N] [--max-seconds N]"
            << " [--csv output.csv]\n"
            << "  Example ping-pong: --workers 1 --window 1 --requests 100000 --payload-bytes 64\n"
            << "  Example 16 connections: --workers 16 --window 1 --requests 200000 --payload-bytes 1024\n"
            << "  Example 1 connection/16 inflight: --workers 1 --window 16 --requests 200000\n";

        std::exit(error.empty() ? 0 : 2);
    }

    // 只接受非负十进制整数。
    std::uint64_t ParseUnsigned(const std::string& s)
    {
        if (s.empty() || s[0] == '-')
        {
            throw std::invalid_argument(s);
        }

        std::size_t used = 0;
        auto value = std::stoull(s, &used, 10);

        if (used != s.size())
        {
            throw std::invalid_argument(s);
        }

        return value;
    }

    // 02. 解析命令行，检查参数范围。
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
                    auto n = ParseUnsigned(value);

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
                    auto n = ParseUnsigned(value);

                    if (n > 120000)
                    {
                        Usage(argv[0], "timeout-ms too large");
                    }

                    o.timeout_ms = static_cast<int>(n);
                }
                else if (name == "--connect-timeout-ms")
                {
                    auto n = ParseUnsigned(value);

                    if (n > 120000)
                    {
                        Usage(argv[0], "connect-timeout-ms too large");
                    }

                    o.connect_timeout_ms = static_cast<int>(n);
                }
                else if (name == "--max-seconds")
                {
                    auto n = ParseUnsigned(value);

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

    // 03. 所有 Worker 预热后，由主线程统一放行。
    struct SharedStart
    {
        std::atomic<std::size_t> ready{ 0 };
        std::atomic<bool> go{ false };
    };

    // 04. 每个 Worker 独立统计，结束后由主线程汇总。
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

    // 05. 保持请求、响应和 Controller 存活至异步完成。
    struct CallContext
    {
        nebula::example::EchoRequest request;
        nebula::example::EchoResponse response;
        nebula::rpc::RpcController controller;
        TimePoint sent_at{};
    };

    // 06. 每个 Worker 持有独立的事件循环和 RPC 连接。
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
            payload_(opts.payload_bytes, 'x')
        {
            stats_.worker_id = worker_id;
            stats_.latency_ns.reserve(static_cast<std::size_t>(target_));
        }

        // 07. 当前线程进入 EventLoop，退出后返回统计结果。
        Stats Run()
        {
            connection_deadline_ = Clock::now() + std::chrono::milliseconds(opts_.connect_timeout_ms);

            loop_.RunAfter(std::chrono::milliseconds(100), [this]
                {
                    Probe();
                });

            loop_.Loop();

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

        // 记录错误并结束当前 Worker 的事件循环。
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

        // 08. 探测连接，失败后通过定时器重试。
        void Probe()
        {
            if (Clock::now() > connection_deadline_)
            {
                Abort("RPC connection/probe failed before deadline");
                return;
            }

            auto c = std::make_shared<CallContext>();
            c->request.set_text("nebula-benchmark-probe");
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
            channel_.Flush();
        }

        // 09. 每个 Worker 单独预热，不计入正式测试。
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
            channel_.Flush();
        }

        // 10. 等待全局起跑信号，然后开始计时和发压。
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

            Pump();

            // 初始 window 个请求全部进入发送缓冲后统一提交。
            // 稳态阶段由 RpcChannel::OnMessage 在响应批次末尾继续统一 Flush。
            channel_.Flush();
        }

        // 11. 闭环发压：按 window 填充在途 RPC。
        void Pump()
        {
            if (!started_ || !stats_.error.empty())
            {
                return;
            }

            while (in_flight_ < opts_.window && stats_.issued < target_)
            {
                auto c = std::make_shared<CallContext>();
                c->request.set_text(payload_);
                c->controller.SetTimeout(std::chrono::milliseconds(opts_.timeout_ms));
                c->sent_at = Clock::now();

                ++stats_.issued;
                ++in_flight_;

                // 回调统一投递回本 Worker 的 EventLoop。
                auto* done = new nebula::rpc::RpcClosure([this, c]
                    {
                        loop_.QueueInLoop([this, c]
                            {
                                OnDone(c);
                            });
                    });

                // 进入 NebulaRPC 框架：Stub -> RpcChannel::CallMethod。
                stub_.Echo(&c->controller, &c->request, &c->response, done);
            }
        }

        // 12. 统计本次完成结果，并继续填充窗口。
        void OnDone(const std::shared_ptr<CallContext>& c)
        {
            const auto ended = Clock::now();

            --in_flight_;
            ++stats_.completed;

            if (c->controller.Failed())
            {
                ++stats_.failed;

                if (c->controller.CallState() == nebula::rpc::RpcCallState::Timeout)
                {
                    ++stats_.timed_out;
                }
            }
            else if (c->response.text() != payload_)
            {
                ++stats_.mismatched;
            }
            else
            {
                ++stats_.succeeded;

                stats_.latency_ns.push_back(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        ended - c->sent_at).count());
            }

            if (stats_.completed == target_)
            {
                stats_.stopped = ended;
                loop_.Quit();
            }
            else
            {
                Pump();
            }
        }

        const Options& opts_;
        SharedStart& shared_;
        std::uint64_t target_;
        nebula::net::EventLoop loop_;
        nebula::rpc::RpcChannel channel_;
        nebula::example::EchoService_Stub stub_;
        std::string payload_;
        TimePoint connection_deadline_{};
        std::size_t warmed_ = 0;
        std::size_t in_flight_ = 0;
        bool announced_ready_ = false;
        bool started_ = false;
        Stats stats_;
    };

    // 13. 在已排序的延迟样本中按秩取分位值。
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

    // 14. 汇总各 Worker 的 QPS、延迟和错误统计。
    int Report(const Options& o, std::vector<Stats>& results)
    {
        std::vector<std::int64_t> latencies;
        std::uint64_t issued = 0, completed = 0, success = 0, failed = 0;
        std::uint64_t timeout = 0, mismatch = 0, unfinished = 0;
        TimePoint earliest{}, latest{};
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

            latencies.insert(latencies.end(), s.latency_ns.begin(), s.latency_ns.end());
        }

        std::sort(latencies.begin(), latencies.end());

        // 统计窗口：最早 Worker 开始到最晚 Worker 结束。
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
            << "host=" << o.host << ':' << o.port << " workers=" << o.workers
            << " connections=" << o.workers << " window_per_connection=" << o.window
            << " payload_text_bytes=" << o.payload_bytes << '\n'
            << "target=" << o.requests << " issued=" << issued << " completed=" << completed
            << " success=" << success << " failed=" << failed
            << " timeout=" << timeout << " mismatched=" << mismatch
            << " unfinished=" << unfinished << '\n'
            << "wall_seconds=" << seconds << " success_qps=" << qps << '\n'
            << "latency_success_only_us: avg=" << mean_us << " p50=" << p50
            << " p95=" << p95 << " p99=" << p99 << " p999=" << p999 << '\n';

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
                    "target,issued,completed,success,failed,timeout,mismatch,unfinished,"
                    "wall_seconds,success_qps,avg_us,p50_us,p95_us,p99_us,p999_us\n";
            }

            out << "NebulaRPC," << o.host << ',' << o.port << ',' << o.workers << ','
                << o.workers << ',' << o.window << ',' << o.payload_bytes << ','
                << o.requests << ',' << issued << ',' << completed << ',' << success << ','
                << failed << ',' << timeout << ',' << mismatch << ',' << unfinished << ','
                << seconds << ',' << qps << ',' << mean_us << ',' << p50 << ','
                << p95 << ',' << p99 << ',' << p999 << '\n';
        }

        if (bad || issued != o.requests || completed != issued ||
            failed || mismatch || unfinished)
        {
            return 1;
        }

        return 0;
    }
}

// 15. 主线程：创建 Worker、统一起跑、回收线程、输出报告。
int main(int argc, char** argv)
{
    try
    {
        const Options o = ParseOptions(argc, argv);

        // 避免逐请求日志影响压测结果。
        spdlog::set_level(spdlog::level::err);

        SharedStart shared;
        std::vector<Stats> results(o.workers);
        std::vector<std::thread> threads;
        threads.reserve(o.workers);

        for (std::size_t i = 0; i < o.workers; ++i)
        {
            // 总请求数均分给各 Worker，余数分配给前面的线程。
            const std::uint64_t count = o.requests / o.workers +
                (i < o.requests % o.workers ? 1 : 0);

            threads.emplace_back([&, i, count]
                {
                    try
                    {
                        Worker w(o, shared, i, count);
                        results[i] = w.Run();
                    }
                    catch (const std::exception& e)
                    {
                        results[i].worker_id = i;
                        results[i].error = e.what();
                        shared.ready.fetch_add(1, std::memory_order_release);
                    }
                });
        }

        // 等待所有 Worker 就绪，或达到准备阶段的截止时间。
        const auto deadline = Clock::now() +
            std::chrono::milliseconds(o.connect_timeout_ms +
                static_cast<int>(o.warmup_requests * 10 + 10000));

        while (shared.ready.load(std::memory_order_acquire) < o.workers &&
            Clock::now() < deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }

        shared.go.store(true, std::memory_order_release);

        for (auto& t : threads)
        {
            t.join();
        }

        return Report(o, results);
    }
    catch (const std::exception& e)
    {
        std::cerr << "benchmark error: " << e.what() << '\n';
        return 2;
    }
}