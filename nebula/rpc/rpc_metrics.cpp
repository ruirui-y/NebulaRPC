#include "nebula/rpc/rpc_metrics.h"

#include <sstream>

namespace nebula::rpc
{

void LatencyHistogram::Record(std::chrono::microseconds latency) noexcept
{
    const std::int64_t value = latency.count();
    std::size_t bucket = kBoundCount;

    for (std::size_t i = 0; i < kBoundCount; ++i)
    {
        if (value <= kBoundsUs[i])
        {
            bucket = i;
            break;
        }
    }

    buckets_[bucket].fetch_add(1, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
}

void LatencyHistogram::Reset() noexcept
{
    for (auto& bucket : buckets_)
    {
        bucket.store(0, std::memory_order_relaxed);
    }
    count_.store(0, std::memory_order_relaxed);
}

std::uint64_t LatencyHistogram::Count() const noexcept
{
    return count_.load(std::memory_order_relaxed);
}

std::int64_t LatencyHistogram::PercentileUs(double percentile) const noexcept
{
    const std::uint64_t total = Count();
    if (total == 0U)
    {
        return 0;
    }

    const auto target = static_cast<std::uint64_t>(
        static_cast<double>(total) * percentile + 0.999999);

    std::uint64_t accumulated = 0;
    for (std::size_t i = 0; i < kBucketCount; ++i)
    {
        accumulated += buckets_[i].load(std::memory_order_relaxed);

        if (accumulated >= target)
        {
            // 溢出桶没有上界，返回 -1 表达「超过最大桶界」而不是编一个数字
            return i < kBoundCount ? kBoundsUs[i] : -1;
        }
    }

    return -1;
}

void MethodMetrics::Record(RpcCallState state, std::chrono::microseconds latency) noexcept
{
    total_.fetch_add(1, std::memory_order_relaxed);

    if (state == RpcCallState::Timeout)
    {
        timeout_.fetch_add(1, std::memory_order_relaxed);
    }
    else if (state != RpcCallState::Completed)
    {
        failed_.fetch_add(1, std::memory_order_relaxed);
    }

    latency_.Record(latency);
}

std::uint64_t MethodMetrics::Total() const noexcept
{
    return total_.load(std::memory_order_relaxed);
}

std::uint64_t MethodMetrics::Failed() const noexcept
{
    return failed_.load(std::memory_order_relaxed);
}

std::uint64_t MethodMetrics::Timeout() const noexcept
{
    return timeout_.load(std::memory_order_relaxed);
}

const LatencyHistogram& MethodMetrics::Latency() const noexcept
{
    return latency_;
}

void RpcMetrics::Record(std::string_view service_method,
                        RpcCallState state,
                        std::chrono::microseconds latency)
{
    methods_[std::string(service_method)].Record(state, latency);
    cycle_count_.fetch_add(1, std::memory_order_relaxed);
}

std::uint64_t RpcMetrics::TakeCycleCount() noexcept
{
    return cycle_count_.exchange(0, std::memory_order_relaxed);
}

std::string RpcMetrics::Dump() const
{
    // 溢出桶无上界，打印成 ">1000ms" 而不是编一个数字
    const auto format_percentile = [](std::int64_t us) -> std::string
    {
        if (us < 0)
        {
            return ">" + std::to_string(LatencyHistogram::kBoundsUs.back() / 1000) + "ms";
        }
        return std::to_string(us / 1000) + "ms";
    };

    std::ostringstream output;
    output << "methods=" << methods_.size();

    for (const auto& [name, metric] : methods_)
    {
        const LatencyHistogram& latency = metric.Latency();

        output << "\n  " << name
               << " total=" << metric.Total()
               << " failed=" << metric.Failed()
               << " timeout=" << metric.Timeout()
               << " p50=" << format_percentile(latency.PercentileUs(0.50))
               << " p95=" << format_percentile(latency.PercentileUs(0.95))
               << " p99=" << format_percentile(latency.PercentileUs(0.99));
    }

    return output.str();
}

}  // namespace nebula::rpc
