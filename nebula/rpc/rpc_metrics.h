#pragma once

#include "nebula/rpc/rpc_call.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

namespace nebula::rpc
{

// 固定桶延迟直方图：桶界即 Roadmap 的 1/5/10/50/100/500/1000ms，超出落最后一个溢出桶
class LatencyHistogram
{
public:
    static constexpr std::size_t kBoundCount = 7;
    static constexpr std::size_t kBucketCount = kBoundCount + 1;

    // 每个桶的上界（含），单位微秒
    static constexpr std::array<std::int64_t, kBoundCount> kBoundsUs{
        1000, 5000, 10000, 50000, 100000, 500000, 1000000};

    void Record(std::chrono::microseconds latency) noexcept;
    void Reset() noexcept;

    [[nodiscard]] std::uint64_t Count() const noexcept;

    // 桶内累计反推：返回第一个使累计占比 >= percentile 的桶上界；落溢出桶时返回 -1
    [[nodiscard]] std::int64_t PercentileUs(double percentile) const noexcept;

private:
    std::array<std::atomic_uint64_t, kBucketCount> buckets_{};
    std::atomic_uint64_t count_{0};
};

// 单个 method 的计数与延迟分布；写在 loop 线程，计数用 atomic 以便跨线程读
class MethodMetrics
{
public:
    void Record(RpcCallState state, std::chrono::microseconds latency) noexcept;

    [[nodiscard]] std::uint64_t Total() const noexcept;
    [[nodiscard]] std::uint64_t Failed() const noexcept;
    [[nodiscard]] std::uint64_t Timeout() const noexcept;
    [[nodiscard]] const LatencyHistogram& Latency() const noexcept;

private:
    std::atomic_uint64_t total_{0};
    std::atomic_uint64_t failed_{0};
    std::atomic_uint64_t timeout_{0};
    LatencyHistogram latency_;
};

// 客户端侧指标表：按 "service.method" 分桶；QPS 的分母由调用方计时，这里只管计数
class RpcMetrics
{
public:
    void Record(std::string_view service_method,
                RpcCallState state,
                std::chrono::microseconds latency);

    // 结清本周期计数并返回它
    [[nodiscard]] std::uint64_t TakeCycleCount() noexcept;

    // 多行快照：每个 method 的计数与 P50/P95/P99，不含 QPS
    [[nodiscard]] std::string Dump() const;

private:
    std::unordered_map<std::string, MethodMetrics> methods_;
    std::atomic_uint64_t cycle_count_{0};
};

}  // namespace nebula::rpc
