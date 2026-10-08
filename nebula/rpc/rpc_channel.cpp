#include "nebula/rpc/rpc_channel.h"

#include "nebula/base/logger.h"
#include "nebula/net/buffer.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_client.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"

#include <cassert>
#include <chrono>
#include <optional>
#include <unistd.h>
#include <utility>
#include <vector>

namespace nebula::rpc
{
namespace
{

// 应用层心跳：3s 一次、连丢 3 次判死。keepalive 由对端内核应答，探不出进程僵死，这条才是进程级探测
constexpr auto kHeartbeatInterval = std::chrono::seconds(3);
constexpr int kHeartbeatMaxMissed = 3;

// trace_id 只需全局唯一：进程盐 + 单调计数 + 时钟，避免为每次调用引入随机数发生器
std::string GenerateTraceId()
{
    static const std::uint64_t process_salt =
        static_cast<std::uint64_t>(::getpid()) * 0x9E3779B97F4A7C15ULL;
    static std::atomic_uint64_t counter{0};

    std::uint64_t value = process_salt;
    value ^= counter.fetch_add(1, std::memory_order_relaxed) * 0x9E3779B97F4A7C15ULL;
    value ^= static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());

    constexpr char kHex[] = "0123456789abcdef";
    std::string trace_id(16, '0');

    for (int i = 15; i >= 0; --i)
    {
        trace_id[static_cast<std::size_t>(i)] =
            kHex[static_cast<std::size_t>(value & 0x0FU)];
        value >>= 4U;
    }

    return trace_id;
}

// Timeout / Deadline 二选一，同时设以 Deadline 为准
std::optional<RpcController::TimePoint> ResolveDeadline(
    google::protobuf::RpcController* controller)
{
    auto* rpc_controller = dynamic_cast<RpcController*>(controller);

    // 别的实现没有我们的 Timeout/Deadline 语义，只能当作无 deadline
    if (rpc_controller == nullptr)
    {
        return std::nullopt;
    }

    if (auto deadline = rpc_controller->Deadline(); deadline.has_value())
    {
        return deadline;
    }

    if (auto timeout = rpc_controller->Timeout(); timeout.has_value())
    {
        return RpcController::Clock::now() + *timeout;
    }

    return std::nullopt;
}

void SetControllerFailure(google::protobuf::RpcController* controller,
                          const std::string& reason)
{
    if (controller != nullptr)
    {
        controller->SetFailed(reason);
    }
}

void SetCallState(google::protobuf::RpcController* controller, RpcCallState state)
{
    auto* rpc_controller = dynamic_cast<RpcController*>(controller);

    if (rpc_controller != nullptr)
    {
        rpc_controller->MarkCallState(state);
    }
}

// state 不给默认值：强制每个调用点表态，终态漏写编译期就拦下
void CompleteFailure(google::protobuf::RpcController* controller,
                     google::protobuf::Closure* done,
                     const std::string& reason,
                     RpcCallState state)
{
    SetControllerFailure(controller, reason);
    SetCallState(controller, state);

    if (done != nullptr)
    {
        done->Run();
    }
}

void CompleteFrame(google::protobuf::Message* response,
                   google::protobuf::RpcController* controller,
                   google::protobuf::Closure* done,
                   RpcFrame frame)
{
    if (frame.meta.type() == proto::RpcMeta::ERROR)
    {
        SetControllerFailure(controller, frame.meta.error_text());
    }
    else if (frame.meta.type() != proto::RpcMeta::RESPONSE)
    {
        SetControllerFailure(controller, "unexpected RPC frame type");
    }
    else if (!response->ParseFromString(frame.payload))
    {
        SetControllerFailure(controller, "response protobuf parse failed");
    }

    if (done != nullptr)
    {
        done->Run();
    }
}

}  // namespace

RpcChannel::RpcChannel(net::EventLoop* loop, std::string ip, std::uint16_t port)
    : loop_(loop),
      client_(std::make_unique<net::TcpClient>(loop, std::move(ip), port)),
      alive_guard_(std::make_shared<AliveGuard>())
{
    alive_guard_->channel.store(this, std::memory_order_release);
    client_->SetConnectionCallback([this](const net::TcpConnectionPtr& conn)
        {
            OnConnection(conn);
        });

    client_->SetMessageCallback([this](const net::TcpConnectionPtr& conn,
                                       net::Buffer* buffer)
        {
            OnMessage(conn, buffer);
        });

    client_->Connect();
}

RpcChannel::~RpcChannel()
{
    assert(loop_->IsInLoopThread());

    // 定时器先进回收站：晚一拍的回调即使拿到 weak_guard 也只会看到一个已失效的 channel
    CancelHeartbeatInLoop();
    StopMetricsReport();

    // 先失效存活守卫：此后任何迟到/在途的取消回调都会看到空指针直接放弃
    alive_guard_->channel.store(nullptr, std::memory_order_release);

    client_->SetConnectionCallback({});
    client_->SetMessageCallback({});
    client_->SetWriteCompleteCallback({});
    client_.reset();

    connection_.reset();
    FailAllPendingNow("RPC channel closed");
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done)
{
    if (done == nullptr)
    {
        CompleteFailure(controller,
                        nullptr,
                        "async RpcChannel requires a non-null done callback",
                        RpcCallState::Invalid);
        return;
    }

    if (method == nullptr || request == nullptr || response == nullptr)
    {
        loop_->RunInLoop([controller, done]
            {
                CompleteFailure(controller,
                                done,
                                "invalid RPC call arguments",
                                RpcCallState::Invalid);
            });
        return;
    }

    const std::optional<TimePoint> deadline = ResolveDeadline(controller);

    std::string payload;

    if (!request->SerializeToString(&payload))
    {
        loop_->RunInLoop([controller, done]
            {
                CompleteFailure(controller,
                                done,
                                "request protobuf serialization failed",
                                RpcCallState::Invalid);
            });
        return;
    }

    const std::uint64_t request_id = next_request_id_.fetch_add(1);
    const std::string trace_id = GenerateTraceId();

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::REQUEST);
    meta.set_request_id(request_id);
    meta.set_service_name(method->service()->full_name());
    meta.set_method_name(method->name());
    meta.set_trace_id(trace_id);

    std::string bytes = RpcCodec::Encode(std::move(meta), payload);

    if (bytes.empty())
    {
        loop_->RunInLoop([controller, done]
            {
                CompleteFailure(controller,
                                done,
                                "request frame encoding failed",
                                RpcCallState::Invalid);
            });
        return;
    }

    const TimePoint sent_at = TimePoint::clock::now();

    NLOG_DEBUG("rpc call start request_id={} trace_id={} service={} method={}",
               request_id,
               trace_id,
               method->service()->full_name(),
               method->name());

    // PendingCall 里的 RpcCall 不可拷贝，过投递边界只能用 shared_ptr
    auto pending_call = std::make_shared<PendingCall>(PendingCall{
        .response = response,
        .controller = controller,
        .done = done,
        .method = method,
        .deadline = deadline,
        .sent_at = sent_at,
        .trace_id = trace_id,
        .timeout_timer = {},
        .cancel_token = -1,
        .call = {},
    });

    loop_->RunInLoop([this,
                      request_id,
                      bytes = std::move(bytes),
                      pending_call]() mutable
        {
            RegisterAndSend(request_id, std::move(bytes), std::move(*pending_call));
        });
}

void RpcChannel::RegisterAndSend(std::uint64_t request_id,
                                 std::string bytes,
                                 PendingCall pending_call)
{
    loop_->AssertInLoopThread();

    // 取消回调与超时定时器都可能活到 channel 析构之后，一律经 weak_ptr 校验，禁止捕获 this
    const std::weak_ptr<AliveGuard> weak_guard = alive_guard_;

    // 两种不可用状态都当场失败、都不排队：kDisconnected 透传永久原因，kConnecting 只报暂时不可用
    if (connection_state_ != ConnectionState::kConnected)
    {
        const std::string& reason = connection_state_ == ConnectionState::kDisconnected
                                        ? connection_error_
                                        : "RPC connection is not available";

        // 没进 pending_calls_ 的调用也计入指标，否则整类被拦下的请求都不会出现在统计里
        const auto latency = RecordMetrics(pending_call, RpcCallState::Failed);
        LogCompletion(request_id, pending_call, "rejected", latency, reason);

        CompleteFailure(pending_call.controller, pending_call.done, reason, RpcCallState::Failed);
        return;
    }

    // 过载闸门放在 deadline 之前：系统级资源保护优先于单次调用的自身超时
    if (max_pending_calls_ > 0U && pending_calls_.size() >= max_pending_calls_)
    {
        const auto latency = RecordMetrics(pending_call, RpcCallState::Failed);
        LogCompletion(request_id,
                      pending_call,
                      "rejected",
                      latency,
                      "too many pending rpcs");

        RejectOverloaded(pending_call.controller,
                         pending_call.response,
                         pending_call.done,
                         "too many pending rpcs");
        return;
    }

    if (pending_call.deadline.has_value() &&
        *pending_call.deadline <= std::chrono::steady_clock::now())
    {
        const auto latency = RecordMetrics(pending_call, RpcCallState::Timeout);
        LogCompletion(request_id, pending_call, "timeout", latency, "RPC timeout");

        CompleteFailure(pending_call.controller,
                        pending_call.done,
                        "RPC timeout",
                        RpcCallState::Timeout);
        return;
    }

    const auto [it, inserted] = pending_calls_.emplace(
        request_id, std::move(pending_call));
    assert(inserted);

    // ---- 第一步：注册取消回调，业务线程随时可能 StartCancel ----
    auto* rpc_controller = dynamic_cast<RpcController*>(it->second.controller);
    int cancel_token = -1;

    if (rpc_controller != nullptr)
    {
        // RegisterOnCancel 在 controller 已取消时会内联执行并摘掉 pending → it 失效，token 要先存
        cancel_token = rpc_controller->RegisterOnCancel(
            new RpcClosure([weak_guard, request_id, loop = loop_]
                {
                    // StartCancel 在任意业务线程：先确认 channel 活（活则 loop 活），否则这里是唯一的裸 loop 解引用
                    auto guard = weak_guard.lock();

                    if (guard == nullptr || guard->Acquire() == nullptr)
                    {
                        return;
                    }

                    loop->RunInLoop([weak_guard, request_id]
                        {
                            // 投递与执行之间 channel 仍可能析构，进 loop 线程后再校验一次
                            auto inner_guard = weak_guard.lock();

                            if (inner_guard == nullptr)
                            {
                                return;
                            }

                            RpcChannel* channel = inner_guard->Acquire();

                            if (channel == nullptr)
                            {
                                return;
                            }

                            channel->CompleteCallWithCancel(request_id);
                        });
                }));
    }

    // ---- 第二步：重新查表；注册前已被取消的调用会在这里发现条目已摘除 ----
    const auto alive_it = pending_calls_.find(request_id);

    if (alive_it == pending_calls_.end())
    {
        return;                                         // 已走取消完成路径
    }

    alive_it->second.cancel_token = cancel_token;

    // ---- 第三步：有 deadline 就挂超时定时器 ----
    if (alive_it->second.deadline.has_value())
    {
        alive_it->second.timeout_timer = loop_->RunAt(
            *alive_it->second.deadline,
            [weak_guard, request_id]
                {
                    // 定时器可能比 channel 长寿，捕 this 会在 channel 析构后打进已释放对象
                    auto guard = weak_guard.lock();

                    if (guard == nullptr)
                    {
                        return;
                    }

                    RpcChannel* channel = guard->Acquire();

                    if (channel == nullptr)
                    {
                        return;
                    }

                    channel->OnTimeout(request_id);
                });
    }

    // ---- 第四步：发送。闸门与发送之间同线程无交错，这里的检查纯粹是防御 ----
    if (connection_ == nullptr || !connection_->Connected())
    {
        CompleteCallWithFailure(request_id,
                                "RPC connection is not available",
                                RpcCallState::Failed);
        return;
    }

    connection_->Send(std::move(bytes));
}

void RpcChannel::OnConnection(const net::TcpConnectionPtr& conn)
{
    loop_->AssertInLoopThread();

    if (conn->Connected())
    {
        connection_ = conn;
        connection_state_ = ConnectionState::kConnected;
        connection_error_.clear();

        NLOG_INFO("rpc connection established name={}", conn->Name());

        // 连上即重新起表：重连成功本身不能算一次漏探测
        heartbeat_missed_ = 0;
        ScheduleHeartbeatInLoop();
        return;
    }

    CancelHeartbeatInLoop();

    if (connection_ == conn)
    {
        connection_.reset();
    }

    // 已判永久下线的（协议错误）不再被后续的掉线事件救活
    if (connection_state_ == ConnectionState::kDisconnected)
    {
        return;
    }

    // 回 kConnecting 而非 kDisconnected：Connector 会重连，这个窗口只代表「暂时不可用」
    connection_state_ = ConnectionState::kConnecting;
    connection_error_ = "RPC connection closed";

    NLOG_WARN("rpc connection closed name={} in_flight={}",
              conn->Name(),
              pending_calls_.size());

    FailAllPending(connection_error_);
}

void RpcChannel::OnMessage(const net::TcpConnectionPtr& conn,
                           net::Buffer* buffer)
{
    loop_->AssertInLoopThread();
    (void)conn;                                         // 一个 channel 只挂一条连接，帧不必区分来源

    while (true)
    {
        RpcFrame frame;
        std::string error;
        const RpcCodec::DecodeResult result = RpcCodec::Decode(buffer, &frame, &error);

        if (result == RpcCodec::DecodeResult::kNeedMore)
        {
            return;
        }

        if (result == RpcCodec::DecodeResult::kError)
        {
            connection_state_ = ConnectionState::kDisconnected;
            connection_error_ = "RPC protocol error: " + error;

            NLOG_ERROR("rpc protocol error detail={}", error);

            FailAllPending(connection_error_);

            // 协议不兼容重连治不好，且不停自愈会陷入「连上 -> 报错 -> 重连」死循环
            client_->Disconnect();
            return;
        }

        HandleFrame(std::move(frame));
    }
}

void RpcChannel::OnTimeout(std::uint64_t request_id)
{
    loop_->AssertInLoopThread();
    CompleteCallWithFailure(request_id, "RPC timeout", RpcCallState::Timeout);
}

void RpcChannel::HandleFrame(RpcFrame frame)
{
    loop_->AssertInLoopThread();

    // 心跳回包不进 pending_calls_：它只用来证明对端进程还在干活
    if (frame.meta.type() == proto::RpcMeta::HEARTBEAT)
    {
        heartbeat_missed_ = 0;
        return;
    }

    auto request_id = frame.meta.request_id();
    CompleteCallWithFrame(request_id, std::move(frame));
}

void RpcChannel::ScheduleHeartbeatInLoop()
{
    loop_->AssertInLoopThread();
    CancelHeartbeatInLoop();

    const std::weak_ptr<AliveGuard> weak_guard = alive_guard_;
    heartbeat_timer_ = loop_->RunAfter(kHeartbeatInterval, [weak_guard]
        {
            auto guard = weak_guard.lock();

            if (guard == nullptr)
            {
                return;
            }

            RpcChannel* channel = guard->Acquire();

            if (channel == nullptr)
            {
                return;
            }

            channel->OnHeartbeatTimer();
        });
}

void RpcChannel::CancelHeartbeatInLoop()
{
    loop_->AssertInLoopThread();

    if (!heartbeat_timer_.Valid())
    {
        return;
    }

    loop_->CancelTimer(heartbeat_timer_);
    heartbeat_timer_ = {};
}

void RpcChannel::OnHeartbeatTimer()
{
    loop_->AssertInLoopThread();

    if (connection_state_ != ConnectionState::kConnected || connection_ == nullptr)
    {
        heartbeat_missed_ = 0;                          // 没有连接可探，计数不该累加到下一条连接上
    }
    else if (heartbeat_missed_ >= kHeartbeatMaxMissed)
    {
        NLOG_WARN("rpc heartbeat lost missed={} name={}",
                  heartbeat_missed_,
                  connection_->Name());

        heartbeat_missed_ = 0;

        // 进程僵死时对端内核照样 ACK，只有走报丧线才能让 Connector 重连
        connection_->ForceClose();
    }
    else
    {
        ++heartbeat_missed_;
        SendHeartbeatInLoop();
    }

    ScheduleHeartbeatInLoop();
}

void RpcChannel::SendHeartbeatInLoop()
{
    loop_->AssertInLoopThread();

    if (connection_ == nullptr)
    {
        return;
    }

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::HEARTBEAT);
    meta.set_request_id(next_request_id_.fetch_add(1));
    meta.set_trace_id(GenerateTraceId());

    const std::string bytes = RpcCodec::Encode(std::move(meta), {});

    if (!bytes.empty())
    {
        connection_->Send(bytes);
    }
}

void RpcChannel::StartMetricsReport(std::chrono::milliseconds interval)
{
    loop_->RunInLoop([this, interval]
        {
            metrics_interval_ = interval;
            metrics_window_start_ = TimePoint::clock::now();

            if (metrics_interval_.count() <= 0)
            {
                return;
            }

            // 第一份报告落在一个周期之后：启动瞬间的分母接近 0，QPS 会被放大成噪声
            const std::weak_ptr<AliveGuard> weak_guard = alive_guard_;
            metrics_timer_ = loop_->RunAfter(metrics_interval_, [weak_guard]
                {
                    auto guard = weak_guard.lock();

                    if (guard == nullptr)
                    {
                        return;
                    }

                    RpcChannel* channel = guard->Acquire();

                    if (channel == nullptr)
                    {
                        return;
                    }

                    channel->ReportMetricsInLoop();
                });
        });
}

void RpcChannel::StopMetricsReport()
{
    loop_->RunInLoop([this]
        {
            metrics_interval_ = {};

            if (metrics_timer_.Valid())
            {
                loop_->CancelTimer(metrics_timer_);
                metrics_timer_ = {};
            }
        });
}

void RpcChannel::ReportMetricsInLoop()
{
    loop_->AssertInLoopThread();

    if (metrics_timer_.Valid())
    {
        loop_->CancelTimer(metrics_timer_);
        metrics_timer_ = {};
    }

    if (metrics_interval_.count() <= 0)
    {
        return;
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        TimePoint::clock::now() - metrics_window_start_);
    const auto count = metrics_.TakeCycleCount();
    const double seconds = std::chrono::duration<double>(elapsed).count();
    const double qps = seconds > 0.0 ? static_cast<double>(count) / seconds : 0.0;

    NLOG_INFO("rpc metrics qps={:.2f} in_flight={}\n{}",
              qps,
              pending_calls_.size(),
              metrics_.Dump());

    metrics_window_start_ = TimePoint::clock::now();

    const std::weak_ptr<AliveGuard> weak_guard = alive_guard_;
    metrics_timer_ = loop_->RunAfter(metrics_interval_, [weak_guard]
        {
            auto guard = weak_guard.lock();

            if (guard == nullptr)
            {
                return;
            }

            RpcChannel* channel = guard->Acquire();

            if (channel == nullptr)
            {
                return;
            }

            channel->ReportMetricsInLoop();
        });
}

std::optional<RpcChannel::PendingCall> RpcChannel::TakePendingCall(
    std::uint64_t request_id)
{
    loop_->AssertInLoopThread();

    const auto it = pending_calls_.find(request_id);

    if (it == pending_calls_.end())
    {
        return std::nullopt;
    }

    PendingCall pending_call = std::move(it->second);
    pending_calls_.erase(it);

    if (pending_call.timeout_timer.Valid())
    {
        loop_->CancelTimer(pending_call.timeout_timer);
    }

    // ---- 反注册取消回调：四条完成路径唯一收口（取消路径自身触发时已被换走，摘不到属正常）----
    if (pending_call.cancel_token >= 0)
    {
        auto* rpc_controller = dynamic_cast<RpcController*>(pending_call.controller);

        if (rpc_controller != nullptr)
        {
            rpc_controller->RemoveOnCancel(pending_call.cancel_token);
        }
    }

    return pending_call;
}

void RpcChannel::CompleteCallWithFrame(std::uint64_t request_id,
                                       RpcFrame frame)
{
    // ---- 第一步：找到在途调用，CAS 抢占完成权 ----
    auto it = pending_calls_.find(request_id);

    if (it == pending_calls_.end())
    {
        // timeout/cancel/disconnect 已完成的调用，其迟到响应直接丢弃
        return;
    }

    if (!it->second.call.TryComplete(RpcCallState::Completed))
    {
        return;                                         // 其他完成来源抢先
    }

    // ---- 第二步：仲裁成功，摘除并清理资源 ----
    auto pending_call = TakePendingCall(request_id);

    if (!pending_call.has_value())
    {
        return;
    }

    // 对端回 ERROR 也算一次失败：指标与日志按对端结果记，完成权仍归 response
    const bool server_error = frame.meta.type() == proto::RpcMeta::ERROR;
    const auto latency = RecordMetrics(*pending_call,
                                       server_error ? RpcCallState::Failed
                                                    : RpcCallState::Completed);

    LogCompletion(request_id,
                  *pending_call,
                  server_error ? "server-error" : "completed",
                  latency,
                  server_error ? std::string_view(frame.meta.error_text()) : std::string_view{});

    // ---- 第三步：写结果并执行回调 ----
    loop_->RunInLoop([response = pending_call->response,
                        controller = pending_call->controller,
                        done = pending_call->done,
                        frame = std::move(frame)]() mutable
        {
            SetCallState(controller, RpcCallState::Completed);
            CompleteFrame(response, controller, done, std::move(frame));
        });
}

void RpcChannel::CompleteCallWithFailure(std::uint64_t request_id,
                                         const std::string& reason,
                                         RpcCallState state)
{
    // ---- 第一步：找到在途调用，CAS 抢占完成权 ----
    auto it = pending_calls_.find(request_id);

    if (it == pending_calls_.end())
    {
        return;                                         // 已被其他来源完成
    }

    if (!it->second.call.TryComplete(state))
    {
        return;                                         // 其他完成来源抢先
    }

    // ---- 第二步：仲裁成功，摘除并清理资源 ----
    auto pending_call = TakePendingCall(request_id);

    if (!pending_call.has_value())
    {
        return;
    }

    const auto latency = RecordMetrics(*pending_call, state);
    LogCompletion(request_id, *pending_call, "failed", latency, reason);

    // ---- 第三步：写错误并执行回调 ----
    loop_->QueueInLoop([controller = pending_call->controller,
                        done = pending_call->done,
                        reason,
                        state]
        {
            CompleteFailure(controller, done, reason, state);
        });
}

void RpcChannel::CompleteCallWithCancel(std::uint64_t request_id)
{
    loop_->AssertInLoopThread();

    // ---- 第一步：找到在途调用，CAS 抢占完成权 ----
    auto it = pending_calls_.find(request_id);

    if (it == pending_calls_.end())
    {
        return;                                         // 已被其他来源完成
    }

    if (!it->second.call.TryComplete(RpcCallState::Cancelled))
    {
        return;                                         // response/timeout 抢先
    }

    // ---- 第二步：仲裁成功，摘除并清理资源 ----
    auto pending_call = TakePendingCall(request_id);

    if (!pending_call.has_value())
    {
        return;
    }

    const auto latency = RecordMetrics(*pending_call, RpcCallState::Cancelled);
    LogCompletion(request_id, *pending_call, "cancelled", latency, {});

    // ---- 第三步：抢到完成权之后才把「取消」标记为已生效（输给 response/timeout 时保持 false）----
    auto* rpc_controller = dynamic_cast<RpcController*>(pending_call->controller);

    if (rpc_controller != nullptr)
    {
        rpc_controller->MarkCanceled();
    }

    // ---- 第四步：执行回调；取消不算失败，不 SetFailed，业务读 IsCanceled() ----
    loop_->QueueInLoop([done = pending_call->done]
        {
            if (done != nullptr)
            {
                done->Run();
            }
        });
}

std::chrono::microseconds RpcChannel::RecordMetrics(const PendingCall& pending_call,
                                                    RpcCallState state)
{
    if (pending_call.method == nullptr)
    {
        return {};
    }

    const auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
        TimePoint::clock::now() - pending_call.sent_at);

    metrics_.Record(pending_call.method->service()->full_name() + "." +
                        pending_call.method->name(),
                    state,
                    latency);

    // 返回实测延迟，让日志与指标用同一份数字，不各算一次
    return latency;
}

void RpcChannel::LogCompletion(std::uint64_t request_id,
                               const PendingCall& pending_call,
                               std::string_view outcome,
                               std::chrono::microseconds latency,
                               std::string_view error)
{
    const auto* method = pending_call.method;

    const std::string_view service = method != nullptr
                                         ? std::string_view(method->service()->full_name())
                                         : std::string_view{};
    const std::string_view method_name = method != nullptr
                                             ? std::string_view(method->name())
                                             : std::string_view{};

    NLOG_DEBUG("rpc call request_id={} trace_id={} outcome={} service={} method={} latency_ms={:.3f} error={}",
               request_id,
               pending_call.trace_id,
               outcome,
               service,
               method_name,
               static_cast<double>(latency.count()) / 1000.0,
               error);
}

void RpcChannel::FailAllPending(const std::string& reason)
{
    loop_->AssertInLoopThread();

    std::vector<std::uint64_t> request_ids;
    request_ids.reserve(pending_calls_.size());

    for (const auto& [request_id, call] : pending_calls_)
    {
        (void)call;
        request_ids.push_back(request_id);
    }

    for (const std::uint64_t request_id : request_ids)
    {
        CompleteCallWithFailure(request_id, reason, RpcCallState::Failed);
    }
}

void RpcChannel::FailAllPendingNow(const std::string& reason)
{
    while (!pending_calls_.empty())
    {
        const std::uint64_t request_id = pending_calls_.begin()->first;

        // ---- 与其他完成路径一致：先 CAS 抢占完成权 ----
        const bool won = pending_calls_.begin()->second.call.TryComplete(
            RpcCallState::Failed);

        // 无论是否抢到完成权都要摘除，否则定时器会在析构后触发
        auto pending_call = TakePendingCall(request_id);

        if (!pending_call.has_value())
        {
            continue;
        }

        if (won)
        {
            const auto latency = RecordMetrics(*pending_call, RpcCallState::Failed);
            LogCompletion(request_id, *pending_call, "failed", latency, reason);

            CompleteFailure(pending_call->controller,
                            pending_call->done,
                            reason,
                            RpcCallState::Failed);
        }
    }
}

void RpcChannel::SetMaxPendingCalls(std::size_t max_pending_calls) noexcept
{
    max_pending_calls_ = max_pending_calls;
}

void RpcChannel::SetDegradeHandler(DegradeHandler handler)
{
    degrade_handler_ = std::move(handler);
}

// 以下两个读的是 loop 线程独占的容器与计数器，只能在 owner loop 线程调用
std::size_t RpcChannel::PendingCallCount() const noexcept
{
    return pending_calls_.size();
}

std::uint64_t RpcChannel::OverloadRejectCount() const noexcept
{
    return overload_reject_count_.load();
}

void RpcChannel::RejectOverloaded(google::protobuf::RpcController* controller,
                                  google::protobuf::Message* response,
                                  google::protobuf::Closure* done,
                                  const std::string& reason)
{
    overload_reject_count_.fetch_add(1);

    // 钩子返回 true 即由业务给出兜底结果；返回 false 说明它不接管，回落为直接失败
    if (degrade_handler_ != nullptr && degrade_handler_(controller, response, reason))
    {
        // 钩子已自行完成该次调用：往返被收口，业务是否失败由 Failed() 表达
        SetCallState(controller, RpcCallState::Completed);

        if (done != nullptr)
        {
            done->Run();
        }
        return;
    }

    CompleteFailure(controller, done, reason, RpcCallState::Failed);
}

}  // namespace nebula::rpc
