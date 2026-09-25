#include "nebula/rpc/rpc_channel.h"

#include "nebula/net/buffer.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_client.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <optional>
#include <utility>
#include <vector>

namespace nebula::rpc
{
namespace
{

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

void CompleteFailure(google::protobuf::RpcController* controller,
                     google::protobuf::Closure* done,
                     const std::string& reason)
{
    SetControllerFailure(controller, reason);

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

std::optional<RpcController::TimePoint> ResolveDeadline(
    google::protobuf::RpcController* controller)
{
    auto* rpc_controller = dynamic_cast<RpcController*>(controller);

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

    client_->SetConnectErrorCallback([this](const std::string& reason)
        {
            OnConnectError(reason);
        });

    client_->Connect();
}

RpcChannel::~RpcChannel()
{
    assert(loop_->IsInLoopThread());

    // 先失效存活守卫：此后任何迟到/在途的取消回调都会看到空指针直接放弃
    alive_guard_->channel.store(nullptr, std::memory_order_release);

    client_->SetConnectionCallback({});
    client_->SetMessageCallback({});
    client_->SetWriteCompleteCallback({});
    client_->SetConnectErrorCallback({});
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
        SetControllerFailure(controller, "async RpcChannel requires a non-null done callback");
        return;
    }

    if (method == nullptr || request == nullptr || response == nullptr)
    {
        loop_->RunInLoop([controller, done]
            {
                CompleteFailure(controller, done, "invalid RPC call arguments");
            });
        return;
    }

    const std::optional<TimePoint> deadline = ResolveDeadline(controller);

    std::string payload;

    if (!request->SerializeToString(&payload))
    {
        loop_->RunInLoop([controller, done]
            {
                CompleteFailure(controller, done, "request protobuf serialization failed");
            });
        return;
    }

    const std::uint64_t request_id = next_request_id_.fetch_add(1);

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::REQUEST);
    meta.set_request_id(request_id);
    meta.set_service_name(method->service()->full_name());
    meta.set_method_name(method->name());

    std::string bytes = RpcCodec::Encode(std::move(meta), payload);

    if (bytes.empty())
    {
        loop_->RunInLoop([controller, done]
            {
                CompleteFailure(controller, done, "request frame encoding failed");
            });
        return;
    }

    // PendingCall 里的 RpcCall 不可拷贝，过投递边界只能用 shared_ptr
    auto pending_call = std::make_shared<PendingCall>(PendingCall{
        .response = response,
        .controller = controller,
        .done = done,
        .deadline = deadline,
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

    if (connection_state_ == ConnectionState::kDisconnected)
    {
        CompleteFailure(pending_call.controller,
                        pending_call.done,
                        connection_error_.empty()
                            ? "RPC connection is not available"
                            : connection_error_);
        return;
    }

    if (pending_call.deadline.has_value() &&
        *pending_call.deadline <= std::chrono::steady_clock::now())
    {
        CompleteFailure(pending_call.controller, pending_call.done, "RPC timeout");
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

    // ---- 第四步：已连接直接发，未连接进写排队 ----
    if (connection_state_ == ConnectionState::kConnected &&
        connection_ &&
        connection_->Connected())
    {
        connection_->Send(bytes);
        return;
    }

    pending_writes_.push_back(PendingWrite{request_id, std::move(bytes)});
}

void RpcChannel::OnConnection(const net::TcpConnectionPtr& conn)
{
    loop_->AssertInLoopThread();

    if (conn->Connected())
    {
        connection_ = conn;
        connection_state_ = ConnectionState::kConnected;
        connection_error_.clear();

        while (!pending_writes_.empty())
        {
            PendingWrite write = std::move(pending_writes_.front());
            pending_writes_.pop_front();

            if (!pending_calls_.contains(write.request_id))
            {
                continue;
            }

            connection_->Send(write.bytes);
        }

        return;
    }

    if (connection_ == conn)
    {
        connection_.reset();
    }

    connection_state_ = ConnectionState::kDisconnected;
    connection_error_ = "RPC connection closed";
    FailAllPending(connection_error_);
}

void RpcChannel::OnMessage(const net::TcpConnectionPtr& conn,
                           net::Buffer* buffer)
{
    loop_->AssertInLoopThread();

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
            FailAllPending(connection_error_);
            conn->Shutdown();
            return;
        }

        HandleFrame(std::move(frame));
    }
}

void RpcChannel::OnConnectError(const std::string& reason)
{
    loop_->AssertInLoopThread();

    connection_state_ = ConnectionState::kDisconnected;
    connection_error_ = reason;
    connection_.reset();
    FailAllPending(reason);
}

void RpcChannel::OnTimeout(std::uint64_t request_id)
{
    loop_->AssertInLoopThread();
    CompleteCallWithFailure(request_id, "RPC timeout", RpcCallState::Timeout);
}

void RpcChannel::HandleFrame(RpcFrame frame)
{
    loop_->AssertInLoopThread();
    auto request_id = frame.meta.request_id();
    CompleteCallWithFrame(request_id, std::move(frame));
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

    RemovePendingWrite(request_id);
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

    // ---- 第三步：写错误并执行回调 ----
    loop_->QueueInLoop([controller = pending_call->controller,
                        done = pending_call->done,
                        reason,
                        state]
        {
            SetCallState(controller, state);
            CompleteFailure(controller, done, reason);
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

void RpcChannel::RemovePendingWrite(std::uint64_t request_id)
{
    pending_writes_.erase(
        std::remove_if(pending_writes_.begin(),
                       pending_writes_.end(),
                       [request_id](const PendingWrite& write)
                           {
                               return write.request_id == request_id;
                           }),
        pending_writes_.end());
}

void RpcChannel::FailAllPending(const std::string& reason)
{
    loop_->AssertInLoopThread();

    pending_writes_.clear();

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
    pending_writes_.clear();

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
            SetCallState(pending_call->controller, RpcCallState::Failed);
            CompleteFailure(pending_call->controller,
                            pending_call->done,
                            reason);
        }
    }
}

}  // namespace nebula::rpc
