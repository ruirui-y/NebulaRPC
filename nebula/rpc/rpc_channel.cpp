#include "nebula/rpc/rpc_channel.h"

#include "nebula/net/buffer.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_client.h"
#include "nebula/net/tcp_connection.h"
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
      client_(std::make_unique<net::TcpClient>(loop, std::move(ip), port))
{
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
    // 这一版 RpcChannel 是纯异步接口，不再同步等待响应。
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

    PendingCall pending_call{
        .response = response,
        .controller = controller,
        .done = done,
        .deadline = deadline,
        .timeout_timer = {},
    };

    loop_->RunInLoop([this,
                      request_id,
                      bytes = std::move(bytes),
                      pending_call = std::move(pending_call)]() mutable
        {
            RegisterAndSend(request_id, std::move(bytes), std::move(pending_call));
        });
}

void RpcChannel::RegisterAndSend(std::uint64_t request_id,
                                 std::string bytes,
                                 PendingCall pending_call)
{
    loop_->AssertInLoopThread();

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

    if (it->second.deadline.has_value())
    {
        it->second.timeout_timer = loop_->RunAt(
            *it->second.deadline,
            [this, request_id]
                {
                    OnTimeout(request_id);
                });
    }

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
    CompleteCallWithFailure(request_id, "RPC timeout");
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

    RemovePendingWrite(request_id);
    return pending_call;
}

void RpcChannel::CompleteCallWithFrame(std::uint64_t request_id,
                                       RpcFrame frame)
{
auto pending_call = TakePendingCall(request_id);

    if (!pending_call.has_value())
    {
        // timeout/disconnect 已经完成的调用，其迟到响应直接丢弃。
        return;
    }

    loop_->RunInLoop([response = pending_call->response,
                        controller = pending_call->controller,
                        done = pending_call->done,
                        frame = std::move(frame)]() mutable
        {
            CompleteFrame(response, controller, done, std::move(frame));
        });
}

void RpcChannel::CompleteCallWithFailure(std::uint64_t request_id,
                                         const std::string& reason)
{
    auto pending_call = TakePendingCall(request_id);

    if (!pending_call.has_value())
    {
        return;
    }

    loop_->QueueInLoop([controller = pending_call->controller,
                        done = pending_call->done,
                        reason]
        {
            CompleteFailure(controller, done, reason);
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
        CompleteCallWithFailure(request_id, reason);
    }
}

void RpcChannel::FailAllPendingNow(const std::string& reason)
{
    pending_writes_.clear();

    while (!pending_calls_.empty())
    {
        const std::uint64_t request_id = pending_calls_.begin()->first;
        auto pending_call = TakePendingCall(request_id);

        if (pending_call.has_value())
        {
            CompleteFailure(pending_call->controller,
                            pending_call->done,
                            reason);
        }
    }
}

}  // namespace nebula::rpc
