#include "nebula/rpc/rpc_server.h"

#include "nebula/base/logger.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/rpc/rpc_closure.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <chrono>
#include <memory>
#include <utility>

namespace nebula::rpc
{
namespace
{

// 优雅关闭的总上限：等 in-flight 清空与等连接退场共用这一条
constexpr auto kGracefulTimeout = std::chrono::seconds(10);

}  // namespace

struct RpcServer::ServerCall
{
    std::unique_ptr<google::protobuf::Message> request;
    std::unique_ptr<google::protobuf::Message> response;
};

RpcServer::RpcServer(net::EventLoop* loop, std::string ip, std::uint16_t port)
    : loop_(loop),
      server_(loop, std::move(ip), port)
{
    server_.SetMessageCallback(
        [this](const net::TcpConnectionPtr& conn, net::Buffer* buffer)
            {
                OnMessage(conn, buffer);
            });
}

void RpcServer::RegisterService(google::protobuf::Service* service)
{
    if (service == nullptr)
    {
        return;
    }
    const auto* descriptor = service->GetDescriptor();
    services_[descriptor->full_name()] = service;
}

void RpcServer::Start(std::size_t io_thread_count)
{
    server_.Start(io_thread_count);
}

void RpcServer::SetShutdownCompleteCallback(std::function<void()> callback)
{
    shutdown_complete_callback_ = std::move(callback);
}

void RpcServer::Stop()
{
    loop_->RunInLoop([this]
        {
            StopInLoop();
        });
}

void RpcServer::StopInLoop()
{
    loop_->AssertInLoopThread();

    if (stopping_.exchange(true))
    {
        return;                                         // 幂等：重复 Stop 不重排关闭流程
    }

    NLOG_INFO("rpc server stopping in_flight={}", in_flight_.load());

    // 第一步：停止接受新连接；已在途的调用不受影响
    server_.Stop();

    grace_timer_ = loop_->RunAfter(kGracefulTimeout, [this]
        {
            NLOG_WARN("rpc server shutdown timeout in_flight={}", in_flight_.load());
            CloseConnectionsInLoop();
            FinishShutdownInLoop("timeout");
        });

    if (in_flight_.load() == 0U)
    {
        CloseConnectionsInLoop();
    }
}

void RpcServer::CloseConnectionsInLoop()
{
    loop_->AssertInLoopThread();

    if (connections_closed_)
    {
        return;
    }
    connections_closed_ = true;

    const std::uint64_t remaining = in_flight_.load();

    if (remaining != 0U)
    {
        NLOG_WARN("rpc server dropping in_flight={} at graceful shutdown", remaining);
    }

    // 用 Shutdown 而非 ForceClose：FIN 排在待发缓冲之后，已生成的响应不会丢
    server_.CloseAllConnections([this]
        {
            FinishShutdownInLoop("drained");
        });
}

void RpcServer::FinishShutdownInLoop(std::string_view reason)
{
    loop_->AssertInLoopThread();

    if (shutdown_finished_)
    {
        return;
    }
    shutdown_finished_ = true;

    if (grace_timer_.Valid())
    {
        loop_->CancelTimer(grace_timer_);
        grace_timer_ = {};
    }

    NLOG_INFO("rpc server stopped reason={} in_flight={}", reason, in_flight_.load());

    if (shutdown_complete_callback_)
    {
        shutdown_complete_callback_();
    }
}

void RpcServer::OnCallFinished()
{
    // done 可能被业务在任意线程调用，关连接的决策必须回到 loop 线程做
    if (in_flight_.fetch_sub(1, std::memory_order_acq_rel) == 1U && stopping_.load())
    {
        loop_->RunInLoop([this]
            {
                CloseConnectionsInLoop();
            });
    }
}

void RpcServer::OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer)
{
    while (true)
    {
        RpcFrame frame;
        std::string error;
        const auto result = RpcCodec::Decode(buffer, &frame, &error);
        if (result == RpcCodec::DecodeResult::kNeedMore)
        {
            return;
        }
        if (result == RpcCodec::DecodeResult::kError)
        {
            NLOG_ERROR("rpc protocol error name={} detail={}", conn->Name(), error);
            conn->Shutdown();
            return;
        }

        if (frame.meta.type() == proto::RpcMeta::HEARTBEAT)
        {
            SendHeartbeatEcho(conn, frame.meta);
            return;
        }

        if (frame.meta.type() != proto::RpcMeta::REQUEST)
        {
            HandleRequest(conn, frame);
            return;
        }
        
        SendError(conn,
            frame.meta.request_id(),
            frame.meta.trace_id(),
            RpcErrorCode::BadRequest,
            "server received a non-request/heartbeat frame");
    }
}

void RpcServer::HandleRequest(const net::TcpConnectionPtr& conn, const RpcFrame& frame)
{
    const auto service_it = services_.find(frame.meta.service_name());
    if (service_it == services_.end())
    {
        SendError(conn, frame.meta.request_id(), frame.meta.trace_id(), RpcErrorCode::NotFound, "service not found");
        return;
    }

    google::protobuf::Service* service = service_it->second;
    const auto* method = service->GetDescriptor()->FindMethodByName(frame.meta.method_name());
    if (method == nullptr)
    {
        SendError(conn, frame.meta.request_id(), frame.meta.trace_id(), RpcErrorCode::NotFound, "method not found");
        return;
    }

    auto call = std::make_shared<ServerCall>();
    call->request.reset(service->GetRequestPrototype(method).New());
    call->response.reset(service->GetResponsePrototype(method).New());

    if (!call->request->ParseFromString(frame.payload))
    {
        SendError(conn,
                  frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::BadRequest,
                  "request protobuf parse failed");
        return;
    }

    const std::uint64_t request_id = frame.meta.request_id();
    const std::string trace_id = frame.meta.trace_id();

    NLOG_DEBUG("rpc server dispatch request_id={} trace_id={} service={} method={} name={}",
               request_id,
               trace_id,
               frame.meta.service_name(),
               frame.meta.method_name(),
               conn->Name());

    // 先计数再调用：业务可能同步回调 done，计数晚了就会在清零之后又减一次
    in_flight_.fetch_add(1, std::memory_order_relaxed);

    auto* done = new RpcClosure([this, conn, call, request_id, trace_id]
        {
            SendResponse(conn, request_id, trace_id, *call->response);
            OnCallFinished();
        });

    service->CallMethod(method, nullptr, call->request.get(), call->response.get(), done);
}

void RpcServer::SendResponse(const net::TcpConnectionPtr& conn,
                             std::uint64_t request_id,
                             const std::string& trace_id,
                             const google::protobuf::Message& response)
{
    std::string payload;
    if (!response.SerializeToString(&payload))
    {
        SendError(conn, request_id, trace_id, RpcErrorCode::InternalError, "response protobuf serialization failed");
        return;
    }

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::RESPONSE);
    meta.set_request_id(request_id);
    meta.set_trace_id(trace_id);
    const std::string bytes = RpcCodec::Encode(std::move(meta), payload);
    if (bytes.empty())
    {
        SendError(conn, request_id, trace_id, RpcErrorCode::InternalError, "response frame encoding failed");
        return;
    }
    conn->Send(bytes);
}

void RpcServer::SendError(const net::TcpConnectionPtr& conn,
                          std::uint64_t request_id,
                          const std::string& trace_id,
                          RpcErrorCode error_code,
                          std::string error_text)
{
    // 枚举只在本端表达语义，出网前统一落地成 int32（日志同样用整数，避免依赖枚举的格式化器）
    const auto code = static_cast<std::int32_t>(error_code);

    NLOG_WARN("rpc server error request_id={} trace_id={} code={} text={}",
              request_id,
              trace_id,
              code,
              error_text);

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::ERROR);
    meta.set_request_id(request_id);
    meta.set_trace_id(trace_id);
    meta.set_error_code(code);
    meta.set_error_text(std::move(error_text));
    const std::string bytes = RpcCodec::Encode(std::move(meta), {});
    if (!bytes.empty())
    {
        conn->Send(bytes);
    }
}

void RpcServer::SendHeartbeatEcho(const net::TcpConnectionPtr& conn,
                                  const proto::RpcMeta& request_meta)
{
    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::HEARTBEAT);
    meta.set_request_id(request_meta.request_id());
    meta.set_trace_id(request_meta.trace_id());

    const std::string bytes = RpcCodec::Encode(std::move(meta), {});

    if (!bytes.empty())
    {
        conn->Send(bytes);
    }
}

}  // namespace nebula::rpc
