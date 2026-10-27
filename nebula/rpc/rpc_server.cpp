#include "nebula/rpc/rpc_server.h"

#include "nebula/base/logger.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/rpc/rpc_closure.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <chrono>
#include <exception>
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

// RpcServer 只决定 Flush 边界，不再保存响应字节。
// 同步完成的响应全部 Write 到 TcpConnection，OnMessage 结束时统一 Flush。
// 异步完成若已经错过本轮，则在所属连接 EventLoop 中 Write + Flush。
struct RpcServer::ResponseFlushScope final
    : public std::enable_shared_from_this<ResponseFlushScope>
{
    ResponseFlushScope(RpcServer* server, net::TcpConnectionPtr connection)
        : server_(server),
          conn_(std::move(connection))
    {
    }

    void Write(std::string bytes, bool finish_call)
    {
        if (conn_->Loop()->IsInLoopThread())
        {
            WriteInLoop(std::move(bytes), finish_call);
            return;
        }

        auto self = shared_from_this();
        conn_->Loop()->QueueInLoop([self, bytes = std::move(bytes), finish_call]() mutable
            {
                self->WriteInLoop(std::move(bytes), finish_call);
            });
    }

    void Close()
    {
        conn_->Loop()->AssertInLoopThread();
        if (!collecting_)
        {
            return;
        }

        collecting_ = false;

        // 当前 OnMessage 内所有同步响应已经入 TcpConnection 队列，现在由 RPC 层明确结束批次。
        conn_->Flush();

        if (finished_calls_ != 0U)
        {
            server_->OnCallsFinished(finished_calls_);
            finished_calls_ = 0;
        }
    }

private:
    void WriteInLoop(std::string bytes, bool finish_call)
    {
        conn_->Loop()->AssertInLoopThread();

        if (!bytes.empty())
        {
            conn_->Write(std::move(bytes));
        }

        if (collecting_)
        {
            if (finish_call)
            {
                ++finished_calls_;
            }
            return;
        }

        // 异步业务完成发生在 OnMessage 批次结束之后，不能等下一次读事件帮它发送。
        conn_->Flush();

        if (finish_call)
        {
            server_->OnCallsFinished(1);
        }
    }

    RpcServer* server_;
    net::TcpConnectionPtr conn_;
    std::uint64_t finished_calls_{0};
    bool collecting_{true};
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

void RpcServer::RegisterCoroutineService(CoroutineService* service)
{
    if (service == nullptr || service->Descriptor() == nullptr)
    {
        return;
    }

    coroutine_services_[service->Descriptor()->full_name()] = service;
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
        return;
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

    // 用 Shutdown 而非 ForceClose：TcpConnection 会先 Flush 已入队数据，再发送 FIN。
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

void RpcServer::OnCallsFinished(std::uint64_t count)
{
    if (count == 0U)
    {
        return;
    }

    // 同步批次在 Flush 后一次回收；异步完成在 Write + Flush 后单独回收。
    const auto previous = in_flight_.fetch_sub(count, std::memory_order_acq_rel);
    if (previous == count && stopping_.load())
    {
        loop_->RunInLoop([this]
            {
                CloseConnectionsInLoop();
            });
    }
}

void RpcServer::OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer)
{
    auto flush_scope = std::make_shared<ResponseFlushScope>(this, conn);

    while (true)
    {
        RpcFrame frame;
        std::string error;
        const auto result = RpcCodec::Decode(buffer, &frame, &error);

        if (result == RpcCodec::DecodeResult::kNeedMore)
        {
            flush_scope->Close();
            return;
        }

        if (result == RpcCodec::DecodeResult::kError)
        {
            flush_scope->Close();
            NLOG_ERROR("rpc protocol error name={} detail={}", conn->Name(), error);
            conn->Shutdown();
            return;
        }

        if (frame.meta.type() == proto::RpcMeta::HEARTBEAT)
        {
            SendHeartbeatEcho(frame.meta, flush_scope);
            continue;
        }

        if (frame.meta.type() == proto::RpcMeta::REQUEST)
        {
            HandleRequest(conn, frame, flush_scope);
            continue;
        }

        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::BadRequest,
                  "server received a non-request/heartbeat frame",
                  flush_scope);
    }
}

void RpcServer::HandleRequest(const net::TcpConnectionPtr& conn,
                              const RpcFrame& frame,
                              const std::shared_ptr<ResponseFlushScope>& flush_scope)
{
    const auto coroutine_service_it = coroutine_services_.find(frame.meta.service_name());
    if (coroutine_service_it != coroutine_services_.end())
    {
        HandleCoroutineRequest(conn, frame, coroutine_service_it->second, flush_scope);
        return;
    }

    const auto service_it = services_.find(frame.meta.service_name());
    if (service_it == services_.end())
    {
        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::NotFound,
                  "service not found",
                  flush_scope);
        return;
    }

    google::protobuf::Service* service = service_it->second;
    const auto* descriptor = service->GetDescriptor();
    const std::uint32_t method_index = frame.meta.method_index();
    if (method_index >= static_cast<std::uint32_t>(descriptor->method_count()))
    {
        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::NotFound,
                  "method not found",
                  flush_scope);
        return;
    }

    const auto* method = descriptor->method(static_cast<int>(method_index));

    auto call = std::make_shared<ServerCall>();
    call->request.reset(service->GetRequestPrototype(method).New());
    call->response.reset(service->GetResponsePrototype(method).New());

    if (!call->request->ParseFromString(frame.payload))
    {
        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::BadRequest,
                  "request protobuf parse failed",
                  flush_scope);
        return;
    }

    const std::uint64_t request_id = frame.meta.request_id();
    const std::uint64_t trace_id = frame.meta.trace_id();

    NLOG_DEBUG("rpc server dispatch request_id={} trace_id={} service={} method={} name={}",
               request_id,
               trace_id,
               frame.meta.service_name(),
               method->name(),
               conn->Name());

    // 先计数再调用：业务可能同步回调 done，计数晚了就会在清零之后又减一次
    in_flight_.fetch_add(1, std::memory_order_relaxed);

    auto* done = new RpcClosure([this, call, request_id, trace_id, flush_scope]
        {
            SendResponse(request_id, trace_id, *call->response, flush_scope);
        });

    service->CallMethod(method, nullptr, call->request.get(), call->response.get(), done);
}

void RpcServer::HandleCoroutineRequest(
    const net::TcpConnectionPtr& conn,
    const RpcFrame& frame,
    CoroutineService* service,
    const std::shared_ptr<ResponseFlushScope>& flush_scope)
{
    const CoroutineService::Method* method = service->FindMethod(frame.meta.method_index());
    if (method == nullptr)
    {
        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::NotFound,
                  "coroutine method not found",
                  flush_scope);
        return;
    }

    std::unique_ptr<google::protobuf::Message> request = method->new_request();
    if (request == nullptr || !request->ParseFromString(frame.payload))
    {
        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::BadRequest,
                  "request protobuf parse failed",
                  flush_scope);
        return;
    }

    const std::uint64_t request_id = frame.meta.request_id();
    const std::uint64_t trace_id = frame.meta.trace_id();

    NLOG_DEBUG("rpc coroutine dispatch request_id={} trace_id={} service={} method={} name={}",
               request_id,
               trace_id,
               frame.meta.service_name(),
               method->descriptor->name(),
               conn->Name());

    // DetachedTask 会立刻执行到第一个挂起点。
    // 若 handler 同步完成，响应仍属于当前 OnMessage 批次；若挂起后再完成，
    // ResponseFlushScope 会在 collecting=false 时自动 Write + Flush。
    in_flight_.fetch_add(1, std::memory_order_relaxed);

    RpcServerContext context(
        conn->Loop(),
        request_id,
        trace_id,
        conn->Name());

    DispatchCoroutineRequest(
        method,
        std::move(context),
        std::move(request),
        flush_scope);
}

DetachedTask RpcServer::DispatchCoroutineRequest(
    const CoroutineService::Method* method,
    RpcServerContext context,
    std::unique_ptr<google::protobuf::Message> request,
    std::shared_ptr<ResponseFlushScope> flush_scope)
{
    const std::uint64_t request_id = context.RequestId();
    const std::uint64_t trace_id = context.TraceId();

    try
    {
        std::unique_ptr<google::protobuf::Message> response =
            co_await method->invoke(std::move(context), std::move(request));

        if (response == nullptr)
        {
            SendError(request_id,
                      trace_id,
                      RpcErrorCode::InternalError,
                      "coroutine handler returned a null response",
                      flush_scope);
            flush_scope->Write({}, true);
            co_return;
        }

        SendResponse(request_id, trace_id, *response, flush_scope);
    }
    catch (const RpcServiceError& error)
    {
        SendError(request_id,
                  trace_id,
                  error.Code(),
                  error.what(),
                  flush_scope);
        flush_scope->Write({}, true);
    }
    catch (const RpcError& error)
    {
        // handler 内部调用下游 RPC 失败但没有自行处理时，向上游统一映射为服务端错误。
        SendError(request_id,
                  trace_id,
                  RpcErrorCode::InternalError,
                  error.what(),
                  flush_scope);
        flush_scope->Write({}, true);
    }
    catch (const std::exception& error)
    {
        SendError(request_id,
                  trace_id,
                  RpcErrorCode::InternalError,
                  error.what(),
                  flush_scope);
        flush_scope->Write({}, true);
    }
    catch (...)
    {
        SendError(request_id,
                  trace_id,
                  RpcErrorCode::InternalError,
                  "unknown coroutine handler exception",
                  flush_scope);
        flush_scope->Write({}, true);
    }
}

void RpcServer::SendResponse(std::uint64_t request_id,
                             std::uint64_t trace_id,
                             const google::protobuf::Message& response,
                             const std::shared_ptr<ResponseFlushScope>& flush_scope)
{
    std::string payload;
    if (!response.SerializeToString(&payload))
    {
        SendError(request_id,
                  trace_id,
                  RpcErrorCode::InternalError,
                  "response protobuf serialization failed",
                  flush_scope);
        flush_scope->Write({}, true);
        return;
    }

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::RESPONSE);
    meta.set_request_id(request_id);
    meta.set_trace_id(trace_id);

    std::string bytes = RpcCodec::Encode(std::move(meta), payload);
    if (bytes.empty())
    {
        SendError(request_id,
                  trace_id,
                  RpcErrorCode::InternalError,
                  "response frame encoding failed",
                  flush_scope);
        flush_scope->Write({}, true);
        return;
    }

    flush_scope->Write(std::move(bytes), true);
}

void RpcServer::SendError(std::uint64_t request_id,
                          std::uint64_t trace_id,
                          RpcErrorCode error_code,
                          std::string error_text,
                          const std::shared_ptr<ResponseFlushScope>& flush_scope)
{
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

    std::string bytes = RpcCodec::Encode(std::move(meta), {});
    if (!bytes.empty())
    {
        flush_scope->Write(std::move(bytes), false);
    }
}

void RpcServer::SendHeartbeatEcho(const proto::RpcMeta& request_meta,
                                  const std::shared_ptr<ResponseFlushScope>& flush_scope)
{
    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::HEARTBEAT);
    meta.set_request_id(request_meta.request_id());
    meta.set_trace_id(request_meta.trace_id());

    std::string bytes = RpcCodec::Encode(std::move(meta), {});
    if (!bytes.empty())
    {
        flush_scope->Write(std::move(bytes), false);
    }
}

}  // namespace nebula::rpc
