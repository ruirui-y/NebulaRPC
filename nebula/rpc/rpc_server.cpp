#include "nebula/rpc/rpc_server.h"

#include "nebula/base/logger.h"
#include "nebula/net/event_loop.h"
#include "nebula/net/tcp_connection.h"
#include "nebula/rpc/rpc_closure.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <chrono>
#include <memory>
#include <mutex>
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

// 一次 OnMessage 调用对应一个批次。
// 同步完成的响应先聚合；OnMessage 结束后一次性交给 TcpConnection。
// 如果业务异步完成、错过当前批次，则自动退回原来的单响应发送路径。
struct RpcServer::ResponseBatch
{
    ResponseBatch(RpcServer* owner, net::TcpConnectionPtr connection)
        : owner_(owner),
          conn_(std::move(connection))
    {
    }

    void Enqueue(std::string bytes, bool finish_call)
    {
        std::string direct;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (collecting)
            {
                if (!bytes.empty())
                {
                    output.append(bytes);
                }
                if (finish_call)
                {
                    ++finished_calls;
                }
                return;
            }

            direct = std::move(bytes);
        }

        if (!direct.empty())
        {
            conn_->Send(direct);
        }
        if (finish_call)
        {
            owner_->OnCallsFinished(1);
        }
    }

    void FlushAndClose()
    {
        std::string bytes;
        std::uint64_t calls = 0;

        {
            std::lock_guard<std::mutex> lock(mutex);
            if (!collecting)
            {
                return;
            }

            collecting = false;
            bytes.swap(output);
            calls = finished_calls;
            finished_calls = 0;
        }

        if (!bytes.empty())
        {
            conn_->Send(bytes);
        }
        if (calls != 0U)
        {
            owner_->OnCallsFinished(calls);
        }
    }

    RpcServer* owner_;
    net::TcpConnectionPtr conn_;
    std::mutex mutex;
    std::string output;
    std::uint64_t finished_calls{0};
    bool collecting{true};
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

void RpcServer::OnCallsFinished(std::uint64_t count)
{
    if (count == 0U)
    {
        return;
    }

    // 响应真正交给 TcpConnection 后再减少 in-flight。
    // 批量响应可一次完成多个调用，异步响应仍按 1 个调用回收。
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
    auto batch = std::make_shared<ResponseBatch>(this, conn);

    while (true)
    {
        RpcFrame frame;
        std::string error;
        const auto result = RpcCodec::Decode(buffer, &frame, &error);

        if (result == RpcCodec::DecodeResult::kNeedMore)
        {
            // 当前 input buffer 中的完整帧已经全部处理完。
            // 把本轮同步生成的多个响应合并成一次 Send。
            batch->FlushAndClose();
            return;
        }

        if (result == RpcCodec::DecodeResult::kError)
        {
            // 前面合法帧已经生成的响应先交给连接，再关闭协议错误连接。
            batch->FlushAndClose();
            NLOG_ERROR("rpc protocol error name={} detail={}", conn->Name(), error);
            conn->Shutdown();
            return;
        }

        if (frame.meta.type() == proto::RpcMeta::HEARTBEAT)
        {
            SendHeartbeatEcho(frame.meta, batch);
            continue;
        }

        if (frame.meta.type() == proto::RpcMeta::REQUEST)
        {
            HandleRequest(conn, frame, batch);
            continue;
        }

        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::BadRequest,
                  "server received a non-request/heartbeat frame",
                  batch);
    }
}

void RpcServer::HandleRequest(const net::TcpConnectionPtr& conn,
                              const RpcFrame& frame,
                              const std::shared_ptr<ResponseBatch>& batch)
{
    const auto service_it = services_.find(frame.meta.service_name());
    if (service_it == services_.end())
    {
        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::NotFound,
                  "service not found",
                  batch);
        return;
    }

    google::protobuf::Service* service = service_it->second;
    const auto* method = service->GetDescriptor()->FindMethodByName(frame.meta.method_name());
    if (method == nullptr)
    {
        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::NotFound,
                  "method not found",
                  batch);
        return;
    }

    auto call = std::make_shared<ServerCall>();
    call->request.reset(service->GetRequestPrototype(method).New());
    call->response.reset(service->GetResponsePrototype(method).New());

    if (!call->request->ParseFromString(frame.payload))
    {
        SendError(frame.meta.request_id(),
                  frame.meta.trace_id(),
                  RpcErrorCode::BadRequest,
                  "request protobuf parse failed",
                  batch);
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

    auto* done = new RpcClosure([this, call, request_id, trace_id, batch]
        {
            SendResponse(request_id, trace_id, *call->response, batch);
        });

    service->CallMethod(method, nullptr, call->request.get(), call->response.get(), done);
}

void RpcServer::SendResponse(std::uint64_t request_id,
                             const std::string& trace_id,
                             const google::protobuf::Message& response,
                             const std::shared_ptr<ResponseBatch>& batch)
{
    std::string payload;
    if (!response.SerializeToString(&payload))
    {
        SendError(request_id,
                  trace_id,
                  RpcErrorCode::InternalError,
                  "response protobuf serialization failed",
                  batch);
        batch->Enqueue({}, true);
        return;
    }

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::RESPONSE);
    meta.set_request_id(request_id);
    meta.set_trace_id(trace_id);
    const std::string bytes = RpcCodec::Encode(std::move(meta), payload);
    if (bytes.empty())
    {
        SendError(request_id,
                  trace_id,
                  RpcErrorCode::InternalError,
                  "response frame encoding failed",
                  batch);
        batch->Enqueue({}, true);
        return;
    }
    batch->Enqueue(bytes, true);
}

void RpcServer::SendError(std::uint64_t request_id,
                          const std::string& trace_id,
                          RpcErrorCode error_code,
                          std::string error_text,
                          const std::shared_ptr<ResponseBatch>& batch)
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
        batch->Enqueue(bytes, false);
    }
}

void RpcServer::SendHeartbeatEcho(const proto::RpcMeta& request_meta,
                                  const std::shared_ptr<ResponseBatch>& batch)
{
    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::HEARTBEAT);
    meta.set_request_id(request_meta.request_id());
    meta.set_trace_id(request_meta.trace_id());

    const std::string bytes = RpcCodec::Encode(std::move(meta), {});

    if (!bytes.empty())
    {
        batch->Enqueue(bytes, false);
    }
}

}  // namespace nebula::rpc
