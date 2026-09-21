#include "nebula/rpc/rpc_server.h"

#include "nebula/rpc/rpc_closure.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <iostream>
#include <memory>
#include <utility>

namespace nebula::rpc {

struct RpcServer::ServerCall {
    std::unique_ptr<google::protobuf::Message> request;
    std::unique_ptr<google::protobuf::Message> response;
};

RpcServer::RpcServer(net::EventLoop* loop, std::string ip, std::uint16_t port)
    : server_(loop, std::move(ip), port) {
    server_.SetMessageCallback(
        [this](const net::TcpConnectionPtr& conn, net::Buffer* buffer) { OnMessage(conn, buffer); });
}

void RpcServer::RegisterService(google::protobuf::Service* service) {
    if (service == nullptr) {
        return;
    }
    const auto* descriptor = service->GetDescriptor();
    services_[descriptor->full_name()] = service;
}

void RpcServer::Start(std::size_t io_thread_count) {
    server_.Start(io_thread_count);
}

void RpcServer::OnMessage(const net::TcpConnectionPtr& conn, net::Buffer* buffer) {
    while (true) {
        RpcFrame frame;
        std::string error;
        const auto result = RpcCodec::Decode(buffer, &frame, &error);
        if (result == RpcCodec::DecodeResult::kNeedMore) {
            return;
        }
        if (result == RpcCodec::DecodeResult::kError) {
            std::cerr << "RPC decode error: " << error << '\n';
            conn->Shutdown();
            return;
        }

        if (frame.meta.type() != proto::RpcMeta::REQUEST) {
            SendError(conn, frame.meta.request_id(), 400, "server received a non-request frame");
            continue;
        }
        HandleRequest(conn, frame);
    }
}

void RpcServer::HandleRequest(const net::TcpConnectionPtr& conn, const RpcFrame& frame) {
    const auto service_it = services_.find(frame.meta.service_name());
    if (service_it == services_.end()) {
        SendError(conn, frame.meta.request_id(), 404, "service not found");
        return;
    }

    google::protobuf::Service* service = service_it->second;
    const auto* method = service->GetDescriptor()->FindMethodByName(frame.meta.method_name());
    if (method == nullptr) {
        SendError(conn, frame.meta.request_id(), 404, "method not found");
        return;
    }

    auto call = std::make_shared<ServerCall>();
    call->request.reset(service->GetRequestPrototype(method).New());
    call->response.reset(service->GetResponsePrototype(method).New());

    if (!call->request->ParseFromString(frame.payload)) {
        SendError(conn, frame.meta.request_id(), 400, "request protobuf parse failed");
        return;
    }

    const std::uint64_t request_id = frame.meta.request_id();
    auto* done = new RpcClosure([this, conn, call, request_id] {
        SendResponse(conn, request_id, *call->response);
    });

    service->CallMethod(method, nullptr, call->request.get(), call->response.get(), done);
}

void RpcServer::SendResponse(const net::TcpConnectionPtr& conn,
                             std::uint64_t request_id,
                             const google::protobuf::Message& response) {
    std::string payload;
    if (!response.SerializeToString(&payload)) {
        SendError(conn, request_id, 500, "response protobuf serialization failed");
        return;
    }

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::RESPONSE);
    meta.set_request_id(request_id);
    const std::string bytes = RpcCodec::Encode(std::move(meta), payload);
    if (bytes.empty()) {
        SendError(conn, request_id, 500, "response frame encoding failed");
        return;
    }
    conn->Send(bytes);
}

void RpcServer::SendError(const net::TcpConnectionPtr& conn,
                          std::uint64_t request_id,
                          int error_code,
                          std::string error_text) {
    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::ERROR);
    meta.set_request_id(request_id);
    meta.set_error_code(error_code);
    meta.set_error_text(std::move(error_text));
    const std::string bytes = RpcCodec::Encode(std::move(meta), {});
    if (!bytes.empty()) {
        conn->Send(bytes);
    }
}

}  // namespace nebula::rpc
