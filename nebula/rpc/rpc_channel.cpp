#include "nebula/rpc/rpc_channel.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <exception>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace nebula::rpc {
namespace {

void SetControllerFailure(google::protobuf::RpcController* controller, const std::string& reason) {
    if (controller != nullptr) {
        controller->SetFailed(reason);
    }
}

}  // namespace

RpcChannel::RpcChannel(std::string ip,
                       std::uint16_t port,
                       std::chrono::milliseconds timeout)
    : ip_(std::move(ip)), port_(port), timeout_(timeout) {
    Connect();
    running_ = true;
    receiver_thread_ = std::thread([this] { ReceiverLoop(); });
}

RpcChannel::~RpcChannel() {
    running_ = false;
    if (socket_fd_ >= 0) {
        ::shutdown(socket_fd_, SHUT_RDWR);
    }
    if (receiver_thread_.joinable()) {
        receiver_thread_.join();
    }
    if (socket_fd_ >= 0) {
        ::close(socket_fd_);
        socket_fd_ = -1;
    }
    FailAllPending("RPC channel closed");
}

void RpcChannel::CallMethod(const google::protobuf::MethodDescriptor* method,
                            google::protobuf::RpcController* controller,
                            const google::protobuf::Message* request,
                            google::protobuf::Message* response,
                            google::protobuf::Closure* done) {
    if (method == nullptr || request == nullptr || response == nullptr) {
        SetControllerFailure(controller, "invalid RPC call arguments");
        if (done != nullptr) {
            done->Run();
        }
        return;
    }

    const std::uint64_t request_id = next_request_id_.fetch_add(1);

    std::string payload;
    if (!request->SerializeToString(&payload)) {
        SetControllerFailure(controller, "request protobuf serialization failed");
        if (done != nullptr) {
            done->Run();
        }
        return;
    }

    proto::RpcMeta meta;
    meta.set_type(proto::RpcMeta::REQUEST);
    meta.set_request_id(request_id);
    meta.set_service_name(method->service()->full_name());
    meta.set_method_name(method->name());

    const std::string bytes = RpcCodec::Encode(std::move(meta), payload);
    if (bytes.empty()) {
        SetControllerFailure(controller, "request frame encoding failed");
        if (done != nullptr) {
            done->Run();
        }
        return;
    }

    auto promise = std::make_shared<PendingPromise>();
    auto future = promise->get_future();
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_calls_.emplace(request_id, promise);
    }

    {
        std::lock_guard<std::mutex> lock(write_mutex_);
        if (!SendAll(bytes)) {
            {
                std::lock_guard<std::mutex> pending_lock(pending_mutex_);
                pending_calls_.erase(request_id);
            }
            SetControllerFailure(controller, "socket send failed: " + std::string(std::strerror(errno)));
            if (done != nullptr) {
                done->Run();
            }
            return;
        }
    }

    if (future.wait_for(timeout_) != std::future_status::ready) {
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_calls_.erase(request_id);
        }
        SetControllerFailure(controller, "RPC timeout");
        if (done != nullptr) {
            done->Run();
        }
        return;
    }

    try {
        RpcFrame frame = future.get();
        if (frame.meta.type() == proto::RpcMeta::ERROR) {
            SetControllerFailure(controller, frame.meta.error_text());
        } else if (frame.meta.type() != proto::RpcMeta::RESPONSE) {
            SetControllerFailure(controller, "unexpected RPC frame type");
        } else if (!response->ParseFromString(frame.payload)) {
            SetControllerFailure(controller, "response protobuf parse failed");
        }
    } catch (const std::exception& ex) {
        SetControllerFailure(controller, ex.what());
    }

    if (done != nullptr) {
        done->Run();
    }
}

void RpcChannel::Connect() {
    socket_fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    if (socket_fd_ < 0) {
        throw std::runtime_error("socket failed: " + std::string(std::strerror(errno)));
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);
    if (::inet_pton(AF_INET, ip_.c_str(), &address.sin_addr) != 1) {
        ::close(socket_fd_);
        socket_fd_ = -1;
        throw std::invalid_argument("invalid IPv4 address: " + ip_);
    }

    if (::connect(socket_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        const std::string reason = std::strerror(errno);
        ::close(socket_fd_);
        socket_fd_ = -1;
        throw std::runtime_error("connect failed: " + reason);
    }
}

void RpcChannel::ReceiverLoop() {
    while (running_.load()) {
        std::uint32_t frame_size_be = 0;
        if (!RecvExactly(&frame_size_be, sizeof(frame_size_be))) {
            break;
        }

        const std::uint32_t frame_size = ntohl(frame_size_be);
        if (frame_size < sizeof(std::uint32_t) || frame_size > RpcCodec::kMaxFrameSize) {
            break;
        }

        std::string body(frame_size, '\0');
        if (!RecvExactly(body.data(), body.size())) {
            break;
        }

        RpcFrame frame;
        std::string error;
        if (!RpcCodec::DecodeBody(body, &frame, &error)) {
            continue;
        }

        std::shared_ptr<PendingPromise> promise;
        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            const auto it = pending_calls_.find(frame.meta.request_id());
            if (it != pending_calls_.end()) {
                promise = it->second;
                pending_calls_.erase(it);
            }
        }

        if (promise != nullptr) {
            promise->set_value(std::move(frame));
        }
    }

    running_ = false;
    FailAllPending("RPC connection closed");
}

bool RpcChannel::SendAll(std::string_view bytes) {
    std::size_t sent = 0;
    while (sent < bytes.size()) {
        const ssize_t n = ::send(socket_fd_, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
        if (n > 0) {
            sent += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return true;
}

bool RpcChannel::RecvExactly(void* data, std::size_t size) {
    auto* cursor = static_cast<char*>(data);
    std::size_t received = 0;
    while (received < size && running_.load()) {
        const ssize_t n = ::recv(socket_fd_, cursor + received, size - received, 0);
        if (n > 0) {
            received += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
    return received == size;
}

void RpcChannel::FailAllPending(const std::string& reason) {
    std::unordered_map<std::uint64_t, std::shared_ptr<PendingPromise>> pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending.swap(pending_calls_);
    }

    for (auto& [request_id, promise] : pending) {
        (void)request_id;
        try {
            throw std::runtime_error(reason);
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    }
}

}  // namespace nebula::rpc
