#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/rpc/rpc_codec.h"

#include <google/protobuf/service.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

namespace nebula::rpc {

class RpcChannel final : public google::protobuf::RpcChannel,
                         private base::Noncopyable {
public:
    RpcChannel(std::string ip,
               std::uint16_t port,
               std::chrono::milliseconds timeout = std::chrono::seconds(5));
    ~RpcChannel() override;

    void CallMethod(const google::protobuf::MethodDescriptor* method,
                    google::protobuf::RpcController* controller,
                    const google::protobuf::Message* request,
                    google::protobuf::Message* response,
                    google::protobuf::Closure* done) override;

private:
    using PendingPromise = std::promise<RpcFrame>;

    void Connect();
    void ReceiverLoop();
    [[nodiscard]] bool SendAll(std::string_view bytes);
    [[nodiscard]] bool RecvExactly(void* data, std::size_t size);
    void FailAllPending(const std::string& reason);

    std::string ip_;
    std::uint16_t port_;
    std::chrono::milliseconds timeout_;
    int socket_fd_{-1};
    std::atomic_bool running_{false};
    std::thread receiver_thread_;

    std::mutex write_mutex_;
    std::mutex pending_mutex_;
    std::unordered_map<std::uint64_t, std::shared_ptr<PendingPromise>> pending_calls_;

    inline static std::atomic_uint64_t next_request_id_{1};
};

}  // namespace nebula::rpc
