#pragma once

#include "nebula/base/noncopyable.h"

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace nebula::net {

class Channel;
class EventLoop;

class Connector final : public std::enable_shared_from_this<Connector>,
                        private base::Noncopyable {
public:
    using NewConnectionCallback = std::function<void(int)>;
    using ErrorCallback = std::function<void(const std::string&)>;

    Connector(EventLoop* loop, std::string ip, std::uint16_t port);
    ~Connector();

    void SetNewConnectionCallback(NewConnectionCallback cb) {
        new_connection_callback_ = std::move(cb);
    }
    void SetErrorCallback(ErrorCallback cb) {
        error_callback_ = std::move(cb);
    }

    void Start();
    void Stop();

private:
    enum class State {
        kDisconnected,
        kConnecting,
        kConnected,
    };

    void StartInLoop();
    void StopInLoop();
    void ConnectInLoop();
    void Connecting(int socket_fd);
    void HandleWrite();
    void HandleError();

    [[nodiscard]] int RemoveAndResetChannel();
    void ResetChannel();
    void ReportError(const std::string& reason);
    [[nodiscard]] static int GetSocketError(int socket_fd);

    EventLoop* loop_;
    std::string ip_;
    std::uint16_t port_;

    std::atomic_bool connect_{false};
    State state_{State::kDisconnected};

    // 连接完成前由 Connector 持有，成功后移交给 TcpConnection。
    int socket_fd_{-1};
    std::unique_ptr<Channel> channel_;

    NewConnectionCallback new_connection_callback_;
    ErrorCallback error_callback_;
};

}  // namespace nebula::net
