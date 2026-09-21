#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/channel.h"
#include "nebula/net/socket.h"

#include <cstdint>
#include <functional>
#include <string>

namespace nebula::net {

class EventLoop;

class Acceptor final : private base::Noncopyable {
public:
    using NewConnectionCallback = std::function<void(int)>;

    Acceptor(EventLoop* loop, std::string ip, std::uint16_t port, bool reuse_port);
    ~Acceptor() = default;

    void SetNewConnectionCallback(NewConnectionCallback cb) {
        new_connection_callback_ = std::move(cb);
    }

    [[nodiscard]] bool Listening() const noexcept { return listening_; }
    void Listen();

private:
    void HandleRead();

    EventLoop* loop_;
    Socket accept_socket_;
    Channel accept_channel_;
    NewConnectionCallback new_connection_callback_;
    bool listening_{false};
};

}  // namespace nebula::net
