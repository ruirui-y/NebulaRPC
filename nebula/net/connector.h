#pragma once

#include "nebula/base/noncopyable.h"
#include "nebula/net/timer_id.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

namespace nebula::net
{

class Channel;
class EventLoop;

class Connector final : public std::enable_shared_from_this<Connector>,
                        private base::Noncopyable {
public:
    using NewConnectionCallback = std::function<void(int)>;
    using ErrorCallback = std::function<void(const std::string&)>;

    // 重试节奏：起步 initial，每次乘 multiplier，封顶 max
    struct ReconnectPolicy
    {
        std::chrono::milliseconds initial{100};
        std::chrono::milliseconds max{5000};
        std::uint32_t multiplier{2};
    };

    Connector(EventLoop* loop, std::string ip, std::uint16_t port);
    ~Connector();

    void SetNewConnectionCallback(NewConnectionCallback cb)
    {
        new_connection_callback_ = std::move(cb);
    }
    void SetErrorCallback(ErrorCallback cb)
    {
        error_callback_ = std::move(cb);
    }
    void SetReconnectPolicy(ReconnectPolicy policy)
    {
        reconnect_policy_ = policy;
    }

    void Start();
    void Stop();

    // 已建连接掉线后由外部报丧调用：把状态带回起点重新开始，不改 connect_ 意图
    void Restart();

    // 已排入的退避重试次数，供验收观测
    [[nodiscard]] std::uint64_t ReconnectAttemptCount() const noexcept
    {
        return reconnect_attempt_count_.load();
    }

private:
    enum class State
    {
        kDisconnected,
        kConnecting,
        kConnected,
    };

    void StartInLoop();
    void StopInLoop();
    void RestartInLoop();
    void ConnectInLoop();
    void Connecting(int socket_fd);
    void HandleWrite();
    void HandleError();
    void ScheduleReconnect();
    void CancelReconnect();

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

    ReconnectPolicy reconnect_policy_;
    std::chrono::milliseconds next_backoff_{};   // 0 表示下一轮从 initial 起步
    TimerId retry_timer_;
    std::atomic_uint64_t reconnect_attempt_count_{0};

    NewConnectionCallback new_connection_callback_;
    ErrorCallback error_callback_;
};

}  // namespace nebula::net
