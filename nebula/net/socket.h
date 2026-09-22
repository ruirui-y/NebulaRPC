#pragma once

#include "nebula/base/noncopyable.h"

#include <cstdint>
#include <string>

namespace nebula::net
{

class Socket final : private base::Noncopyable
{
public:
    explicit Socket(int fd) : fd_(fd)
    {
    }
    ~Socket();

    [[nodiscard]] int Fd() const noexcept
    {
        return fd_;
    }

    static int CreateNonblocking();
    void BindAddress(const std::string& ip, std::uint16_t port) const;
    void Listen() const;
    [[nodiscard]] int Accept() const;
    void ShutdownWrite() const;
    void SetReuseAddr(bool on) const;
    void SetReusePort(bool on) const;
    void SetTcpNoDelay(bool on) const;

private:
    int fd_;
};

}  // namespace nebula::net
