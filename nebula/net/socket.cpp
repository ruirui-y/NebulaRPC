#include "nebula/net/socket.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace nebula::net {

Socket::~Socket() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

int Socket::CreateNonblocking() {
    const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (fd < 0) {
        throw std::runtime_error("socket failed: " + std::string(std::strerror(errno)));
    }
    return fd;
}

void Socket::BindAddress(const std::string& ip, std::uint16_t port) const {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);

    if (::inet_pton(AF_INET, ip.c_str(), &address.sin_addr) != 1) {
        throw std::invalid_argument("invalid IPv4 address: " + ip);
    }

    if (::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        throw std::runtime_error("bind failed: " + std::string(std::strerror(errno)));
    }
}

void Socket::Listen() const {
    if (::listen(fd_, SOMAXCONN) < 0) {
        throw std::runtime_error("listen failed: " + std::string(std::strerror(errno)));
    }
}

int Socket::Accept() const {
    sockaddr_in peer{};
    socklen_t length = sizeof(peer);
    return ::accept4(fd_, reinterpret_cast<sockaddr*>(&peer), &length, SOCK_NONBLOCK | SOCK_CLOEXEC);
}

void Socket::ShutdownWrite() const {
    ::shutdown(fd_, SHUT_WR);
}

void Socket::SetReuseAddr(bool on) const {
    const int value = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(value));
}

void Socket::SetReusePort(bool on) const {
#ifdef SO_REUSEPORT
    const int value = on ? 1 : 0;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &value, sizeof(value));
#else
    (void)on;
#endif
}

void Socket::SetTcpNoDelay(bool on) const {
    const int value = on ? 1 : 0;
    ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
}

}  // namespace nebula::net
