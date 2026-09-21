#pragma once

#include <arpa/inet.h>
#include <sys/uio.h>

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace nebula::net {

class Buffer {
public:
    static constexpr std::size_t kCheapPrepend = 8;
    static constexpr std::size_t kInitialSize = 1024;

    explicit Buffer(std::size_t initial_size = kInitialSize)
        : buffer_(kCheapPrepend + initial_size),
          read_index_(kCheapPrepend),
          write_index_(kCheapPrepend) {}

    [[nodiscard]] std::size_t ReadableBytes() const noexcept {
        return write_index_ - read_index_;
    }

    [[nodiscard]] std::size_t WritableBytes() const noexcept {
        return buffer_.size() - write_index_;
    }

    [[nodiscard]] std::size_t PrependableBytes() const noexcept {
        return read_index_;
    }

    [[nodiscard]] const char* Peek() const noexcept {
        return Begin() + read_index_;
    }

    void Retrieve(std::size_t len) {
        assert(len <= ReadableBytes());
        if (len < ReadableBytes()) {
            read_index_ += len;
        } else {
            RetrieveAll();
        }
    }

    void RetrieveAll() noexcept {
        read_index_ = kCheapPrepend;
        write_index_ = kCheapPrepend;
    }

    [[nodiscard]] std::string RetrieveAsString(std::size_t len) {
        assert(len <= ReadableBytes());
        std::string result(Peek(), len);
        Retrieve(len);
        return result;
    }

    [[nodiscard]] std::string RetrieveAllAsString() {
        return RetrieveAsString(ReadableBytes());
    }

    void Append(const char* data, std::size_t len) {
        EnsureWritableBytes(len);
        std::copy(data, data + len, BeginWrite());
        HasWritten(len);
    }

    void Append(std::string_view data) {
        Append(data.data(), data.size());
    }

    void AppendUInt32(std::uint32_t value) {
        const std::uint32_t be32 = htonl(value);
        Append(reinterpret_cast<const char*>(&be32), sizeof(be32));
    }

    [[nodiscard]] std::uint32_t PeekUInt32(std::size_t offset = 0) const {
        assert(ReadableBytes() >= offset + sizeof(std::uint32_t));
        std::uint32_t be32 = 0;
        std::memcpy(&be32, Peek() + offset, sizeof(be32));
        return ntohl(be32);
    }

    [[nodiscard]] std::uint32_t RetrieveUInt32() {
        const std::uint32_t value = PeekUInt32();
        Retrieve(sizeof(std::uint32_t));
        return value;
    }

    void EnsureWritableBytes(std::size_t len) {
        if (WritableBytes() < len) {
            MakeSpace(len);
        }
    }

    [[nodiscard]] char* BeginWrite() noexcept {
        return Begin() + write_index_;
    }

    void HasWritten(std::size_t len) {
        assert(len <= WritableBytes());
        write_index_ += len;
    }

    ssize_t ReadFd(int fd, int* saved_errno) {
        char extra_buffer[65536];
        iovec vec[2];
        const std::size_t writable = WritableBytes();

        vec[0].iov_base = Begin() + write_index_;
        vec[0].iov_len = writable;
        vec[1].iov_base = extra_buffer;
        vec[1].iov_len = sizeof(extra_buffer);

        const int iov_count = writable < sizeof(extra_buffer) ? 2 : 1;
        const ssize_t n = ::readv(fd, vec, iov_count);
        if (n < 0) {
            if (saved_errno != nullptr) {
                *saved_errno = errno;
            }
            return n;
        }

        const auto bytes_read = static_cast<std::size_t>(n);
        if (bytes_read <= writable) {
            write_index_ += bytes_read;
        } else {
            write_index_ = buffer_.size();
            Append(extra_buffer, bytes_read - writable);
        }
        return n;
    }

private:
    [[nodiscard]] char* Begin() noexcept { return buffer_.data(); }
    [[nodiscard]] const char* Begin() const noexcept { return buffer_.data(); }

    void MakeSpace(std::size_t len) {
        if (WritableBytes() + PrependableBytes() < len + kCheapPrepend) {
            buffer_.resize(write_index_ + len);
            return;
        }

        const std::size_t readable = ReadableBytes();
        std::copy(Begin() + read_index_, Begin() + write_index_, Begin() + kCheapPrepend);
        read_index_ = kCheapPrepend;
        write_index_ = read_index_ + readable;
    }

    std::vector<char> buffer_;
    std::size_t read_index_;
    std::size_t write_index_;
};

}  // namespace nebula::net
