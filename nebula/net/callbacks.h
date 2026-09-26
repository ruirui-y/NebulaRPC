#pragma once

#include <cstddef>
#include <functional>
#include <memory>

namespace nebula::net
{

class Buffer;
class TcpConnection;
using TcpConnectionPtr = std::shared_ptr<TcpConnection>;

using ConnectionCallback = std::function<void(const TcpConnectionPtr&)>;
using MessageCallback = std::function<void(const TcpConnectionPtr&, Buffer*)>;
using WriteCompleteCallback = std::function<void(const TcpConnectionPtr&)>;
using CloseCallback = std::function<void(const TcpConnectionPtr&)>;
// 第二个参数是越过水位时的待发字节数
using HighWatermarkCallback = std::function<void(const TcpConnectionPtr&, std::size_t)>;

}  // namespace nebula::net
