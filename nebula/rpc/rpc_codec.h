#pragma once

#include "nebula/net/buffer.h"
#include "rpc_meta.pb.h"

#include <string>
#include <string_view>

namespace nebula::rpc
{

struct RpcFrame
{
    proto::RpcMeta meta;
    std::string payload;
};

class RpcCodec
{
public:
    enum class DecodeResult
    {
        kNeedMore,
        kOk,
        kError,
    };

    static constexpr std::uint32_t kMaxFrameSize = 16U * 1024U * 1024U;

    [[nodiscard]] static std::string Encode(proto::RpcMeta meta, std::string_view payload);
    static DecodeResult Decode(net::Buffer* buffer, RpcFrame* frame, std::string* error);
    static bool DecodeBody(std::string_view body, RpcFrame* frame, std::string* error);
};

}  // namespace nebula::rpc
