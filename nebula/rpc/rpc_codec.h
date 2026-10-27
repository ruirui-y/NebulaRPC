#pragma once

#include "nebula/net/buffer.h"
#include "rpc_meta.pb.h"

#include <cstddef>
#include <string>
#include <string_view>

namespace google::protobuf
{
class MessageLite;
}

namespace nebula::rpc
{

struct RpcFrame
{
    proto::RpcMeta meta;
    std::string_view payload;                                               // 非 owning，底层 frame bytes 必须存活到 payload 消费完毕
    std::size_t wire_size{0};                                               // Decode 成功时，当前完整 frame 在 input_buffer 中占用的总字节数
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

    // 单帧最大 16 MiB，防止异常长度导致过量内存占用。
    static constexpr std::uint32_t kMaxFrameSize = 16U * 1024U * 1024U;

    // Wire Format：
    //
    // +-------------------+
    // | frame_size   4B   |  后续 body 总长度，不包含自身 4B
    // +-------------------+
    // | meta_size    4B   |  RpcMeta 序列化后的长度
    // +-------------------+
    // | RpcMeta           |  meta_size 字节
    // +-------------------+
    // | payload           |  业务 protobuf 数据
    // +-------------------+
    //
    // frame_size = 4B(meta_size) + meta_size + payload_size

    [[nodiscard]] static std::string Encode(proto::RpcMeta meta, std::string_view payload);

    [[nodiscard]] static std::string EncodeMessage(proto::RpcMeta meta, const google::protobuf::MessageLite& payload);

    // Decode 只建立 payload view，调用者在 payload 消费完后再 Retrieve(frame.wire_size)。
    static DecodeResult Decode(net::Buffer* buffer, RpcFrame* frame, std::string* error);
    static bool DecodeBody(std::string_view body, RpcFrame* frame, std::string* error);
};

}  // namespace nebula::rpc
