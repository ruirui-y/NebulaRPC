#include "nebula/rpc/rpc_codec.h"

#include <arpa/inet.h>
#include <google/protobuf/message_lite.h>

#include <cstdint>
#include <cstring>
#include <limits>

namespace nebula::rpc
{
namespace
{

void SetError(std::string* error, std::string message)
{
    if (error != nullptr)
    {
        *error = std::move(message);
    }
}

}  // namespace

std::string RpcCodec::Encode(proto::RpcMeta meta, std::string_view payload)
{
    if (payload.size() > std::numeric_limits<std::uint32_t>::max())
    {
        return {};
    }

    meta.set_payload_size(static_cast<std::uint32_t>(payload.size()));
    const std::size_t meta_size = meta.ByteSizeLong();
    const std::size_t body_size = sizeof(std::uint32_t) + meta_size + payload.size();

    if (body_size > kMaxFrameSize || body_size > std::numeric_limits<std::uint32_t>::max())
    {
        return {};
    }

    const std::uint32_t frame_size_be = htonl(static_cast<std::uint32_t>(body_size));
    const std::uint32_t meta_size_be = htonl(static_cast<std::uint32_t>(meta_size));

    std::string output;
    output.reserve(sizeof(frame_size_be) + body_size);
    output.append(reinterpret_cast<const char*>(&frame_size_be), sizeof(frame_size_be));
    output.append(reinterpret_cast<const char*>(&meta_size_be), sizeof(meta_size_be));

    if (!meta.AppendToString(&output))
    {
        return {};
    }

    output.append(payload.data(), payload.size());
    return output;
}

std::string RpcCodec::EncodeMessage(proto::RpcMeta meta, const google::protobuf::MessageLite& payload)
{
    // 先计算最终尺寸，只为最终 frame 分配一次连续空间。
    const std::size_t payload_size = payload.ByteSizeLong();
    if (payload_size > std::numeric_limits<std::uint32_t>::max())
    {
        return {};
    }

    meta.set_payload_size(static_cast<std::uint32_t>(payload_size));

    const std::size_t meta_size = meta.ByteSizeLong();

    // frame_size = meta_size字段(4B) + RpcMeta + payload。
    const std::size_t body_size = sizeof(std::uint32_t) + meta_size + payload_size;

    if (body_size > kMaxFrameSize || body_size > std::numeric_limits<std::uint32_t>::max())
    {
        return {};
    }

    const std::uint32_t frame_size_be = htonl(static_cast<std::uint32_t>(body_size));
    const std::uint32_t meta_size_be = htonl(static_cast<std::uint32_t>(meta_size));

    // ByteSizeLong 已缓存序列化大小，直接构造最终 frame，避免 AppendToString 再次计算长度。
    std::string output(sizeof(frame_size_be) + body_size, '\0');
    char* data = output.data();

    // frame_size 和 meta_size 是固定 4B 头，直接写到最终缓冲的前 8 字节。
    std::memcpy(data, &frame_size_be, sizeof(frame_size_be));
    std::memcpy(data + sizeof(frame_size_be), &meta_size_be, sizeof(meta_size_be));

    // RpcMeta 紧跟在两个长度字段之后，payload 再紧跟在 RpcMeta 后面。
    auto* meta_begin = reinterpret_cast<std::uint8_t*>(data + sizeof(frame_size_be) + sizeof(meta_size_be));

    std::uint8_t* payload_begin = meta.SerializeWithCachedSizesToArray(meta_begin);
    std::uint8_t* end = payload.SerializeWithCachedSizesToArray(payload_begin);

    // 最终写入位置必须刚好落在 frame 尾部，防止尺寸计算与实际序列化结果不一致。
    if (end != reinterpret_cast<std::uint8_t*>(data + output.size()))
    {
        return {};
    }

    return output;
}

RpcCodec::DecodeResult RpcCodec::Decode(net::Buffer* buffer, RpcFrame* frame, std::string* error)
{
    if (buffer->ReadableBytes() < sizeof(std::uint32_t))
    {
        return DecodeResult::kNeedMore;
    }

    const std::uint32_t frame_size = buffer->PeekUInt32();
    if (frame_size < sizeof(std::uint32_t) || frame_size > kMaxFrameSize)
    {
        SetError(error, "invalid RPC frame size");
        return DecodeResult::kError;
    }

    const std::size_t total_size = sizeof(std::uint32_t) + static_cast<std::size_t>(frame_size);
    if (buffer->ReadableBytes() < total_size)
    {
        return DecodeResult::kNeedMore;
    }

    buffer->Retrieve(sizeof(std::uint32_t));
    const std::string body = buffer->RetrieveAsString(frame_size);
    if (!DecodeBody(body, frame, error))
    {
        return DecodeResult::kError;
    }
    return DecodeResult::kOk;
}

bool RpcCodec::DecodeBody(std::string_view body, RpcFrame* frame, std::string* error)
{
    if (body.size() < sizeof(std::uint32_t))
    {
        SetError(error, "RPC frame body is too small");
        return false;
    }

    std::uint32_t meta_size_be = 0;
    std::memcpy(&meta_size_be, body.data(), sizeof(meta_size_be));
    const std::uint32_t meta_size = ntohl(meta_size_be);

    if (meta_size > body.size() - sizeof(std::uint32_t))
    {
        SetError(error, "RPC meta size exceeds frame body");
        return false;
    }

    const std::string_view meta_bytes = body.substr(sizeof(std::uint32_t), meta_size);
    const std::string_view payload = body.substr(sizeof(std::uint32_t) + meta_size);

    proto::RpcMeta meta;
    if (!meta.ParseFromArray(meta_bytes.data(), static_cast<int>(meta_bytes.size())))
    {
        SetError(error, "failed to parse RPC meta");
        return false;
    }

    if (meta.payload_size() != payload.size())
    {
        SetError(error, "RPC payload size mismatch");
        return false;
    }

    frame->meta = std::move(meta);
    frame->payload.assign(payload.data(), payload.size());
    return true;
}

}  // namespace nebula::rpc
