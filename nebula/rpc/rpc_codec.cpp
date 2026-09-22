#include "nebula/rpc/rpc_codec.h"

#include <arpa/inet.h>

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
    meta.set_payload_size(static_cast<std::uint32_t>(payload.size()));

    std::string meta_bytes;
    if (!meta.SerializeToString(&meta_bytes))
    {
        return {};
    }

    const std::size_t body_size = sizeof(std::uint32_t) + meta_bytes.size() + payload.size();
    if (body_size > kMaxFrameSize || body_size > std::numeric_limits<std::uint32_t>::max())
    {
        return {};
    }

    const auto frame_size = static_cast<std::uint32_t>(body_size);
    const auto meta_size = static_cast<std::uint32_t>(meta_bytes.size());
    const std::uint32_t frame_size_be = htonl(frame_size);
    const std::uint32_t meta_size_be = htonl(meta_size);

    std::string output;
    output.reserve(sizeof(frame_size_be) + body_size);
    output.append(reinterpret_cast<const char*>(&frame_size_be), sizeof(frame_size_be));
    output.append(reinterpret_cast<const char*>(&meta_size_be), sizeof(meta_size_be));
    output.append(meta_bytes);
    output.append(payload.data(), payload.size());
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
