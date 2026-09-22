#include "nebula/rpc/rpc_codec.h"

#include <iostream>

int main()
{
    nebula::rpc::proto::RpcMeta meta;
    meta.set_type(nebula::rpc::proto::RpcMeta::REQUEST);
    meta.set_request_id(42);
    meta.set_service_name("demo.EchoService");
    meta.set_method_name("Echo");

    const std::string encoded = nebula::rpc::RpcCodec::Encode(meta, "payload");
    if (encoded.empty())
    {
        return 1;
    }

    nebula::net::Buffer buffer;
    buffer.Append(encoded);

    nebula::rpc::RpcFrame frame;
    std::string error;
    const auto result = nebula::rpc::RpcCodec::Decode(&buffer, &frame, &error);
    if (result != nebula::rpc::RpcCodec::DecodeResult::kOk)
    {
        std::cerr << error << '\n';
        return 2;
    }

    if (frame.meta.request_id() != 42 || frame.payload != "payload")
    {
        return 3;
    }

    std::cout << "RpcCodec frame round-trip passed\n";
    return 0;
}
