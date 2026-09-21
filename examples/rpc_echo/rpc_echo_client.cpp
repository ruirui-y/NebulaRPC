#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_controller.h"
#include "echo.pb.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>

int main(int argc, char** argv) {
    const std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    const std::uint16_t port = argc > 2 ? static_cast<std::uint16_t>(std::atoi(argv[2])) : 9000;

    nebula::rpc::RpcChannel channel(ip, port);
    nebula::example::EchoService_Stub stub(&channel);

    for (int i = 0; i < 3; ++i) {
        nebula::example::EchoRequest request;
        nebula::example::EchoResponse response;
        nebula::rpc::RpcController controller;

        request.set_text("hello NebulaRPC #" + std::to_string(i + 1));
        stub.Echo(&controller, &request, &response, nullptr);

        if (controller.Failed()) {
            std::cerr << "RPC failed: " << controller.ErrorText() << '\n';
            return 1;
        }

        std::cout << "response=" << response.text()
                  << ", server_sequence=" << response.server_sequence() << '\n';
    }

    return 0;
}
