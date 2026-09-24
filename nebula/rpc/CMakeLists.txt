find_package(spdlog REQUIRED)

find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

set(NEBULA_RPC_META_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/proto/rpc_meta.proto)
protobuf_generate_cpp(NEBULA_RPC_META_SRCS NEBULA_RPC_META_HDRS ${NEBULA_RPC_META_PROTO})

add_library(nebula_rpc_proto STATIC
    ${NEBULA_RPC_META_SRCS}
    ${NEBULA_RPC_META_HDRS}
)

target_include_directories(nebula_rpc_proto
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(nebula_rpc_proto
    PUBLIC
        protobuf::libprotobuf
)

add_library(nebula_rpc STATIC
    rpc_call.cpp
    rpc_controller.cpp
    rpc_codec.cpp
    rpc_server.cpp
    rpc_channel.cpp
)

target_include_directories(nebula_rpc
    PUBLIC
        ${PROJECT_SOURCE_DIR}
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(nebula_rpc
    PUBLIC
        nebula_build_options
        nebula_net
        nebula_rpc_proto
        protobuf::libprotobuf
        Threads::Threads
)

# spdlog logging dependency
target_link_libraries(nebula_rpc PUBLIC spdlog::spdlog)
