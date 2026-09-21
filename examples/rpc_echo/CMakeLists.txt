find_package(Protobuf REQUIRED)

set(ECHO_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/echo.proto)
protobuf_generate_cpp(ECHO_PROTO_SRCS ECHO_PROTO_HDRS ${ECHO_PROTO})

add_library(nebula_rpc_echo_proto STATIC
    ${ECHO_PROTO_SRCS}
    ${ECHO_PROTO_HDRS}
)

target_include_directories(nebula_rpc_echo_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(nebula_rpc_echo_proto PUBLIC protobuf::libprotobuf)

add_executable(nebula_rpc_echo_server rpc_echo_server.cpp)
target_include_directories(nebula_rpc_echo_server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(nebula_rpc_echo_server PRIVATE nebula_rpc nebula_rpc_echo_proto)

add_executable(nebula_rpc_echo_client rpc_echo_client.cpp)
target_include_directories(nebula_rpc_echo_client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(nebula_rpc_echo_client PRIVATE nebula_rpc nebula_rpc_echo_proto)
