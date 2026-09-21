# ============================================================
# 最小网络 Runtime
# Stage 1 从这里开始逐步补齐 EventLoop / Channel / TcpConnection
# ============================================================

add_library(nebula_net STATIC
    event_loop.cpp
    channel.cpp
    tcp_connection.cpp
)

target_include_directories(nebula_net
    PUBLIC
        ${PROJECT_SOURCE_DIR}
)

target_link_libraries(nebula_net
    PUBLIC
        nebula_build_options
)
