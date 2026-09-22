find_package(Threads REQUIRED)

add_library(nebula_net STATIC
    channel.cpp
    poller.cpp
    epoll_poller.cpp
    event_loop.cpp
    timer_queue.cpp
    socket.cpp
    acceptor.cpp
    tcp_connection.cpp
    event_loop_thread.cpp
    event_loop_thread_pool.cpp
    tcp_server.cpp
    connector.cpp
    tcp_client.cpp
)

target_include_directories(nebula_net
    PUBLIC
        ${PROJECT_SOURCE_DIR}
)

target_link_libraries(nebula_net
    PUBLIC
        nebula_build_options
        Threads::Threads
)

# spdlog is header-only but linked for dependency propagation
target_link_libraries(nebula_net PUBLIC spdlog::spdlog)
