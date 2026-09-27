# 知识点探针沙盒：只依赖 C++20 标准库，由根 CMakeLists.txt 统一引入
# 命名约定：NN_topic.cpp -> pg_topic，一个文件只验证一个点

add_executable(pg_coroutine_suspend_order 01_coroutine_suspend_order.cpp)
target_link_libraries(pg_coroutine_suspend_order PRIVATE nebula_build_options)

# 依赖 net 层，不是纯标准库探针
add_executable(pg_connector_backoff 02_connector_backoff.cpp)
target_link_libraries(pg_connector_backoff PRIVATE nebula_net)

add_executable(pg_client_reconnect 03_client_reconnect.cpp)
target_link_libraries(pg_client_reconnect PRIVATE nebula_net)
