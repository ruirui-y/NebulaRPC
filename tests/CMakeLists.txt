add_executable(nebula_net_smoke_test net_smoke_test.cpp)
target_link_libraries(nebula_net_smoke_test PRIVATE nebula_net)
add_test(NAME nebula_net_smoke_test COMMAND nebula_net_smoke_test)

add_executable(nebula_timer_queue_test timer_queue_test.cpp)
target_link_libraries(nebula_timer_queue_test PRIVATE nebula_net)
add_test(NAME nebula_timer_queue_test COMMAND nebula_timer_queue_test)

add_executable(nebula_rpc_codec_test rpc_codec_test.cpp)
target_link_libraries(nebula_rpc_codec_test PRIVATE nebula_rpc)
add_test(NAME nebula_rpc_codec_test COMMAND nebula_rpc_codec_test)

add_executable(nebula_rpc_timeout_test rpc_timeout_test.cpp)
target_include_directories(nebula_rpc_timeout_test
    PRIVATE
        ${PROJECT_BINARY_DIR}/examples/rpc_echo
)
target_link_libraries(nebula_rpc_timeout_test
    PRIVATE
        nebula_rpc
        nebula_rpc_echo_proto
)
add_test(NAME nebula_rpc_timeout_test COMMAND nebula_rpc_timeout_test)
set_tests_properties(nebula_rpc_timeout_test PROPERTIES RUN_SERIAL TRUE)

add_executable(nebula_rpc_call_test rpc_call_test.cpp)
target_link_libraries(nebula_rpc_call_test PRIVATE nebula_rpc)
add_test(NAME nebula_rpc_call_test COMMAND nebula_rpc_call_test)

add_executable(nebula_rpc_cancel_test rpc_cancel_test.cpp)
target_include_directories(nebula_rpc_cancel_test
    PRIVATE
        ${PROJECT_BINARY_DIR}/examples/rpc_echo
)
target_link_libraries(nebula_rpc_cancel_test
    PRIVATE
        nebula_rpc
        nebula_rpc_echo_proto
)
add_test(NAME nebula_rpc_cancel_test COMMAND nebula_rpc_cancel_test)
set_tests_properties(nebula_rpc_cancel_test PROPERTIES RUN_SERIAL TRUE)

add_executable(nebula_rpc_coroutine_test rpc_coroutine_test.cpp)
target_include_directories(nebula_rpc_coroutine_test
    PRIVATE
        ${PROJECT_BINARY_DIR}/examples/rpc_echo
)
target_link_libraries(nebula_rpc_coroutine_test
    PRIVATE
        nebula_rpc
        nebula_rpc_echo_proto
)
add_test(NAME nebula_rpc_coroutine_test COMMAND nebula_rpc_coroutine_test)
set_tests_properties(nebula_rpc_coroutine_test PROPERTIES RUN_SERIAL TRUE)

add_executable(nebula_rpc_backpressure_test rpc_backpressure_test.cpp)
target_include_directories(nebula_rpc_backpressure_test
    PRIVATE
        ${PROJECT_BINARY_DIR}/examples/rpc_echo
)
target_link_libraries(nebula_rpc_backpressure_test
    PRIVATE
        nebula_rpc
        nebula_rpc_echo_proto
)
add_test(NAME nebula_rpc_backpressure_test COMMAND nebula_rpc_backpressure_test)
set_tests_properties(nebula_rpc_backpressure_test PROPERTIES RUN_SERIAL TRUE)
