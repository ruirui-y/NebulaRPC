# ============================================================
# 最小 smoke test
# 暂时不引入第三方测试框架，保证新仓库开箱即可 build/test
# ============================================================

add_executable(nebula_smoke_test
    smoke_test.cpp
)

target_link_libraries(nebula_smoke_test
    PRIVATE
        nebula_net
        nebula_build_options
)

add_test(NAME nebula_smoke_test COMMAND nebula_smoke_test)
