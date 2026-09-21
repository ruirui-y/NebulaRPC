cmake_minimum_required(VERSION 3.22)

# ============================================================
# NebulaRPC 顶层构建文件
# 当前阶段只支持 Linux + C++20 + Ninja
# ============================================================

project(NebulaRPC VERSION 0.1.0 LANGUAGES CXX)

# ---- 平台限制 ----
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "NebulaRPC 当前仅支持 Linux")
endif()

# ---- C++ 标准 ----
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---- 通用构建设置 ----
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

# ---- 可选 Sanitizer ----
option(NEBULA_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(NEBULA_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

add_library(nebula_build_options INTERFACE)

target_compile_options(nebula_build_options INTERFACE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Wshadow
)

if(NEBULA_ENABLE_ASAN)
    target_compile_options(nebula_build_options INTERFACE -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(nebula_build_options INTERFACE -fsanitize=address)
endif()

if(NEBULA_ENABLE_UBSAN)
    target_compile_options(nebula_build_options INTERFACE -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(nebula_build_options INTERFACE -fsanitize=undefined)
endif()

# ---- 子目录 ----
add_subdirectory(nebula)

enable_testing()
add_subdirectory(tests)
