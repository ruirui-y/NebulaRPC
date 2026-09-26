cmake_minimum_required(VERSION 3.22)

project(NebulaRPC VERSION 0.2.0 LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
    message(FATAL_ERROR "NebulaRPC baseline currently supports Linux only")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

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

add_subdirectory(nebula)
add_subdirectory(examples)
add_subdirectory(playground)

enable_testing()
add_subdirectory(tests)
