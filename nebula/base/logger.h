#pragma once

#include <spdlog/spdlog.h>

namespace nebula::base
{

inline void InitLogger()
{
    spdlog::set_level(spdlog::level::debug);
}

} // namespace nebula::base

#define NLOG_DEBUG(...) spdlog::debug(__VA_ARGS__)
#define NLOG_INFO(...)  spdlog::info(__VA_ARGS__)
#define NLOG_WARN(...)  spdlog::warn(__VA_ARGS__)
#define NLOG_ERROR(...) spdlog::error(__VA_ARGS__)
