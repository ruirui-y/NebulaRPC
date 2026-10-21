#pragma once

#include "nebula/coro/task.h"

namespace nebula::rpc
{

// 兼容旧 include；新的通用协程代码优先直接使用 nebula::coro::Task。
template <typename T>
using Task = coro::Task<T>;

using DetachedTask = coro::DetachedTask;

}  // namespace nebula::rpc
