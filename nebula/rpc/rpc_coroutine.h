#pragma once

#include "nebula/coro/task.h"
#include "nebula/net/coroutine_awaiters.h"
#include "nebula/rpc/rpc_awaiter.h"
#include "nebula/rpc/rpc_coroutine_client.h"
#include "nebula/rpc/rpc_coroutine_service.h"
#include "nebula/rpc/rpc_result.h"

// 新的协程业务代码可以只包含这个聚合头。
