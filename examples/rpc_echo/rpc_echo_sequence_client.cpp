// 回调版：顺序发 3 个请求，成功就打印响应，失败就报错中止。
// 对照文件：rpc_echo_sequence_coroutine_client.cpp（同一个任务，协程写法）
//
// 这个版本为了跨过「异步等待」，必须额外引入两样东西：
//   SequenceState —— 所有跨步骤存活的状态（本来该是 for 里的局部变量）
//   SendNext      —— 手写的状态机，靠递归回到「循环」的下一轮

#include "nebula/net/event_loop.h"
#include "nebula/rpc/rpc_channel.h"
#include "nebula/rpc/rpc_closure.h"
#include "nebula/rpc/rpc_controller.h"
#include "echo.pb.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>

namespace
{

constexpr int kRequestCount = 3;

// 回调在下一次事件循环里执行，那时函数栈早就没了。
// 所以所有要跨步骤存活的东西，都只能塞进这个共享结构体。
struct SequenceState
{
    nebula::net::EventLoop* loop = nullptr;
    nebula::example::EchoService_Stub* stub = nullptr;

    nebula::example::EchoRequest request;
    nebula::example::EchoResponse response;
    nebula::rpc::RpcController controller;

    int index = 0;   // 本来应该是 for 循环里的 i，现在得自己维护
    int exit_code = 0;
};

void SendNext(const std::shared_ptr<SequenceState>& state);   // 前置声明，为了递归

void SendNext(const std::shared_ptr<SequenceState>& state)
{
    if (state->index >= kRequestCount)   // 循环条件得手写
    {
        state->loop->Quit();
        return;
    }

    state->controller.Reset();   // controller 复用了，自己记着复位
    state->request.set_text("hello #" + std::to_string(state->index + 1));

    auto* done = new nebula::rpc::RpcClosure([state]
        {
            if (state->controller.Failed())   // 错误处理：手工 if
            {
                std::cerr << "RPC failed: " << state->controller.ErrorText() << '\n';
                state->exit_code = 1;
                state->loop->Quit();   // 中止：直接掐掉事件循环
                return;
            }

            std::cout << "response=" << state->response.text() << '\n';

            ++state->index;        // 循环变量自增得手写
            SendNext(state);       // 回到「循环下一轮」＝递归调自己
        });

    state->stub->Echo(&state->controller,
                      &state->request,
                      &state->response,
                      done);

    std::cout << "CallMethod returned immediately for #" << state->index + 1 << '\n';
}

}  // namespace

int main(int argc, char** argv)
{
    const std::string ip = argc > 1 ? argv[1] : "127.0.0.1";
    const std::uint16_t port = argc > 2
        ? static_cast<std::uint16_t>(std::atoi(argv[2]))
        : 9000;

    nebula::net::EventLoop loop;
    nebula::rpc::RpcChannel channel(&loop, ip, port);
    nebula::example::EchoService_Stub stub(&channel);

    auto state = std::make_shared<SequenceState>();
    state->loop = &loop;
    state->stub = &stub;

    SendNext(state);   // 启动第一步

    loop.Loop();
    return state->exit_code;
}
