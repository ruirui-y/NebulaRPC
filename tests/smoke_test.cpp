#include "nebula/net/event_loop.h"

int main()
{
    nebula::net::EventLoop loop;
    return loop.IsInLoopThread() ? 0 : 1;
}
