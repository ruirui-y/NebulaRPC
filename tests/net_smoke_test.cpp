#include "nebula/net/event_loop.h"

#include <iostream>

int main()
{
    nebula::net::EventLoop loop;
    if (!loop.IsInLoopThread())
    {
        return 1;
    }
    std::cout << "EventLoop owner-thread check passed\n";
    return 0;
}
