# 02 · TCP 的连接与断开

状态：**已完成（2026-09-26）**

一句话：TCP 的「连接」不是一根管子，是**四元组 + 两端各自的独立状态机**；断开也不是一次动作，是**两个方向各关一次**。中间那个只关了一半的状态叫 **half-close**，它是「晕」的根源。

---

## 0. 速查卡

### 0.1 四个操作，四个后果

| 你想干什么 | 系统调用 | 内核动作 | fd 还活着吗 | 对端看到 |
|---|---|---|---|---|
| 我不发了，但还想收 | `shutdown(fd, SHUT_WR)` | 发 FIN | **活着** | `read` 返回 0 |
| 我不收了 | `shutdown(fd, SHUT_RD)` | 丢弃后续入站数据 | 活着 | 无感知 |
| 两个方向都不玩了 | `shutdown(fd, SHUT_RDWR)` | 发 FIN | 活着 | `read` 返回 0 |
| 彻底释放这个句柄 | `close(fd)` | 尽力发完数据再发 FIN | **关闭** | 数据 + FIN |
| 强行掐断 | `close` + `SO_LINGER=0` | 立刻发 RST | 关闭 | `ECONNRESET` |

**FIN 只有两个来源**：`shutdown(SHUT_WR)` 和 `close(fd)`。不是「只有 close 才发」，也不是「一 close 就发」—— 细节见 §3.4。

### 0.2 应用层能看到的信号

| 你观察到 | 含义 | 是错误吗 |
|---|---|---|
| `read` 返回 **0** | 对端发了 FIN，写通道关了（正常结束） | 不是 |
| `read` 返回 **-1 / EAGAIN** | 非阻塞模式下暂时没数据 | **不是**，要重试 |
| `read` 返回 **-1 / EINTR** | 被信号打断 | **不是**，要重试 |
| `read` 返回 **-1 / ECONNRESET** | 连接被复位（RST） | 是 |
| `write` 返回 **-1 / EPIPE** | 对端已经关了，我还写 | 是（且会附带 `SIGPIPE`，本仓库未处理 —— 见 §9.1） |
| epoll 报 **EPOLLHUP** | 两端都关了 | —— |

### 0.3 本仓库的三个关连接落点

| 落点 | 路径:行 | 干什么 |
|---|---|---|
| `Socket::ShutdownWrite()` | `nebula/net/socket.cpp:65` | `shutdown(fd, SHUT_WR)`，只关写方向，fd 留着 |
| `TcpConnection::ForceCloseInLoop()` | `nebula/net/tcp_connection.cpp:314` | 丢缓冲 + 状态置死，**不关 fd** |
| `Socket::~Socket()` | `nebula/net/socket.cpp:15` | `close(fd)` —— **全项目唯一真正关 fd 的地方** |

记住这张表，后面第 5 节全靠它。

### 0.4 状态机简版

```
            应用层              内核 TCP 状态
─────────────────────────────────────────────────────
                             CLOSED
  socket()+bind()+listen()   LISTEN
  对端 connect()              SYN_RCVD ──> ESTABLISHED
  accept() 拿到 fd            ESTABLISHED
                              
  本端 shutdown(WR)           FIN_WAIT_1 ──> FIN_WAIT_2
  本端 close()                FIN_WAIT_1 ──> FIN_WAIT_2 ──> TIME_WAIT ──> CLOSED
  对端 FIN 到达                CLOSE_WAIT  ──> LAST_ACK ──> CLOSED
```

关键：**本端主动关 → 最后停在 TIME_WAIT；本端被动关 → 停在 CLOSE_WAIT 等应用调 close**。CLOSE_WAIT 堆积是「应用层忘了 close fd」的典型症状。

---

## 1. 先建立正确的心理模型

「晕」通常来自一个错误的默认模型：**把连接想成一根管子，或者一个对象。**

真实的连接是：

```
一条 TCP 连接 = 四元组 (源IP, 源端口, 目的IP, 目的端口)
              + 两端内核里各一份状态机
              + 两个方向各自独立的数据流

     服务端内核                                    客户端内核
   ┌──────────────┐                              ┌──────────────┐
   │ 状态机 A      │   服务端 -> 客户端 数据流       │ 状态机 B      │
   │              │ ─────────────────────────>   │              │
   │              │                              │              │
   │              │   <───────────────────────── │              │
   └──────────────┘   客户端 -> 服务端 数据流       └──────────────┘
          ▲                                              ▲
      应用层的 fd                                   应用层的 fd
     （只是本端的「手柄」）                        （只是本端的「手柄」）
```

从这个模型直接推出三个反直觉的结论：

1. **关 fd ≠ 通知对端。** 通知对端的是内核发的 FIN，而 FIN 是 `close` 的时候才发的 —— 你在应用层把对象状态改成「已断开」，对方一点感觉都没有。本仓库的 `HandleClose` 就是这个例子（第 5 节）。
2. **一端关写 ≠ 连接结束。** 只有一半关掉了，另外一半照样能用 —— 这就是 half-close。
3. **「断开」在两端不是同时发生的**，中间隔着一个网络往返。所以服务端认为连接死了的时候，客户端可能还在正常读写。

---

## 2. 建立：三次握手

### 2.1 三次握手的时序

```
客户端内核                              服务端内核
   │                                       │
   │  ── SYN (seq=x) ────────────────────> │  进 SYN 队列（半连接）
   │                                       │
   │  <────────── SYN+ACK (seq=y, ack=x+1) │
   │                                       │
   │  ── ACK (ack=y+1) ──────────────────> │  从 SYN 队列移到 accept 队列
   │                                       │  此刻连接 = ESTABLISHED
   │                                       │
   │                                       │  accept() 取走 ──> 应用层拿到 fd
```

**为什么要三次，两次不行吗**：两次的话，服务端无法确认「客户端收到了我的 SYN+ACK」。更实际的问题是**历史连接的幽灵**：一个早先卡在网络里的旧 SYN 到达服务端，服务端两次握手就建立连接并开始发数据，而客户端根本不认这条连接 —— 资源被白白占用。第三次握手的 ACK 让服务端能确认「客户端现在确实是活的，且认可这个 seq」。

### 2.2 两个队列

```
                 ┌─────────────────────────┐
   SYN 到达 ───>  │ SYN queue（半连接队列）   │  收到 SYN，还没收到 ACK
                 └─────────────────────────┘
                              │ 第三次握手 ACK 到达
                              ▼
                 ┌─────────────────────────┐
                 │ accept queue（全连接队列）│  握手完成，等应用 accept()
                 └─────────────────────────┘
                              │ accept()
                              ▼
                        应用层的 conn fd
```

**`listen(fd, backlog)` 的 backlog 管的是全连接队列长度**（Linux 2.2 之后；再早的语义是这两个队列之和）。全连接队列满时，Linux 的默认行为是**丢弃新到达的 ACK**（不回 RST），客户端会以为 SYN+ACK 丢了而重传 SYN —— 表现为连接建立变慢，而不是立刻失败。

SYN 队列的长度不由 `listen` 控制，由 `net.ipv4.tcp_max_syn_backlog` 控制。

### 2.3 本仓库的实现

```cpp
// nebula/net/socket.cpp:23
::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP)

// nebula/net/socket.cpp:50
::listen(fd_, SOMAXCONN);

// nebula/net/socket.cpp:58
::accept4(fd_, reinterpret_cast<sockaddr*>(&peer), &length, SOCK_NONBLOCK | SOCK_CLOEXEC);
```

三个设计点：

- **`SOCK_NONBLOCK` 在 `socket()` 时就设**，而不是建好后 `fcntl`。少一次系统调用，也少一个窗口 —— 老写法里 `accept()` 返回阻塞 fd 到 `fcntl` 改成非阻塞之间，如果有数据到达，后续的 `read` 会挂住整个事件循环。
- **`accept4` 的 `SOCK_NONBLOCK | SOCK_CLOEXEC`** 一次性把新 fd 设好，不需要再来两次 `fcntl`。
- **`SOCK_CLOEXEC`** 防止这个 fd 泄漏给 `exec()` 出去的子进程，子进程持有 fd 会让连接迟迟不释放。

```cpp
// nebula/net/acceptor.cpp:33  —— 循环 accept 直到 EAGAIN
while (true)
{
    const int conn_fd = accept_socket_.Accept();
    if (conn_fd >= 0) { new_connection_callback_(conn_fd); continue; }
    if (errno == EINTR) { continue; }
    if (errno == EAGAIN || errno == EWOULDBLOCK) { break; }
    break;
}
```

**为什么必须循环**：epoll 是水平触发。一次 epoll_wait 返回时 accept queue 里可能已经积压了多个连接，只 accept 一个的话，剩下的会继续让 fd 保持可读 —— 循环处理干净才能避免事件循环反复被同一个 fd 唤醒。

`EINTR` 要继续也是同理：信号打断不算错误。

---

## 3. 断开：四次挥手

### 3.1 完整时序

```
主动关闭方（假设是服务端）                  被动关闭方（客户端）
   │                                            │
   │  ── FIN ─────────────────────────────────> │  read() 返回 0
   │     "我不发了"                              │  （对端可能还有数据要发）
   │  <──────────────────────── ACK ─────────── │
   │                                            │
   │  【FIN_WAIT_2：本端还能读，不能写了】          │  【CLOSE_WAIT：应用还没调 close】
   │         ←── 这就是 half-close ──>           │
   │                                            │
   │                                            │  对端发完了，也决定关
   │  <──────────────────────── FIN ─────────── │
   │  ── ACK ─────────────────────────────────> │  【CLOSED】
   │                                            │
   │  【TIME_WAIT 等 2MSL】                      │
   │  【CLOSED】                                 │
```

**为什么是四次而不是三次**：因为**两个方向是独立关闭的**。对端收到 FIN 之后，「回复 ACK」和「我也要发 FIN」是两个不同的时刻 —— 它可能还有数据要发完，所以不能合并。只有当它也没数据要发时，ACK 和 FIN 才可能出现在同一个报文里（这时看上去像三次）。

### 3.2 TIME_WAIT

**只有主动关闭方才有 TIME_WAIT。** 两个存在理由：

1. **保证最后一个 ACK 能重传。** 如果本端发的最后那个 ACK 丢了，对端会重发 FIN。如果本端已经彻底关掉，收到重发的 FIN 就只能回 RST，对端会认为连接异常结束。留着 TIME_WAIT 才能正常重传 ACK。
2. **让本次连接的迟到报文在网络里自然过期。** 否则同一个四元组被复用后，旧连接的残包可能被新连接误收。

Linux 上 TIME_WAIT 的持续时间是内核编译期常量 `TCP_TIMEWAIT_LEN`（**固定 60 秒**），没有任何 sysctl 能直接改它。2MSL 是理论值，Linux 没有按它算。

容易混淆的是 `net.ipv4.tcp_fin_timeout`（默认 60）—— 它管的是 **FIN_WAIT_2** 的超时（「本端发完 FIN、还没等到对端 FIN」能等多久），**不是 TIME_WAIT 时长**。这两个别搞混。

**TIME_WAIT 会占用四元组** —— 高并发短连接服务器会因此耗尽可用端口/四元组。解决办法不是去关 TIME_WAIT（那是自找麻烦），而是：

- 开 `SO_REUSEADDR`（本仓库 `nebula/net/acceptor.cpp:16` 有）
- 用长连接
- 让客户端做主动关闭方（TIME_WAIT 转移到客户端，服务端端口不够的问题就消失了）
- `net.ipv4.tcp_tw_reuse=1` —— 只对**主动发起 connect 的一方**有效，且依赖 TCP 时间戳开启

### 3.3 `read` 为什么返回 0：EOF 是内核告诉你的

这一节是 §4 的前置，也是「半关闭为什么能安全工作」的机制。

FIN 不是「管子断了」，它是**数据流里的一个带序列号的位置** —— 位置在最后一个数据字节之后：

```
服务端发送序列（FIN 消耗一个序号）：

   [data1][data2][data3][FIN]
                          ↑ 分界点："此后不会再有数据"
                            不是"管子断在这里"

客户端内核收到 FIN 后记下：对端写方向已关

客户端 read 的顺序：
   read() -> 1024      (data1)    ← 存量数据照常返回
   read() ->  512      (data2)
   read() ->  300      (data3)
   read() ->    0                 ← 存量读完 + 已知不会再有 → EOF
```

**`read` 返回 0 的准确含义：没有数据，并且永远不会再有了。**

它必须和另外几个返回值区分开：

| 返回值 | 含义 | 该怎么办 |
|---|---|---|
| `> 0` | 读到了 n 字节 | 继续处理 |
| `0` | **EOF**：没有数据，且不会有新的 | 关闭本端，或走 half-close 收尾 |
| `-1` + `EAGAIN` | **暂时**没数据，连接还活着 | 等下一次 epoll 通知，**不要关** |
| `-1` + `ECONNRESET` | 连接被复位 | 立刻关 |

**为什么必须有 `0` 这个值**：非阻塞 IO 下，如果「这次没数据」和「再也不会有数据」不区分，程序就无法判断该继续等还是该关闭 —— 要么永远不关（fd 泄漏），要么提前关（丢数据）。`0` 就是 TCP 给你的那个终止信号。

顺带解释一个现象：**对端 FIN 到达时，epoll 会把这个 fd 报成可读**。因为 `read` 此刻会立刻返回 `0`，满足「读不会阻塞」的定义，所以算可读事件。这也是 §5.2 里 `EPOLLIN` 能和 `EPOLLRDHUP` 一起使用的原因。

注意：**FIN 不会让你丢掉数据。** 客户端一定是先读到 `data1/data2/data3`，最后才读到 `0`。TCP 的有序性保证 FIN 排在数据之后。（如果客户端一直不读，那 `0` 也一直读不到 —— 数据还压在接收缓冲里，但连接状态已经变了。）

### 3.4 FIN 由谁发出：只有两条路

内核不会凭空发 FIN。能按下这个发送键的只有两个系统调用（外加「进程退出」这种内核代劳的隐式形式）：

```
① shutdown(fd, SHUT_WR)  /  SHUT_RDWR    显式：只关写方向，fd 还活着
② close(fd)                              隐式：fd 引用计数归零那一刻
     └─ 特例：进程 exit()
          └─ 内核关闭该进程所有 fd  →  本质仍是 ②
```

所以「只有 `close` 才发 FIN」是不成立的。本仓库**两条路都在用**：

| 路径 | 代码 | 发出 FIN 的时机 | 之后 fd 还活着吗 |
|---|---|---|---|
| `shutdown(SHUT_WR)` | `nebula/net/socket.cpp:67` | 调用当场（内核立即标记 + 发 FIN） | 活着，还能 `read` |
| `close(fd)` | `nebula/net/socket.cpp:15`（`~Socket`） | 引用计数归零那一刻 | 关闭 |

第二条路在本仓库要绕一圈才到达 —— 就是 §5.5 那条销毁链（`HandleClose` → `connections_.erase()` → lambda 执行完 → 引用计数归零 → `~TcpConnection` → `~Socket`）。**所以「FIN 什么时候发」在本仓库其实是「连接对象的最后一个持有者什么时候释放」**。

注意措辞：`close()` 关的是 **fd（句柄）**，不是「连接对象」。C++ 里不存在「close 一个对象」这回事。

#### `close` 不等于「立刻发 FIN」：四个前提

**前提 1：引用计数归零。** `fork` / `dup` 出来的 fd 指向**同一个 socket**，`close` 只是把计数减一，不发 FIN —— 只有最后一个引用消失才发。本仓库不存在这个问题：`socket()` / `accept4` 都带了 `SOCK_CLOEXEC`（`socket.cpp:25`、`:62`），`exec` 不会把它继承给子进程；且 `Socket` 是 `TcpConnection` 的独占成员，没有第二条持有路径。所以本仓库「fd 引用计数 == 连接对象引用计数」。

**前提 2：接收缓冲里没有未读数据。** ⚠️ 这条最容易踩 ——

```
对端已经发了数据过来，本端还没 read() 就把 fd 关了
        ↓
内核判定「这些数据再也没人要了」，直接丢弃
        ↓
不发 FIN，改发 RST          ← 对端看到的是 ECONNRESET，不是优雅 EOF
```

所以「优雅关闭」的标准姿势是**读到 `0` 再 `close`** —— 本仓库 `HandleRead` 的 `n == 0` 分支（`tcp_connection.cpp:149-177`）走的就是这条路，对端 FIN 之后才 `HandleClose()`，中间不会留下未读数据。

`shutdown(SHUT_WR)` **完全不受这条影响**：它只关写方向，接收缓冲里的未读数据照旧能读出来。

**前提 3：内核发送缓冲里的数据。** `close` 的语义是「尽力发完再 FIN」—— 内核接管这部分数据，在后台继续发送，发完才发 FIN。这就是「进程关了 socket 立刻退出，对端仍能收到完整数据」的原因。

但要和**用户态缓冲**分开：`close` 管不到 `output_buffer_`（那是应用层自己的 vector）—— 那部分数据直接报废。这正是 §4.3 里 `if (!channel_->IsWriting())` 要防的事。

**前提 4：`SO_LINGER`。** 本仓库全程没有 `setsockopt(SO_LINGER)`，所以走默认值 `l_onoff = 0`，也就是上面的「后台发送」。如果业务设了 `l_onoff = 1, l_linger = 0`，`close` 会**直接发 RST，一字节都不发** —— 这是「TCP 强制断开」的唯一标准做法。

#### 一张对照表

| 触发方式 | 有数据在**内核发送缓冲** | 有数据在**接收缓冲未读** | 网络动作 |
|---|---|---|---|
| `shutdown(SHUT_WR)` | 先发完，再 FIN | 无影响 | FIN |
| `close(fd)`（默认） | 后台发完，再 FIN | **改发 RST** | FIN / RST |
| `close(fd)` + `SO_LINGER=0` | **丢弃** | **丢弃** | RST |

#### 一个容易被忽略的组合：先 shutdown 再 close

本仓库可能出现「先 `ShutdownWrite()`、对端 FIN 到达后走 `HandleClose()`、最终 `~Socket` 再 `close()`」这条链。**这时 `close` 不会再发一次 FIN** —— 写方向在 `shutdown` 那一步就已经关了，FIN 早发出去了，`close` 此时只剩「回收 fd」这一个作用。

**一个 socket 的写方向只发一次 FIN。** （唯一的例外是 FIN 丢了：对端没 ACK，内核会按 RTO 重传同一个 FIN。那还是同一个 FIN，不是新的。）

---

## 4. half-close：最容易晕的地方

### 4.1 什么是半关闭

回到第 1 节那个「两根管子」的图。**一个方向关掉了，另一个方向还活着** —— 这就是 half-close。

```
      服务端                                             客户端
    ┌───────────┐     方向 A：服务端 -> 客户端             ┌───────────┐
    │           │  ──────────────────────────────>      │           │
    │           │   服务端侧=写          客户端侧=读       │           │
    │           │      ↑ SHUT_WR 关掉的是【整个方向 A】    │           │
    │           │                                       │           │
    │           │  <──────────────────────────────      │           │
    └───────────┘     方向 B：客户端 -> 服务端             └───────────┘
                  客户端侧=写          服务端侧=读
                     ↑ 这个方向完好
```

**先纠一个常见的数法：TCP 有 2 个方向，不是 4 个「读端 / 写端」。**

「服务端写」和「客户端读」不是两个独立的东西，它们是**同一根管子的两头**。关掉服务端的写，客户端的读**同时**就废了 —— 因为它俩本来就是同一个方向。

```
方向 A = 服务端写 -> [网络] -> 客户端读      <- 一根管子，两个头
方向 B = 客户端写 -> [网络] -> 服务端读      <- 另一根管子，两个头
```

所以 `SHUT_WR` 关掉的是 **1/2，不是 1/4**。关完之后：服务端能读、客户端能写（这俩是方向 B 的两头），而客户端**读不到了**（那是方向 A 的另一头，已经被服务端关了）。

`shutdown()` 的第二个参数就是选关哪根：

| 参数 | 关掉什么 | 关闭比例 | 本端后续行为 | 对端能感知吗 |
|---|---|---|---|---|
| `SHUT_WR` | 方向 A **整根** | **50%** | 能 `read`，不能 `write`（写得到 EPIPE） | **能** —— `read` 返回 0 |
| `SHUT_RD` | 只关本端的接收消费 | 0%（管子还在传数据） | `read` 立刻返回 0 | **不能** —— 对端照写不误 |
| `SHUT_RDWR` | 两根都关 | 100% | 读返回 0，写得到 EPIPE | 能 |

`SHUT_RD` 那一行是不对称的，值得单独记：`SHUT_WR` 会在网络上发一个 FIN，所以对端知道；**`SHUT_RD` 什么都不发**，对端完全不知道你不收了，还会继续写（一直写到它自己的发送缓冲满为止）。所以「关一个方向」这个说法，严格讲只对 `SHUT_WR` 成立。

本仓库只用了 `SHUT_WR`（`nebula/net/socket.cpp:67`）。

### 4.2 半关闭的实际用途

- **请求-响应协议**：「我说完了，等你说完」—— 客户端发完请求后 `SHUT_WR`，服务端就知道没有更多请求了，可以放心处理然后关闭。
- **单向推送**：推完就关掉写方向，但还要收对端的确认。
- **`SHUT_RD` 的用法**：告诉内核「我不想收了」—— 但这**不通知对端**，对端的 FIN 才会通知。而且 `SHUT_RD` 之后入站数据被静默丢弃，容易造成对端阻塞，一般不推荐。

### 4.3 本仓库的「有序关闭」

```cpp
// nebula/net/tcp_connection.cpp:304
void TcpConnection::ShutdownInLoop()
{
    loop_->AssertInLoopThread();
    if (!channel_->IsWriting())          // 还有待发数据就不 shutdown
    {
        socket_.ShutdownWrite();
    }
}
```

```cpp
// nebula/net/tcp_connection.cpp:201  —— 排空之后补上那一刀
if (state_.load() == State::kDisconnecting)
{
    ShutdownInLoop();
}
```

这两处配合起来表达的是**有序关闭**。但要拧清一件事：**顺序问题不在网络上，而在「两个缓冲的归属」上。**

```
应用层                              内核
output_buffer_  ──write()──>  内核发送缓冲  ──网络──>  对端
   ↑ 你自己管的                     ↑ 内核管的

shutdown(SHUT_WR) 之后：
  内核发送缓冲  ✅ 内核会先把里面的数据发完，再发 FIN
  output_buffer_ ❌ 还没 write() 进去的那部分，再也发不出去了（再写得到 EPIPE）
```

所以 `if (!channel_->IsWriting())` 保护的**不是网络上的顺序，而是用户态缓冲里那些还没交给内核的数据**。如果这时直接 shutdown：FIN 会立刻发出去（内核缓冲可能是空的），而 `output_buffer_` 里剩下的数据就永远死在本地 —— 对端只收到一个**不完整的响应**，然后就是 FIN。

对比：**FIN 在网络层确实排在数据后面**（它消耗一个序列号，位置在所有已发数据之后），所以「内核发送缓冲」那一层不需要额外保护，内核自己会处理。需要 `IsWriting` 这一道判断的，是上面那一层。

**这两行代码同时也是慢消费者的死结所在**，见第 6 节。

---

## 5. 应用层怎么发现连接断了 —— 三条通路

这一节是「本仓库实际怎么用」的核心。

### 5.1 三条通路

epoll 报三种事件，`Channel` 转发到三个不同的处理函数：

```
epoll 报什么                       Channel 转发                TcpConnection 处理
──────────────────────────────────────────────────────────────────────────────────
EPOLLIN / EPOLLRDHUP          read_callback_     ───>   HandleRead()
                                                        read() 返回 0  ──> HandleClose()
                                                        read() 返回 -1 ──> HandleError()

EPOLLHUP 且无 EPOLLIN         close_callback_    ───>   HandleClose()

EPOLLERR                      error_callback_    ───>   HandleError()
```

```cpp
// nebula/net/channel.cpp:37
void Channel::HandleEventWithGuard()
{
    if ((revents_ & EPOLLHUP) != 0U && (revents_ & EPOLLIN) == 0U)  { close_callback_(); }
    if ((revents_ & EPOLLERR) != 0U)                                { error_callback_(); }
    if ((revents_ & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) != 0U)       { read_callback_(); }
    if ((revents_ & EPOLLOUT) != 0U)                                { write_callback_(); }
}
```

### 5.2 关键一行：EPOLLRDHUP 被挂进了读事件

```cpp
// nebula/net/channel.cpp:10
const std::uint32_t Channel::kReadEvent = EPOLLIN | EPOLLPRI | EPOLLRDHUP;
```

`EPOLLRDHUP` 的语义是「**对端关闭了写方向**（或整条连接）」。把它挂进读事件，对端一 FIN，epoll 就把这个 fd 报成可读，于是 `HandleRead` 被调用，`read` 返回 0，走到 `HandleClose` —— 整条链路串起来。

**如果不加 `EPOLLRDHUP` 会怎样**：对端 FIN 之后，这个 fd 在内核看来**同样是可读的**（`read` 会立刻返回 0），所以 epoll 依然报 `EPOLLIN`，依然能发现。区别在于语义清晰度：

- 有 `EPOLLRDHUP`：内核明确告诉你「这是半关闭」
- 只有 `EPOLLIN`：你得自己 `read` 一次、发现返回 0，才知道是 EOF

对本仓库而言后者也够用（`HandleRead` 里就判断了 `n == 0`），但显式挂上 RDHUP 让「对端关了」这件事从「需要主动探测」变成「被动通知」。

### 5.2.1 `events_` 与 `revents_`：我订阅的 vs 真发生的

`Channel` 里两个长得几乎一样的字段，职责完全不同 —— **读 net 层最容易栽的一处**：

```cpp
// nebula/net/channel.h:98-99
std::uint32_t events_{0};     // 我【订阅】了什么（本端意图，写给内核）
std::uint32_t revents_{0};    // 内核【报】回来什么（客观事实，内核填）
```

| | `events_` | `revents_` |
|---|---|---|
| 谁写 | 本端（`Enable*/Disable*`，`channel.cpp:72-100`） | 内核（经 `epoll_wait`，`epoll_poller.cpp:93`） |
| 含义 | 「我想关注什么」 | 「这次真的发生了什么」 |
| 谁读 | `IsReading()` / `IsWriting()`（`channel.h:59-66`） | `HandleEventWithGuard()`（`channel.cpp:37-70`） |

完整闭环（订阅 → 内核 → 回报 → 注销）：

```
本端 EnableWriting()           channel.cpp:84
   events_ |= EPOLLOUT         ← 改的是【订阅】
   Update() → epoll_ctl(MOD)   → 把订阅写进内核
        ↓
   ...对端读走数据，内核腾出空间...
        ↓
epoll_wait 返回                epoll_poller.cpp:31
   channel->SetRevents(ev)     epoll_poller.cpp:93   ← 填的是【真发生的】
        ↓
HandleEventWithGuard()         channel.cpp:37
   if (revents_ & EPOLLOUT)    → write_callback_ → HandleWrite
        ↓
排空后 DisableWriting()        channel.cpp:90
   events_ &= ~EPOLLOUT        ← 取消订阅
```

**一句话**：`events_` 是「我想要什么」，`revents_` 是「实际给了我什么」。`IsWriting()` 读的是**前者** —— 所以它回答的是「我还在等写事件吗」，而不是「内核现在能写吗」（§6 那个自锁就栽在这个歧义上）。

### 5.3 HandleRead 的两个分支

```cpp
// nebula/net/tcp_connection.cpp:149
const ssize_t n = input_buffer_.ReadFd(socket_.Fd(), &saved_errno);
if (n > 0)          { message_callback_(...); }
else if (n == 0)    { HandleClose(); }                        // 对端 FIN
else if (saved_errno != EAGAIN && saved_errno != EWOULDBLOCK)
                    { HandleError(); }                        // 真错误
```

注意 `EAGAIN/EWOULDBLOCK` **被显式排除在错误分支之外** —— 非阻塞读返回 EAGAIN 是「这次没数据」，完全正常。如果把它当归错误处理，正常连接会被误杀。

### 5.4 HandleClose 是「应用层认定已死」，不是「关 fd」

```cpp
// nebula/net/tcp_connection.cpp:213
void TcpConnection::HandleClose()
{
    loop_->AssertInLoopThread();
    if (state_.load() == State::kDisconnected) { return; }   // 幂等：重复进来直接返回
    SetState(State::kDisconnected);
    channel_->DisableAll();
    auto guard = shared_from_this();                          // 回调期间保命
    if (connection_callback_) { connection_callback_(guard); }
    if (close_callback_)      { close_callback_(guard); }
}
```

三件事，一件都不是 `close(fd)`：改状态、摘掉所有 epoll 关注、通知上层。

### 5.5 fd 是什么时候真正关的

这是最容易漏的一环。完整的销毁链：

```
HandleClose()                             tcp_connection.cpp:213
   └─ close_callback_(guard)
        └─ TcpServer::RemoveConnection()  tcp_server.cpp:80
             └─ RunInLoop →（跨线程投回 owner loop）
                  └─ RemoveConnectionInLoop()          tcp_server.cpp:88
                       ├─ connections_.erase(name)     ← 连接表里的引用没了
                       └─ QueueInLoop([conn]{ conn->ConnectDestroyed(); })
                            └─ lambda 执行完，捕获的 conn 释放
                                 └─ 引用计数归零
                                      └─ ~TcpConnection
                                           └─ ~Socket     socket.cpp:15
                                                └─ ::close(fd)  ← 内核此时才发 FIN
```

两个值得记住的点：

- **`connections_.erase()` 之后对象不一定立刻死。** 它还被 `RemoveConnectionInLoop` 的参数 `conn` 和 `QueueInLoop` 的 lambda 捕获持有。真正的销毁发生在 lambda 执行完之后。
- **`Channel::Tie` 用的是 `weak_ptr`**（`nebula/net/channel.h:102`），所以它**不会**延长连接对象的寿命。它的职责只是「事件处理到一半，对象别被销毁」（`channel.cpp:23-35` 里 `tie_.lock()` 失败就跳过本次事件），不是持有所有权。

推论：**`HandleClose` 之后 fd 还开着，FIN 还没发。** 如果业务代码在这时候还持有这条连接的 `shared_ptr`，客户端就什么都不会收到。所以 `ForceCloseInLoop` 里必须显式 `DisableWriting()`，防止「应用层以为连接死了、内核还在往外发数据」这种双头状态。

---

## 6. 为什么慢消费者会让 Shutdown 永远关不掉

第 9 节（Backpressure）里「硬上限必须用 ForceClose 而不是 Shutdown」的完整推导：

```
对端（慢消费者）不读数据
        ↓
本端内核发送缓冲满 → 本端 socket 不再可写 → epoll 永远不报 EPOLLOUT
        ↓
HandleWrite 永远不被调用 → output_buffer_ 永远排不空
        ↓
channel_->IsWriting() 一直为 true
        ↓
ShutdownInLoop 里 if (!channel_->IsWriting()) 永远不成立
        ↓
FIN 永远发不出去 → 等不到对端的 FIN → 连接永远挂着，内存永远不还
```

**⚠️ 上面第 4 步的 `IsWriting()` 有个必踩的读法陷阱**：它不是「内核现在能不能写」，而是「**我自己还没写完**」。

```cpp
// nebula/net/channel.h:59-62
[[nodiscard]] bool IsWriting() const noexcept
{
    return (events_ & kWriteEvent) != 0U;   // 读的是【有没有订阅 EPOLLOUT】，不是内核状态
}
```

它和「内核可不可写」是**反着**的：

```
内核 sndbuf 有空位 → epoll 报 EPOLLOUT → HandleWrite 被调 → 数据拷进内核 → 排空后复位
内核 sndbuf 满了   → epoll【不报】     → 什么都不发生  → IsWriting()【僵在 true 不动】
```

所以「内核不可写」的后果**不是**把 `IsWriting()` 变成 `false`，而是让它**一直卡在 `true`**。

复位点全项目只有一个：

```cpp
// nebula/net/tcp_connection.cpp:190-192
if (output_buffer_.ReadableBytes() == 0U)   // 必须【排空】
{
    channel_->DisableWriting();              // 只有这里能把 IsWriting() 变回 false
}
```

而「排空」又只能靠 EPOLLOUT 驱动的 `HandleWrite` 推进 —— **那正是被堵住的那个事件**：

```
要复位 IsWriting()
   ↑ 依赖
output_buffer_ 排空
   ↑ 依赖
EPOLLOUT 到来
   ↑ 依赖
内核 sndbuf 有空位
   ↑ 依赖
对端读走数据          ← 慢消费者偏偏不做这件事
```

（读法：下面的支撑上面的。）一个闭环自锁：**唯一的出口，挂在那个永远不来的事件上。**

所以那个 `if` 不是「检查内核能不能写」，而是「**检查我还有没有没发完的数据**」。不变式是 `IsWriting() == true` ⟺ `output_buffer_` 非空（两者在 loop 线程内同步变化）—— 读 `IsWriting()` 只是比看 buffer 更便宜。

**最反直觉的一点**：`Shutdown()` 是「立刻」改状态的 ——

```cpp
// nebula/net/tcp_connection.cpp:109
State expected = State::kConnected;
if (state_.compare_exchange_strong(expected, State::kDisconnecting))
```

状态确实立刻变成了 `kDisconnecting`。**但变的只是状态**：缓冲还在、fd 还在、内存还在。状态的改变不等于资源的释放。

`ForceCloseInLoop` 就是直接跳过「等对端」这一步：

```cpp
// nebula/net/tcp_connection.cpp:314
output_buffer_.RetrieveAll();      // 丢数据 → 内存立刻回落
channel_->DisableWriting();        // 不再等 EPOLLOUT
HandleClose();                     // 状态置死 → 走第 5.5 节的回收链
```

三个动作对应三件事，缺一不可：**丢数据**（否则内存不降）、**摘写事件**（否则 epoll 还可能报 EPOLLOUT 进来）、**置死状态**（否则上层不知道连接没了）。

### 6.1 截断的代价：客户端不知道它「不知道」

`RetrieveAll()` 丢掉的那部分数据，在客户端看起来是什么样？RPC 帧是「长度前缀 + protobuf body」，所以客户端看到的是**半截帧**：

```
服务端本该发的字节流：
   [len = 1024][ body 前 400 字节 ........... ]   ← RetrieveAll() 丢在这里
              └── 内核发完这 404 字节后发 FIN ──┘

客户端解析器：
   读到 len = 1024  →  「还差 1024 字节，继续等」
   读到 400 字节    →  「继续等」
   收到 FIN         →  永远等不到剩下那 624 字节
                        ↓
                    这半截帧【无法使用】，整条只能丢弃
                    但客户端无法判断：服务端到底执行了没有？
```

**真正的伤害不是「丢了几个字节」，是「客户端拿不到结论」。**

| 客户端看到 | 能推断出 | 能安全重试吗 |
|---|---|---|
| 干净断开，一字节响应都没有 | 服务端大概率没执行 | ✓ 可以 |
| **半截响应 + FIN** | **执行与否未知** | ✗ 重试可能重复执行 |
| 完整响应 + FIN | 服务端执行了 | — |

对 RPC 来说，「执行与否未知」是最贵的状态：重试可能把一笔支付扣两次、把一次下单做两遍。这一层**不能靠 TCP 兜** —— 必须靠 request_id + 服务端去重表做成 Exactly-Once（本仓库第 7 节做的事）。

**由此得到一条设计结论**：硬上限砍数据是**最后手段**，不是首选。首选是在被砍之前让业务收到通知、**主动回一个明确的错误响应**，给这次交互一个结论：

```
软水位越线  →  通知业务
                 └─ 业务回 SERVER_BUSY 错误帧（结论明确）
                    ← 这才是第 9 节软水位存在的真正理由
硬上限越线  →  框架丢缓冲 + 断连（结论未知）
                 └─ 只有业务对软水位毫无反应时才会走到这里
```

对照本仓库 echo 示例（`examples/rpc_echo/rpc_echo_server.cpp:48-56`）只打日志不拒绝 —— 因为 echo 没有「可拒绝的入口」（请求已经收了，响应必须发完）。有上游队列的业务（比如 gateway 的 task slot）就能在这里回一个 `SERVER_BUSY`，把「未知」变成「明确失败」。

---

## 7. 纠错清单

参考来源（用于对照，未照抄）：
- 《TCP/IP 详解 卷 1：协议》第 17-18 章（TCP 连接的建立与终止）
- `man 7 tcp`、`man 2 shutdown`、`man 2 close`、`man 7 epoll`
- Linux 内核 `net/ipv4/tcp_minisocks.c` 关于 `tcp_max_syn_backlog` 与全连接队列的处理
- `Documentation/networking/ip-sysctl.rst`（`tcp_fin_timeout`、`tcp_max_syn_backlog`）

以下说法流传很广，但都需要修正：

| # | 常见说法 | 实际情况 |
|---|---|---|
| 1 | 「`shutdown` 就是关闭连接」 | 只关指定方向。`SHUT_WR` 之后 fd 还活着、还能 `read` |
| 2 | 「`close` 之后对端一定收到 RST」 | 默认是**尽力发完数据再 FIN**。RST 只出现在特定条件下（`SO_LINGER=0`、close 时接收缓冲还有未读数据等） |
| 2b | 「只有 `close` 才会发 FIN」 | 两条路：`shutdown(fd, SHUT_WR/SHUT_RDWR)` 和 `close(fd)`。本仓库两条都在用（`socket.cpp:67` / `socket.cpp:15`）。见 §3.4 |
| 2c | 「`close` 就是把连接对象关掉」 | `close` 关的是 **fd（句柄）**。而且 fd 引用计数没归零时 `close` 不发 FIN；写方向已经 `shutdown` 过的话，`close` 也不再发 FIN |
| 3 | 「`read` 返回 -1 就是连接断了」 | 必须看 `errno`：`EAGAIN`/`EINTR` 都**不是**错误。本仓库 `tcp_connection.cpp:172` 显式排除了 EAGAIN |
| 4 | 「四次挥手的四个步骤必须分开」 | 本质是「两个方向各关一次」；对端没数据要发时，ACK 和 FIN 可能在同一个报文里 |
| 5 | 「TIME_WAIT 是浪费，应该关掉」 | 它保证最后的 ACK 能重传 + 避免迟到报文污染新连接。要缓解用 `SO_REUSEADDR` 或长连接 |
| 6 | 「`accept` 返回时连接才建立」 | 握手在 `accept` 之前就完成了；`accept` 只是从全连接队列里取一个现成的 |
| 7 | 「`listen` 的 backlog 是 SYN 队列长度」 | Linux 2.2 之后管的是**全连接队列**；SYN 队列长度由 `tcp_max_syn_backlog` 控制 |
| 8 | 「FIN 排在数据后面，所以 close 时数据一定发得出去」 | 对**内核发送缓冲**成立（内核会先发完里面的数据再发 FIN）。但**用户态缓冲**里还没 `write()` 进去的数据不在此列 —— shutdown 之后再写会得到 EPIPE。本仓库 `if (!channel_->IsWriting())`（`tcp_connection.cpp:308`）保护的正是这一层 |
| 8b | 「对端关了写方向，我这边读会报错 / 阻塞」 | 不会。`read` 会先把存量数据读完，然后返回 **0**（EOF）。见 §3.3 |
| 9 | 「加 `EPOLLRDHUP` 才能发现对端关闭」 | 不加也能发现（FIN 后 fd 变可读，epoll 报 EPOLLIN）。加了是让「半关闭」从「需要主动 read 探测」变成「被动收到通知」 |
| 10 | 「TIME_WAIT 在被动关闭方」 | **只有主动关闭方有**。被动方的对应状态是 `CLOSE_WAIT` |

---

## 8. 映射表

| 知识点 | 本仓库落点 |
|---|---|
| 非阻塞 socket 创建（`SOCK_NONBLOCK \| SOCK_CLOEXEC`） | `nebula/net/socket.cpp:23-31` |
| `listen` backlog = `SOMAXCONN` | `nebula/net/socket.cpp:50-56` |
| `accept4` 一次设好非阻塞 + CLOEXEC | `nebula/net/socket.cpp:58-63` |
| 循环 `accept` 直到 EAGAIN（水平触发必须排空） | `nebula/net/acceptor.cpp:33-63` |
| `SO_REUSEADDR`（TIME_WAIT 复用） | `nebula/net/acceptor.cpp:16` |
| `TCP_NODELAY` | `nebula/net/tcp_connection.cpp:37` |
| `EPOLLRDHUP` 挂进读事件 | `nebula/net/channel.cpp:10` |
| HUP / ERR / 读 / 写 四条转发 | `nebula/net/channel.cpp:37-70` |
| 读方向 EOF（`read` 返回 0） | `nebula/net/tcp_connection.cpp:168-171` |
| 读方向错误分支（排除 EAGAIN） | `nebula/net/tcp_connection.cpp:172-176` |
| half-close 触发点（`SHUT_WR`） | `nebula/net/socket.cpp:65-68` + `tcp_connection.cpp:305-313` |
| 有序关闭（排空后补 FIN） | `nebula/net/tcp_connection.cpp:201-204` |
| 强制关闭（丢缓冲 + 摘写事件 + 置死） | `nebula/net/tcp_connection.cpp:314-326` |
| 应用层认定已死 + 幂等保护 | `nebula/net/tcp_connection.cpp:213-233` |
| 连接表摘除 + 异步销毁 | `nebula/net/tcp_server.cpp:80-97` |
| fd 真正关闭 | `nebula/net/socket.cpp:15-21` |
| `Tie` 用 `weak_ptr`（不延长寿命） | `nebula/net/channel.h:102`、`channel.cpp:17-35` |
| `SIGPIPE` 处理 | **缺失** —— `tcp_connection.cpp:186` 与 `:258` 用裸 `::write`，全仓库无 `SIGPIPE`/`MSG_NOSIGNAL` 处置，见 §9.1 |

---

## 9. 本仓库的两处隐患

### 9.1 已确认：SIGPIPE 全程未处理

`TcpConnection` 往外写用的是裸 `::write`：

```cpp
// nebula/net/tcp_connection.cpp:258   直写路径
written = ::write(socket_.Fd(), data.data(), data.size());

// nebula/net/tcp_connection.cpp:186   HandleWrite 补写
const ssize_t n = ::write(socket_.Fd(), output_buffer_.Peek(), output_buffer_.ReadableBytes());
```

`::write` 对 socket 的行为等价于 `send(fd, buf, len, 0)` —— **没有 `MSG_NOSIGNAL`**。向一个已经被对端 RST 的 socket 写数据，内核会产生 `SIGPIPE`；而 `SIGPIPE` 的默认处置动作是**终止进程**。

全仓库检索 `SIGPIPE` / `MSG_NOSIGNAL` / `signal(`：

```
nebula/ 下：0 处
examples/ 下：0 处
tests/rpc_backpressure_test.cpp:346  有 MSG_NOSIGNAL（但那是测试自己发裸数据用的）
```

也就是说：**只要对端异常断开（RST）后本端还试图写一次，整个服务进程会被信号杀掉。** 慢消费者 → 硬上限断连 → 若此刻还有残留写动作，就可能踩到。

注意 `SendInLoop` 的 `::write` 失败分支（`tcp_connection.cpp:271-279`）只处理了 `EWOULDBLOCK/EAGAIN`，其余 `errno`（含 `EPIPE`）走 `HandleError()` —— 但信号是在 `write` **返回之前**就投递的，所以先到的是信号，进程可能根本活不到 `HandleError`。

两种标准修法，任选其一：

```
A. 调用级（精确，推荐）：把两处 ::write 换成 ::send(fd, buf, len, MSG_NOSIGNAL)
   影响面只限这条 socket

B. 进程级（简单）：main 开头 signal(SIGPIPE, SIG_IGN)
   一处解决全部 fd，但会连带忽略其他来源的 SIGPIPE
```

（本节只记录现状与修法，**未改动任何代码**。）

### 9.2 未验证：`HandleError()` 不关连接

```cpp
// nebula/net/tcp_connection.cpp:235
void TcpConnection::HandleError()
{
    int error = 0;
    socklen_t length = sizeof(error);
    if (::getsockopt(socket_.Fd(), SOL_SOCKET, SO_ERROR, &error, &length) < 0)
    {
        error = errno;
    }
    // 取出来了，然后……没有然后了
}
```

局部变量 `error` 赋值后再没有被使用，函数直接返回 —— **连接不会被关闭**。走 `EPOLLERR` 或 `read` 真错误这两条路径时都会进这里。

这是**观察到的代码现状**，不是结论 —— 是否真的会卡住（epoll 反复报错、连接表项一直不消失）需要实验确认。按「只相信日志」的做法：

```
1. 起 echo server，客户端连上
2. 用 iptables 制造 RST，或直接 kill -9 客户端进程
3. 观察 server 侧：HandleError 之后还有没有后续动作
4. 若 connections_ 里该连接一直不消失 / epoll 反复唤醒 → 确认卡住
```

参考做法：`HandleError` 里取到 `error` 后应当记日志并调 `HandleClose()`（或 `ForceCloseInLoop()`），让连接走正常回收链。

---

## 10. 自检题

每题下面的代码块是**作答区**，留空的，直接在里面写。写完把整份发我批改。

**1.** `shutdown(fd, SHUT_WR)` 之后：本端还能不能 `read`？能不能 `write`？应用层算不算「连接结束」？

```
不能 waite，能读，不算连接结束
```

**2.** 为什么本仓库的 FIN 必须等 `output_buffer_` 排空才能发（`tcp_connection.cpp:308`）？如果反过来先发 FIN 会发生什么？

```
让一个响应完整的到达客户端，如果先发FIN会导致响应很可能只接收到了一半，客户端不知道请求是否成功，此时需要服务端记录该请求的状态，等客户端下次连上时来获取上次请求的状态
```

**3.** `read` 返回 0 和返回 -1/`ECONNRESET`，分别代表对端做了什么？`read` 返回 -1/`EAGAIN` 是不是错误？

```
返回0，说明对端关闭了写通道，返回EONNRESET我也不知道对端做了什么，返回EAGAIN不是错误
```

**4.** 不加 `EPOLLRDHUP` 能不能发现对端关闭？加上它之后语义上多了什么？

```
不知道
```

**5.** 本仓库里 `close(fd)` 只有一个地方，是哪？从「处理完一个对端 FIN」到「fd 关闭」，中间经过了哪几步？为什么 `connections_.erase()` 之后对象不一定立刻销毁？

```

```

**6.** `ForceCloseInLoop` 里的 `RetrieveAll()`、`DisableWriting()`、`HandleClose()` 三个动作，各自解决什么问题？少一个会怎样？

```

```

**7.** TIME_WAIT 出现在主动关闭方还是被动关闭方？它存在的两个理由是什么？

```

```

**8.** `Channel::Tie` 为什么用 `weak_ptr` 而不是 `shared_ptr`？换成 `shared_ptr` 会导致什么后果？

```

```

**9.** 慢消费者场景下，为什么 `Shutdown()` 会永远关不掉连接？（把 §6 的五步链背出来）

```

```

**10.** `listen` 的 backlog 满时，Linux 默认行为是什么？客户端会看到什么现象？

```

```

**11.** 向一个已被对端 RST 的 socket 调 `::write` 会发生什么？进程会怎样？本仓库为什么有风险（§9.1），两种修法各是什么？

```

```

**12.** `net.ipv4.tcp_fin_timeout` 调的是 TIME_WAIT 还是 FIN_WAIT_2？为什么不能靠它缩短 TIME_WAIT？

```

```

**13.** TCP 有 2 个方向还是 4 个「读端/写端」？服务端 `SHUT_WR` 之后，客户端的**读**还能用吗？为什么？

```

```

**14.** `SHUT_RD` 和 `SHUT_WR` 在对端感知上为什么不对称？对端从哪个信号能知道本端 `SHUT_WR` 了？本端 `SHUT_RD` 之后对端会怎样？

```

```

**15.** 对端 `SHUT_WR` 之后，本端 `read` 会不会报错或永久阻塞？它会先返回什么、最后返回什么？`read` 返回 `0` 和返回 `-1/EAGAIN` 的区别为什么是非阻塞 IO 必须区分的一件事？

```

```

**16.** `if (!channel_->IsWriting())` 保护的到底是哪一层缓冲？内核发送缓冲里的数据和 `output_buffer_` 里的数据，在 `shutdown(SHUT_WR)` 时的命运有什么不同？

```

```

**17.** 能触发 FIN 的系统调用有哪几个？本仓库分别在哪一行用了它们？「一个 socket 的写方向最多发几次 FIN」？

```

```

**18.** `close(fd)` 在什么情况下**不发 FIN 而发 RST**？为什么本仓库 `HandleRead` 里「读到 `0` 才 `HandleClose()`」这个顺序，正好避开了这条路径？

```

```

**19.** 硬上限 `ForceCloseInLoop` 会丢掉 `output_buffer_` 里没发出去的数据（§6.1）。客户端这时看到的是什么？为什么它**无法判断「服务端到底执行了没有」**——这和「连接干净断开、一个字节响应都没发」相比，差别在哪？

```

```

**20.** `Channel::IsWriting()`（`channel.h:59-62`）读的到底是什么？为什么慢消费者场景下它一直是 `true` 而不是 `false`？它能变回 `false` 的**唯一条件**在哪一行，这个条件又依赖什么事件（§6）？

```

```