# C++20 协程机制 · 知识点整理

来源：老孟讲编程《C++20 协程机制（Coroutines）》 <https://mengbaoliang.cn/archives/131970.html>

整理原则：**取其机制骨架，去其会误导人的写法，补其缺失的关键环节。**
本文只讲语言知识，项目落地实现看 `../07_Coroutine_RPC.md`，逐条纠错看第 14 节。

**验证状态**：第 3 节 `Minimal`、第 11 节 `IntGenerator` 已用 MSVC 14.44.35207（`/std:c++20 /W4 /EHsc`）编译并运行通过（生成器前 10 项 `count=10 sum=88`）；
第 11 节「双重释放」那条反例已实测复现为访问违例（见 11 节末尾）。第 12 节 `AsyncSleep` 依赖 `nebula::net::EventLoop`，属片段，未单独编译。

---

## 0. 一页速查

```
一个函数是协程的充要条件（两条同时满足）：
  ① 函数体里有 co_await / co_return / co_yield 至少一个
  ② 返回类型里有 promise_type（或 coroutine_traits 特化提供）

协程帧：编译器 new 出来的堆内存，装 promise + 跨挂起点存活的局部变量 + 暂停点
        编译器负责 new，你负责 delete（handle.destroy()）

co_await X 的三问：
  ① 现在就好了吗？            X.await_ready()            → bool
  ② 我要挂了，接下来谁跑？    X.await_suspend(handle)    → void / bool / handle
  ③ 结果给我                  X.await_resume()           → T

  X 必须先满足三条路之一（按优先级）：
    ① X.operator co_await()  →  ② operator co_await(X)  →  ③ X 自己就是 awaiter
  走 ③ 时三个方法【必须齐全】，少一个就编译不过；三条都不满足 = X 不可 co_await
  （反用：刻意不提供三件套，就能在编译期关掉 co_await 入口，见第 8 节末尾）

  await_ready() 返 false 的那一刻，协程【就已视为挂起】——不是等 await_suspend 返回
  → 所以「await_suspend 没返回就 resume」不是 UB。resume 的 UB 只有两条：
     ① resume 一个【没在挂起】的协程（例如正在运行）  ② resume 一个停在 final suspend point 的协程
    它的真问题是【重入】（await_resume 抢在 await_suspend 返回前跑、awaiter 可能已被析构），见第 8 节

promise_type 的钩子（名字编译器写死，不能改）：
  get_return_object / initial_suspend / final_suspend / unhandled_exception   ← 必需
  return_void 或 return_value（二选一）                                       ← 必需

  这些钩子必须【嵌套在协程函数的返回类型里】（返回类型::promise_type）
  → 所以返回类型本身也不可省，哪怕它是个空类（第 3.1 节有编译实证）

最容易搞混的三条：
  · await_ready() == true  → 跳过 await_suspend，直接 await_resume
  · await_suspend() 返回 false → 协程不挂起，继续往下跑
  · co_return 本身不挂起；挂起发生在它之后自动执行的 final_suspend() 里
```

---

## 1. 为什么需要协程

场景：程序里有两个任务，A 要等网络（3 秒），B 是纯本地计算可以立刻做。

```cpp
void read_from_network()      // 模拟耗时 IO
{
    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::cout << "网络 IO 结束\n";
}

void do_other_work()          // 本可立刻执行
{
    std::cout << "执行其他任务\n";
}

int main()
{
    read_from_network();      // ← 阻塞线程 3 秒
    do_other_work();          // ← 明明可以做，却只能排队
}
```

逻辑上没错，但执行效果很差：等待网络的那 3 秒里线程什么都干不了。

**根因**：普通函数一旦开始执行，就必须一路执行到 `return`。需要等外部事件时，只能阻塞线程等，**执行权无法主动让出**。

一句话：**普通函数不能暂停。**

协程就是补这一点的：**可暂停 / 可恢复的函数**。遇到需要等的地方先暂停、把执行权交出去，条件满足后再从暂停的位置接着跑。

两类适用场景：

| 场景 | 特征 | 协程的作用 |
|---|---|---|
| IO 密集型多任务 | 程序频繁等待网络 / 磁盘 / 数据库 | 等待期间不阻塞线程，线程去干别的活 |
| 惰性计算 | 计算可拆成多阶段，结果按需消费（无限序列、分批处理、流式） | 产出一个阶段性结果就暂停，等外部真要下一步再继续 |

**注意边界**：协程不创造并发、不缩短等待时间。它解决的是「等待期间执行权被一个函数霸占」的问题。

---

## 2. 判定规则：什么才算协程

两个条件**同时**满足，缺一个都不是协程：

1. **函数体内部**：至少出现 `co_return`、`co_yield`、`co_await` 中的一个。
2. **返回类型**：必须有合法的嵌套 `promise_type`（缺失则编译报错）。

### 反例：返回类型对了，函数体不对

```cpp
struct CoroRAII
{
    struct promise_type
    {
        CoroRAII get_return_object();
        std::suspend_always initial_suspend()    { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}
    };

    std::coroutine_handle<promise_type> handle;
    void resume() { handle.resume(); }
    ~CoroRAII() { if (handle) { handle.destroy(); } }
};

CoroRAII my_coroutine()
{
    std::cout << "协程执行\n";
}   // ★ 一个 co_* 都没有 → 这是【普通函数】，编译器不会为它建帧
```

调用它会发生什么：

```cpp
CoroRAII coro = my_coroutine();
// 编译器没把它当协程 → 没有帧 → get_return_object() 从没被调用
// coro.handle 是默认构造的空 handle（或未初始化）
coro.resume();     // ★ 对空 handle resume → 未定义行为
// ~CoroRAII 里 destroy() 同理
```

**这是阅读任何协程教程时的第一个检查点：如果示例函数体里没有 `co_*`，那个示例是假的。**

---

## 3. promise_type：编译器开给你的必填表

编译器改写协程时有一堆它自己不知道的事：结果放哪？创建后立刻跑还是停住？跑完销毁还是留着？上层在等我的时候我怎么找到它？异常怎么办？

所以它**点名要求**返回类型里必须有一个叫 `promise_type` 的内嵌类型，然后去调里面**名字写死**的钩子。

| 钩子 | 必需性 | 何时被调 | 决定什么 |
|---|---|---|---|
| `get_return_object()` | ★必需 | 帧和 promise 构造完之后、`initial_suspend` 之前 | 交给调用者的那个「返回对象」怎么造 |
| `initial_suspend()` | ★必需 | 协程体执行之前 | 创建后立刻跑（`suspend_never`）还是先停住（`suspend_always`） |
| `final_suspend()` | ★必需 | 协程体结束之后 | 结束后销毁帧（`suspend_never`）还是留住帧（`suspend_always` / 自定义 awaiter） |
| `return_void()` | ★二选一 | 执行 `co_return;` | 无返回值收尾 |
| `return_value(T)` | ★二选一 | 执行 `co_return v;` | 有返回值收尾，值存哪由你定 |
| `unhandled_exception()` | ★必需 | 协程体抛出的异常没被 catch | 异常落点 |
| `yield_value(T)` | 可选 | 执行 `co_yield v;` | 产出中间值（返回值决定挂不挂） |
| `await_transform(X)` | 可选 | 每次 `co_await X` | 把 X 转成 awaiter |

三条硬约束：

1. **`return_void` 与 `return_value` 互斥**——同时声明、或都不声明，编不过。
2. 协程体里写了 `co_return v;` 就必须有 `return_value`；只写 `co_return;` 就要 `return_void`。**这也正是 `Task<void>` 必须另写一个 promise_type 的原因**——一个 promise 不可能同时满足两种收尾。
3. 钩子名字、参数类型都由标准写死，**不能改名、不能少**。

### 最小可编译版本

```cpp
struct Minimal
{
    struct promise_type
    {
        Minimal get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() {}
    };
};

Minimal Hello() { co_return; }   // ✔ 就是地板：能编译、能跑，但什么都拿不到
```

`Task<T>`、`CoroRAII`、`Generator` 全部是在这块地板上往上加能力。

### 3.1 最外层的类型不可省（编译实证）

「最小形式是不是只要 `promise_type`？」——**不是。最外层那个类型是结构性的必需，它不在上面「5 个钩子」的清单里，但不能删。**

原因：编译器找钩子的路径是 `std::coroutine_traits<返回类型>::promise_type`，而标准库的默认实现就是取 `返回类型::promise_type`（MSVC `<coroutine>` 第 41-46 行）：

```cpp
template <class _Ret>
struct _Coroutine_traits<_Ret, void_t<typename _Ret::promise_type>> {
    using promise_type = typename _Ret::promise_type;      // :42  默认就是取成员的
};

template <class _Ret, class...>
struct coroutine_traits : _Coroutine_traits<_Ret> {};      // :46
```

所以「最外层类型」的唯一职责就是**当 `promise_type` 的挂载点**。

**实证（MSVC 14.44.35207，`/utf-8 /std:c++20 /W4 /EHsc`）：**

| 实验 | 写法 | 结果 |
|---|---|---|
| 1 | 删掉外层，5 个钩子直接放返回类型上 | **编译失败**：`error C2039: "promise_type": 不是 "std::coroutine_traits<Minimal>" 的成员` |
| 2 | 外层 + 嵌套 `promise_type`（就是上面那段） | **编译并运行成功** |
| 3 | 删掉外层，但特化 `std::coroutine_traits<Minimal, Args...>` | 编译并运行成功 |

实验 3 说明「删掉外层」在语言上有一条路，但代价是**额外写 6 行特化 + 把「返回类型」和「promise 类型」强行合并成同一个类型**——它不是更小，是更绕。**所以外层留着。**

**顺带一个分界点（L0 阶段的外层还不是 RAII）**：上面这段的 `get_return_object()` 写的是 `return {};`，说明**外层此刻没有任何状态**，纯粹是个挂载点。等到 `Task<T>` 那种写法出现：

```cpp
Task get_return_object() noexcept
{
    return Task(std::coroutine_handle<promise_type>::from_promise(*this));
}
```

它开始**必须**把 handle 交出去——这时外层才变成真正的 RAII 容器（析构里 `handle_.destroy()`）。**`return {}` 变成 `return Task(handle)` 这一行，就是「外层从空壳变成 RAII」的分界线。RAII 是后加的功能，不是外层存在的理由。**

---

## 4. 协程创建流程（编译器的完整动作）

```cpp
Task<int> t = RunEcho(channel);
```

这一行底下发生 8 件事：

```
① 分配协程帧
     优先找 promise_type::operator new(size, 函数参数...)
     找不到 → 退到全局 ::operator new(size)
② 在帧里就地构造 promise 对象
③ r = promise.get_return_object()      ★ 返回对象在这造好了，但先存在编译器临时量里
④ co_await promise.initial_suspend()
     suspend_always → 停在这里，控制权交回调用者
                      （此刻 ③ 的 r 才真正赋给 t）★ 惰性就靠这一步
     suspend_never  → 不挂，继续
⑤ 执行协程体
⑥ 收尾：
     正常走到结尾或 co_return      → promise.return_void() / return_value(v)
     抛出且未被 catch              → promise.unhandled_exception()
⑦ co_await promise.final_suspend()
⑧ 若 final_suspend 没有真的挂起 → 先析构 promise，再调 operator delete 释放帧
```

三个必须记住的次序：

- **④ 的 `co_await` 不是调用方写的，主语是协程自己。** 编译器把它插在协程体第一个语句之前，它是「起跑线」；而调用方那侧写的 `co_await task`（原 `rpc_task.h:148-164`，**已移除，见 13.6**）是**反方向**的动作——它是 resume 的扳机，不是 suspend。
- **③ 在 ④ 之前。** 所以即便是惰性协程（`initial_suspend = suspend_always`），返回对象也已经造好了，你手上能拿到一个「停在起跑线」的句柄。
- **⑥ 的异常不会逃出协程。** 编译器把协程体包在一层 try/catch 里，catch 到的异常只送到 `unhandled_exception()`。
- **`new` 是编译器做的，`delete` 得你自己安排。** 这就是所有宿主类型（`Task` / `CoroRAII` / `Generator`）都必须在析构里 `handle.destroy()` 的唯一原因。

---

## 5. coroutine_handle：协程的唯一凭证

标准库给的轻量工具，本质就是一个指针包装：

```cpp
constexpr void* address() const;                    // 帧地址
static coroutine_handle from_address(void* addr);
static coroutine_handle from_promise(Promise& p);   // 从 promise 反推帧地址

bool done() const;                                  // 是否已到 final suspend point
void resume() const;                                // 恢复
void destroy() const;                               // 销毁帧
Promise& promise() const;                           // 从帧里掏出 promise
operator coroutine_handle<>() const;                // 有类型版 → 无类型版
```

`from_promise(*this)` 的含义用一句话说：**「我人在帧里，你去把整个帧的地址给我。」** promise 是帧的一部分，所以拿 promise 的地址能反推出帧的地址。

### 纠一处常见误读

> 有些教程说「`std::coroutine_handle` 会在内部创建一块存储空间用于保存协程状态」。

**不对。** 分配帧的是**编译器**（第 4 节第 ① 步），`coroutine_handle` 只是指向那块内存的**轻量指针**——它不拥有帧、也不知道帧多大、更不负责释放。

### 但「必须 RAII 持有」这句是对的，而且很重要

`coroutine_handle` 是**拷贝自由的裸指针语义**：复制一份 handle 不会复制帧，两个 handle 指向同一帧。所以真实代码里不能裸着用它：

```cpp
class Host
{
public:
    explicit Host(std::coroutine_handle<promise_type> handle) : handle_(handle) {}

    Host(const Host&) = delete;                       // ★ 禁止拷贝，否则双重 destroy
    Host& operator=(const Host&) = delete;

    Host(Host&& other) noexcept
        : handle_(std::exchange(other.handle_, nullptr))   // ★ 移动时把对方置空
    {
    }

    ~Host()
    {
        if (handle_) { handle_.destroy(); }           // ★ 谁持有谁销毁
    }

private:
    std::coroutine_handle<promise_type> handle_{};
};
```

对应本仓库 `nebula/rpc/rpc_task.h:57-88`：`~Task()` 销毁帧，拷贝构造被 `delete`，移动构造用 `std::exchange` 把源对象置空。

---

## 6. 协程帧（coroutine frame）

编译器为协程生成的一块**独立存储区**，用来保存跨 `co_await` / `co_yield` 边界必须保留的全部状态。

### 内容布局

```
堆上的帧
├── promise 对象                     ← handle.promise() 摸得到
├── 函数参数的副本
├── 跨挂起点存活的局部变量            ← 挂起后不能死的东西都在这里
├── co_await / co_yield 的临时 awaiter 对象  ← 编译器物化进来的
├── 暂停点编号（恢复到哪一行）
└── 恢复 / 销毁用的簿记信息
```

**「局部变量为什么能活过挂起」的答案就在这一行。** 普通函数返回时栈帧销毁、局部变量全死；协程挂起时栈帧弹出，但局部变量在堆上的帧里，所以活着。这就是协程版的「状态机」不会退化成手工状态机的根本原因。

### 自定义分配

默认走 `new` / `delete`。高频短命协程场景（例如每秒创建上万个）可以接管：

```cpp
struct promise_type
{
    static void* operator new(std::size_t size)
    {
        return pool().Allocate(size);
    }

    static void operator delete(void* ptr, std::size_t size) noexcept
    {
        pool().Free(ptr, size);
    }
    // ... 其余钩子
};
```

要点：

- `size` 是**编译器算好的完整帧大小**，你不要自己估。
- `operator new` 和 `operator delete` **必须成对提供**，只给一个行为未定义。
- 只有「协程数量极大 + 生命周期极短 + 分配已成瓶颈」时才值得做。默认分配先别动，复杂度换来的收益通常不划算。

---

## 7. 四种挂起点，别混在一起

| 挂起点 | 谁触发 | 返回什么 | 决定什么 |
|---|---|---|---|
| `initial_suspend()` | 编译器在协程体前自动 `co_await` | awaiter | 创建后跑不跑（惰性 or 立即） |
| `final_suspend()` | 编译器在协程体后自动 `co_await` | awaiter | 结束后帧死不死 |
| `co_await X` | 你写的 | X 必须满足 awaitable 协议 | 等不等、挂起后谁接手 |
| `co_yield v` | 你写的 | ≡ `co_await promise.yield_value(v)` | 产出中间值 + 可能挂起 |

`co_yield` 的那条等价关系很关键：**它不是新机制，就是一次 `co_await`**，只不过被等的东西是 `yield_value()` 的返回值。

---

## 8. awaitable 协议（本节是精华）

`co_await X;` 不是函数调用，是**编译器语法**。编译器会对 X 依次调三个方法：

| 编译器问 | 方法 | 返回值 | 语义 |
|---|---|---|---|
| ① 现在就好了吗？ | `await_ready()` | `bool` | `true` → **跳过挂起**，直接跳到 `await_resume()`；`false` → 进 ② |
| ② 我要挂起，续点你拿着——**接下来该谁跑？** | `await_suspend(handle)` | `void` | 挂起，控制权回到「把本协程 resume 起来的那个人」（通常是事件循环） |
| | | `bool` | `false` → **不挂起**，继续执行 `await_resume()`；`true` → 挂起 |
| | | `coroutine_handle<>` | 挂起，并把控制权**直接交给这个 handle**（对称转移，见 12.2） |
| ③ 结果给我 | `await_resume()` | `T` | 整个 `co_await` 表达式的值 |

三条最容易记混的：

```
· await_ready() 返回 true  → await_suspend() 根本不会被调用
· await_suspend() 返回 false → 协程不挂起（这个返回值不表示"挂起成功"！）
· await_suspend(handle) 的参数 handle 是【我自己】，不是"等我的人"
```

### 挂起发生在哪一刻？以及 `resume` 的合法边界（高频误解）

**结论先行：协程在进入 `await_suspend` 之前就已经是「挂起态」，不是等它返回之后才算挂起。**

标准原文（`[expr.await]/5.1`）：当 `await_ready()` 的结果为 `false` 时 ——

> the coroutine is **considered suspended**.

所以 `co_await X;` 的展开是：

```
await_ready() == false
  │
  ├──【协程此刻已被视为挂起】          ← 分界点在这一刻
  │
  ├── 求值 await_suspend(handle)       ← 句柄此刻已可合法交给别人 resume
  │
  └── 按返回值分三路：
        void             → 控制权交回 caller/resumer（协程保持挂起）
        bool == false    → 协程被 resume（不挂）
        coroutine_handle → 立刻 resume 那个 handle（对称转移，见 13.2）
        抛异常            → 协程被 resume，异常立刻重抛
  （被 resume 时）马上调 await_resume()，它的值 = 整个 co_await 表达式的值
```

**推论：「`await_suspend` 还没返回就 resume」不是未定义行为。** cppreference 明写：

> Note that the coroutine is **fully suspended before entering** `awaiter.await_suspend()`.
> Its handle **can be shared with another thread and resumed before the `await_suspend()` function returns**.

`resume()` 的硬前置条件只有两条（`[coroutine.handle.resume]`）：

```
UB ①  *this 指向的协程【不在挂起状态】（比如正在运行）
UB ②  协程挂在 final suspend point 上
```

「`await_suspend` 正在跑」既不满足 ①（此刻协程已被视为挂起），也不满足 ② —— **它不在 UB 名单里**。

**真正的危险是另外三条，都不是「resume 本身」：**

| # | 危险 | 机制 | 对策 |
|---|---|---|---|
| 1 | **重入** | 同线程同步 resume：协程在 `await_suspend` 的栈上接着跑 → `await_resume()` 会在它**还没返回**时执行；若协程走完这个 full-expression，**awaiter 临时对象（住在帧里）会被析构** → 此后 `await_suspend` 再碰 `*this` 就是在已析构对象上操作 | 句柄一发布出去，就**把 `*this` 当成已销毁**，发布之后不再访问成员 |
| 2 | **数据竞争** | 跨线程并发 resume。标准原文：*A concurrent resumption of the coroutine may result in a data race* | 发布侧至少 release、恢复侧至少 acquire |
| 3 | **栈增长** | 直接 `handle.resume()` 每挂起/恢复一轮就叠一层栈 | 返回 `coroutine_handle` 让运行库做尾调用（对称转移）。**这才是「别在 `await_suspend` 里直接 resume」的主要动机** |

cppreference 示例里那行注释原文是 `// Potential undefined behavior: accessing potentially destroyed *this` —— 它指的是**访问 `*this`**，不是 resume 本身。

**本仓库落地**：`RpcAwaiter::await_suspend`（`nebula/rpc/rpc_awaiter.h:74-103`）用 `QueueInLoop` 把恢复推迟到当前栈完全展开之后，躲开的正是第 1 条；`channel_->CallMethod(...)` 被放在**最后一句**，正好是「发布句柄后不再碰 `*this`」这条纪律的落地。

---

### `std::suspend_always` / `std::suspend_never` 的全部源码

```cpp
struct suspend_always
{
    bool await_ready() const noexcept { return false; }   // 永远没好 → 一定挂起
    void await_suspend(std::coroutine_handle<>) const noexcept {}
    void await_resume() const noexcept {}
};

struct suspend_never
{
    bool await_ready() const noexcept { return true; }    // 永远好了 → 从不挂起
    void await_suspend(std::coroutine_handle<>) const noexcept {}
    void await_resume() const noexcept {}
};
```

**总共 10 行。** 「awaiter」不是框架、不是概念，就是「能被 `co_await` 的东西」——一个提供了这三个方法名的普通对象。

### `co_await X` 的解析顺序：编译器怎么找 awaiter

`co_await` 后面可以跟**任意表达式**（对象、函数返回的临时量、引用都行）。编译器按下面的顺序找 awaiter：

```
① X.operator co_await()        → 成员版 operator co_await（最优先）
② operator co_await(X)         → 自由函数版，参数是 X
③ X 自己就是 awaiter            → 要求 X 同时有 await_ready / await_suspend / await_resume
      ↑ 三条都不满足 → 编译错误：X 不是 awaitable
```

三条路各自的要求，以及「少写一个会怎样」：

| 路 | 必须实现 | 不满足时 |
|---|---|---|
| ① 成员 `operator co_await()` | 1 个函数，返回 awaiter | 落到 ② |
| ② 自由 `operator co_await(X)` | 1 个函数 | 落到 ③ |
| ③ 直接当 awaiter | `await_ready()` + `await_suspend(handle)` + `await_resume()`，**三个都得有** | **编译不过** —— 三个方法没有默认实现，少一个就报"缺成员" |

**常见疑问：能不能只写 `await_suspend`？** 不能。`await_ready()` 没有默认值 —— 它决定"挂不挂"，编译器不会替你猜。**三个方法是一个整体**，要就走 ③ 全给，要就不用 ③。

### 反用：删掉三个方法 = 编译期关闭 `co_await` 入口

既然"三个成员齐全"是走 ③ 的唯一条件，那么**反过来删掉它们，就等于把这个类型焊死在"不可 await"上**：

```cpp
class Task
{
    struct promise_type { /* ...五钩子... */ };

    void Start();
    T Result();
    bool Done();
    // 刻意不提供 await_ready / await_suspend / await_resume
};

Task<int> t = RunEcho(ch);
co_await t;      // ✘ 编译失败：Task 不是 awaitable
```

这是个有用的设计手段：**当"等一个已经启动的东西"这件事你根本不打算支持时，别给它一个语义含混的运行时行为，直接让它在编译期消失。**

**与运行期断言的关键差别（值得记住）：**

```
assert(!started_)   → NDEBUG（Release）下是空操作 → Release 里违约会静默走进错误语义
删掉三件套          → 任何构建、任何优化级别都挡住，零运行时成本、零状态
```

**编译期失败 > 运行期断言。能用前者就别用后者。** 本仓库最终就是这么做的，见 13.6。

---

## 9. co_return / co_yield / co_await 对照

| 语法 | 结束协程？ | 调 promise 的谁 | 挂起吗 |
|---|---|---|---|
| `co_return;` | ✅ 结束 | `return_void()` | 不挂，直接进 `final_suspend()` |
| `co_return v;` | ✅ 结束 | `return_value(v)` | 不挂，直接进 `final_suspend()` |
| `co_yield v;` | ❌ 不结束 | `yield_value(v)` | 由 `yield_value` 返回的 awaiter 决定 |
| `co_await X;` | ❌ 不结束 | —（X 的三个方法） | 由 X 的三个方法决定 |

**`co_return` 本身不挂起。** 大家以为「`co_return` 会暂停」是错觉——真正挂起的是它之后编译器自动插的 `final_suspend()`。所以「协程逻辑结束」和「协程帧被销毁」是**两件事**，中间隔着一个 `final_suspend()`。

---

## 10. await_transform

在 `promise_type` 里定义 `await_transform(E)` 之后，`co_await E` 会被改写成 `co_await promise.await_transform(E)`：

```cpp
struct promise_type
{
    auto await_transform(int value)
    {
        return MyAwaiter{value};      // 把 int 包装成 awaiter
    }
};

CoroManager my_coroutine()
{
    int ret = co_await 100;           // 100 本来不是 awaiter，被 transform 接住了
    co_return;
}
```

用途：给协程开一个「`co_await` 什么」的**转换层 / 白名单**。典型例子是 asio 用它把当前 executor 塞进 awaiter。

**本仓库未使用**：`rpc_task.h` 和 `rpc_awaiter.h` 的 promise 里都没有 `await_transform`，所以业务协程只能 `co_await` 真正的 awaiter。本仓库当前**唯一**可 `co_await` 的对象是 `RpcAwaiter`（`Task` 已按 13.6 移除 await 能力），`co_await 100` 会直接编不过。

---

## 11. 案例一：惰性计算（Generator）

### 原始动机：普通函数版的「伪生成器」

```cpp
int fibonacci()
{
    static int prev1 = 0;
    static int prev2 = 1;

    const int current = prev1;
    const int next = prev1 + prev2;
    prev1 = prev2;
    prev2 = next;
    return current;
}
```

**状态被迫提升到 `static`**——因为函数返回后局部变量就死了，而序列的「进度」必须留下来。这跟回调版异步里被迫建 `CallContext` 结构体是**同一类病**：状态活不过一次返回。

### 协程版：进度留在帧里

```cpp
#include <coroutine>
#include <iostream>
#include <utility>

class IntGenerator
{
public:
    struct promise_type
    {
        int current{0};

        IntGenerator get_return_object()
        {
            return IntGenerator(std::coroutine_handle<promise_type>::from_promise(*this));
        }

        std::suspend_always initial_suspend() noexcept { return {}; }   // 惰性：创建后不跑
        std::suspend_always final_suspend() noexcept { return {}; }     // 帧留住，由宿主销毁

        std::suspend_always yield_value(int value) noexcept             // 产出后挂起
        {
            current = value;
            return {};
        }

        void return_void() noexcept {}
        void unhandled_exception() { std::terminate(); }
    };

    explicit IntGenerator(std::coroutine_handle<promise_type> handle) : handle_(handle) {}

    IntGenerator(const IntGenerator&) = delete;
    IntGenerator& operator=(const IntGenerator&) = delete;

    IntGenerator(IntGenerator&& other) noexcept
        : handle_(std::exchange(other.handle_, nullptr))
    {
    }

    ~IntGenerator()
    {
        if (handle_) { handle_.destroy(); }
    }

    bool Next()                                    // 推进一步；返回是否还有值
    {
        handle_.resume();
        return !handle_.done();
    }

    int Value() const noexcept
    {
        return handle_.promise().current;
    }

private:
    std::coroutine_handle<promise_type> handle_{};
};

IntGenerator Fibonacci()
{
    int prev1 = 0;
    int prev2 = 1;

    while (true)
    {
        const int current = prev1;
        const int next = prev1 + prev2;
        prev1 = prev2;
        prev2 = next;

        co_yield current;      // 产出 → 挂起 → 等下次 resume
    }
}

int main()
{
    IntGenerator generator = Fibonacci();

    for (int i = 0; i < 10 && generator.Next(); ++i)
    {
        std::cout << generator.Value() << '\n';
    }
}
```

**对照看三处**：

```
① prev1 / prev2 从 static 变成普通局部变量  ← 它们现在住在帧里，跨挂起不死
② while(true) 没有终止条件                 ← 无限序列靠"按需 resume"，不需要边界
③ 没有 return，只有 co_yield               ← 一次调用产出一个值，而不是结束
```

### 这里有个必须避开的坑

`final_suspend()` **不能**用 `std::suspend_never`：

```cpp
// ✘ 错误组合
std::suspend_never final_suspend() noexcept { return {}; }   // 结束时帧当场销毁
~IntGenerator() { if (handle_) { handle_.destroy(); } }      // 又 destroy 一次
```

协程一旦自然结束 → 帧被自动销毁 → `handle_` 悬空 → 析构里再 `destroy()` → **双重释放（未定义行为）**。

这个 bug 很容易被「协程永不结束」掩盖：原文的 `fibonacci()` 是 `while(true)`，永远走不到 `final_suspend`，所以看起来没事。**只要给协程体加一句 `if (i > 10) { co_return; }`，立刻炸。**

**实测证据**（MSVC 14.44.35207 / `/std:c++20 /EHsc`，一个只有 3 个 `co_yield` 的有限协程）：

```
[bad] begin
[bad] coroutine finished, leaving scope
RUN_EXIT=-1073741819          ← 0xC0000005 ACCESS_VIOLATION，崩在宿主析构里
```

最后那句 `[bad] survived scope` 没打出来，进程直接死在作用域退出处。**这不是"理论上可能"，是必然。**

正确组合只有一种：**帧的寿命由宿主对象负责 → `final_suspend` 必须真的挂起 → 析构里 `destroy()`。**

---

## 12. 案例二：异步编程

### 12.1 骨架：事件循环 + Task + 一个 awaiter

协程异步的最小骨架就三块：

```
EventLoop            持有一个待恢复协程的队列，循环 pop 出来 resume
Task / promise_type  宿主：管帧的生死
某个 awaiter          把"等某个事件"翻译成 await_ready/await_suspend/await_resume
```

原文那个 `EventLoop` 骨架方向是对的：

```cpp
class EventLoop
{
public:
    void add_task(std::coroutine_handle<> handle)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(handle);
        }
        ++pending_;
    }

    void run()
    {
        while (true)
        {
            std::coroutine_handle<> handle;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this] { return !queue_.empty() || pending_ == 0; });

                if (queue_.empty() && pending_ == 0)
                {
                    break;
                }

                handle = queue_.front();
                queue_.pop();
            }

            handle.resume();

            if (handle.done())
            {
                --pending_;
                handle.destroy();
            }
        }
    }

private:
    std::queue<std::coroutine_handle<>> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t pending_{0};
};
```

逻辑对：**协程挂起时把控制权还给循环；循环在别的任务执行期间推进队列；外部事件到了再把协程 push 回队列恢复。**

### 12.2 "到底谁是 awaitable" —— `AsyncSleep` 完整写法

原文这里被截断了，补一份完整的（用事件循环定时器，**不起线程**）：

```cpp
struct AsyncSleep
{
    std::chrono::milliseconds duration;
    nebula::net::EventLoop* loop;

    bool await_ready() const noexcept
    {
        return false;                       // 睡 0 毫秒也得走一趟，不能"已经好了"
    }

    void await_suspend(std::coroutine_handle<> handle)
    {
        // 把"到点叫醒我"注册进事件循环的定时器，不占线程、不阻塞 loop
        loop->RunAfter(duration, [handle] { handle.resume(); });
    }

    void await_resume() const noexcept
    {
    }
};
```

用法：

```cpp
nebula::rpc::Task<int> Both(nebula::net::EventLoop& loop)
{
    co_await AsyncSleep{std::chrono::seconds(5), &loop};   // 5 秒，不阻塞
    co_await AsyncSleep{std::chrono::seconds(3), &loop};   // 再 3 秒，不阻塞
    co_return 0;
}
```

**注意 `await_suspend` 返回 `void` 而不是 handle**——因为它等的是「一个外部事件」，不是「另一个协程」。控制权还给事件循环，等定时器到点了由 `handle.resume()` 来叫醒。这跟「等另一个协程」那种写法（返回 handle 做对称转移）是**两种不同形状**：

| | 等另一个协程 | 等一个外部事件（`AsyncSleep` / `RpcAwaiter`） |
|---|---|---|
| `await_suspend` 返回 | `coroutine_handle<>` → 控制权交给下层 | `void` → 控制权还给事件循环 |
| 谁负责叫醒 | 下层跑完后的 `final_suspend` | 完成回调 / 定时器 |
| 续点存哪 | 下层 promise 的 `continuation` 里 | 自己的成员（如 `ResumeGuard`）或闭包捕获 |
| 本仓库 | **无实例**（原 `Task` 的 await 能力已按 13.6 移除） | `RpcAwaiter`（`rpc_awaiter.h:74`） |

### 12.3 别把「不阻塞」说成「变快」

原文例子是 `task1(); task2();` 串行 5 + 3 = 8 秒。**协程版总耗时还是 8 秒。**

协程改变的是**等待期间线程能不能干别的活**，不是总时长。要缩短总时长必须**并发**（同时挂起两个任务），那是上层编排的事，需要 `when_all` / `gather` / `race` 这类组合器——本仓库目前没有。

所以准确表述是：

```
✘ 协程让异步变快了
✔ 协程让"等待期间执行权被霸占"变成"等待期间执行权可让出"
✔ 顺带把"多步异步串起来"从手写状态机变回线性代码
```

---

## 13. 三个原文没讲、但面试常问的点

### 13.1 awaiter 不是协程，它没有帧

`RpcAwaiter` / `AsyncSleep` 这类 awaiter 是**普通类**，一个 `co_*` 都没有。它的三个方法都是普通函数，`await_suspend` 里那句 `CallMethod(...)` 就是一次普通调用。

**它住在别人的帧里。** 编译器在展开 `co_await X;` 时，会把 `X` 这个临时对象**物化进当前协程的帧**（而不是放在栈上），因为要活过挂起：

```
帧（协程函数自己的）
├── promise
├── 局部变量
├── [ awaiter 的槽位 ]      ← co_await 只是往这个已存在的槽位里构造对象，不新开帧
└── 暂停点
```

**判别标准只有一条**：函数体里有没有 `co_await` / `co_return` / `co_yield`。有 → 是协程、有自己的帧；没有 → 普通函数，走栈或作为别人的成员。

### 13.2 对称转移：跑完怎么叫醒等待者

嵌套 `co_await` 时，被等待的协程跑完了，**必须有人去恢复等待它的那个协程**。这个「叫醒」动作不能靠「回到事件循环再排一次队」，否则外层会永久卡住。

做法是让 `final_suspend()` 返回一个自定义 awaiter，在它的 `await_suspend` 里返回等待者的 handle：

```cpp
struct FinalAwaiter
{
    bool await_ready() noexcept { return false; }   // 必须真的挂起，否则帧会当场销毁

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<promise_type> handle) noexcept
    {
        const std::coroutine_handle<> waiting = handle.promise().continuation;

        if (waiting != nullptr)
        {
            return waiting;                          // ★ 直接跳到等待者，不经过调度器
        }

        return std::noop_coroutine();                // 没人等 → 停在原地，等宿主来取结果
    }

    void await_resume() noexcept {}
};
```

配套还需要在「被 `co_await` 时」存下等待者：

```cpp
std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) noexcept
{
    handle_.promise().continuation = continuation;   // ← 记下"谁在等我"
    started_ = true;
    return handle_;                                  // ← 控制权直接交给下层
}
```

**一个写、一个读，缺一个外层就永远醒不过来。** 本仓库曾按这个模式实现过（`Task::await_suspend` 写 `continuation` / `FinalAwaiter` 读 `continuation`），**现已整体移除，见 13.6**。这套模式现在只剩概念层价值：以后要做「协程 await 协程」，它就是骨架。

如果 `final_suspend` 只用 `std::suspend_always`，后果是：

```
Inner 跑完 → suspend_always 挂住（帧保住了，这部分没问题）
           → 但它的 await_suspend 返回 void
           → 没人回答"接下来该谁跑"
           → 控制权回到事件循环
           → ❗ Outer 永远停在 co_await 那一行
```

**所以「留住帧」和「叫醒等待者」是两件事，`suspend_always` 只做到了第一件。**

> **本仓库最终就选了 `suspend_always`**（见 13.6）：因为不存在嵌套 `co_await Task`，「叫醒等待者」没有服务对象 —— 没人被叫醒，就不需要这套机制。代价是永久放弃嵌套组合。

### 13.3 帧的生死可以当成业务信号用

因为 `handle.destroy()` 会销毁帧，而帧里住着局部对象（包括 awaiter），所以**销毁帧会连坐执行这些对象的析构函数**：

```
普通函数   void F() { Widget w; }        函数返回          → 栈帧销毁 → ~Widget 自动跑
协程       { co_await A; }               handle_.destroy() → 帧销毁   → ~A      自动跑
```

于是可以做一个反直觉但很有用的设计：**让 awaiter 的析构函数去取消在途操作**。

```cpp
~RpcAwaiter()
{
    if (guard_ != nullptr && guard_->handle.exchange(nullptr) != nullptr)
    {
        controller_.StartCancel();      // 帧被丢弃 = 这次调用不要了
    }
}
```

`exchange(nullptr)` 一行同时答两个问题：

- **票据还在不在我这** → 返回非空 = 协程还挂着
- **该不该取消** → 非空才取消；空说明已经正常完成，不能去取消一个已完成调用

（用 `exchange` 而不是 `load`，是因为 `load` 之后到用之前有窗口，两方可能都看到非空 → 双重 `resume`：同一协程被两条执行流同时推进 = 数据竞争；若它此刻不在挂起态、或已停在 final suspend point，则是 UB。）

对应本仓库 `rpc_awaiter.h:61-67`。这也是「`Task` 和 `RpcAwaiter` 互相不认识，为什么 `Task` 能销毁 `RpcAwaiter`」的答案——**它们之间没有引用，唯一的联系是「住在同一个帧里」。**

### 13.4 宿主类型的 await_suspend 陷阱（本仓库曾存在，已移除）

> **状态：已按 13.6 整块删除。** 本节保留为设计复盘 —— 它记录「为什么会踩到这个坑、以及为什么最终决定不修而删」。

`rpc_task.h` 原来那组 await 方法要同时服务两种进入方式：

```
A. 首次 co_await（惰性启动）→ 必须 resume 它
B. 已 Start() 过、正跑在半路 → 必须挂住等待者，等它自己跑完
```

现在只有一个 `started_` 标记却没用上（`await_suspend` 里无条件 `started_ = true` 并 `return handle_`），于是 B 路会出错：

```
t.Start()  → 跑到 co_await RpcAwaiter(...) → 挂起等网络
此时 协程 B: co_await t
  await_ready()   → t.done()==false → 不挂，进 await_suspend
  await_suspend(B)→ continuation = B（覆盖）
                  → return handle_  → 对称转移 → resume 从 RpcAwaiter 挂起点继续 ❗
                     → RpcAwaiter::await_resume() 当场执行（RPC 还没回来）
                        CallState()==Pending、Failed()==false（rpc_controller.h:55,63）
                        → 落到 rpc_awaiter.h:125 return std::move(response_)
                        → 返回一个默认构造的空响应（不抛异常，静默错误结果）
                     → 表达式结束 → ~RpcAwaiter() → guard 非空 → StartCancel() 把请求取消掉
```

三个问题：

| # | 问题 | 触发条件 |
|---|---|---|
| 1 | 对「已启动未完成」的 Task 返回 `handle_` → **提前 resume**，语义应是 `noop_coroutine()`（挂住等待者，等 `final_suspend` 叫醒） | Task 先 `Start()`、后被 `co_await` |
| 2 | `continuation` 是**单槽**，第二个等待者会覆盖第一个 → 先到者永久挂起 | 两个协程 `co_await` 同一个 Task |
| 3 | `handle_ == nullptr` 时 `await_ready()` 返 false（原 `:150`），随后 `await_suspend` 解引用空 handle | `co_await` 一个默认构造的 Task |

修复 1 的最小改动 —— **这是当时评估过的历史方案，最终没采用（13.6 选择整块删除）**，留下来是为了看清"修"和"删"的分界：

```cpp
// rpc_task.h 原 :154-159  改前
std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) noexcept
{
    handle_.promise().continuation = continuation;
    started_ = true;
    return handle_;
}

// 历史方案：改后（未采用）
std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) noexcept
{
    handle_.promise().continuation = continuation;

    if (started_)                          // 已在运行：不能再 resume，否则从挂起点提前蹿出去
    {
        return std::noop_coroutine();      // 挂住等待者，等 final_suspend 对称转移叫醒它
    }

    started_ = true;
    return handle_;                        // 只在这里做「惰性启动」
}
```

**为什么最后没走这条路**：它只修问题 1，问题 2（单槽 continuation）修不掉 —— 所以拿到的是"半个 join"，比没有更危险。而两个问题加起来指向同一个根因：**这个类型本就不该支持 await**。

**注意这个 bug 的性质是「还没被踩到」，不是「不存在」**：仓库里所有 `co_await` 等的都是 `RpcAwaiter`，没有一处 `co_await` 一个 `Task`（客户端全走顶层 `Start()`），所以 `Task::await_suspend` 在整个生命周期里**零调用**、一次都没执行过。一旦开始「协程 await 协程」，它立刻生效。**正因为零调用，13.6 才选择直接删掉而不是修它。**

### 13.5 `co_await Task` 的两种形状，以及为什么「支持 join」是半个 join

> 本节两种形状是**概念层**的。本仓库的 `Task` 已按 13.6 移除 await 能力，所以「形状二」在代码里不再存在 —— 留下来是为了讲清「如果要支持，代价是什么」。

同一句 `co_await task`，落在两种状态上语义完全不同：

| task 的状态 | `co_await` 干的事 | 这行语句的价值 |
|---|---|---|
| 未启动（惰性，停在 `initial_suspend`） | 登记 `continuation` + `return handle_` **顺带启动**它，再等它回来 | = 调用 + 启动 + 等待 + 取结果 + 异常传播，**一行顶回调版一整个状态机** |
| 已 `Start()`、跑在半路 | 只登记 `continuation` 然后挂住（**不该 resume**） | = **join**（等 + 取结果）。不需要结果就别写这行 |

于是两个常见质疑可以一次答掉：

- 「`co_await` 什么都不做」——**对，它本来就不做业务逻辑**。它是**接缝**，把「跨挂起的等待」从代码结构里拿掉（回调版必须把后半段搬进另一个函数、状态搬进结构体）。
- 「等待没有意义」——只在**不需要结果**的前提下成立。那种情况写 `co_await` 是用错，不是功能无用。

**为什么不该顺手做成完整 join（铁证在代码里）：**

```cpp
// ① 值只能取一次
T TakeValue()  rpc_task.h:124     return std::move(*handle_.promise().value);
//   值是 move 出来的。Task<int> 看不出问题（标量 move 等价于拷贝），
//   换成 Task<std::string> / Task<Message>，第二次 await 会静默拿到 moved-from 空值。

// ② 等待者只有一个槽（原 rpc_task.h:45，已随 13.6 移除）
std::coroutine_handle<> continuation{};
//   不是列表。第二个等待者覆盖第一个 → 先到者永久挂起。
```

所以 `if (started_) return std::noop_coroutine();` 只解决「不提前蹿出去」，拿到的是**半个 join**：两个 joiner 里一个拿到结果、另一个挂死或拿空值。**半个 join 比没有 join 更危险**（错在静默）。真要支持，得另起一个类型（`JoinableTask`：`continuation` 换列表 + 结果按拷贝/共享交付）。

**推论：把「一个 Task 只能被驱动一次」写成契约。** 概念上驱动路径有两条，二选一：

```
顶层：  task.Start(); → loop.Loop(); → task.Done() → task.Result()
        （examples/rpc_echo_coroutine_client.cpp:66-79）
嵌套：  T v = co_await Inner();     ← 唯一一次，且必须处于「未启动」状态
```

**本仓库最终只保留顶层那条**（13.6），所以下面这段违约处理也一并作废 —— 保留是为了说明"契约"该怎么想：

「已启动 + 被 `co_await`」= 违约。有两条处理路：运行期断言（像 `rpc_awaiter.h:77` 的 `AssertInLoopThread()` 那样），或**编译期直接关掉入口**（删掉三件套）。**本仓库选了后者，见 13.6。**

### 13.6 本仓库的最终选择：不实现 `Task::await_*`

13.4 / 13.5 分析了 `Task::await_*` 的三条陷阱（提前 resume、单槽 continuation、半个 join）。**最终决定不是修，而是删**，依据是本项目的实际用法：

```
网关用法：写一个协程函数 → task.Start() → 保住 Task 到响应回来之后再让它出作用域
真正需要 co_await 的对象：RpcAwaiter，不是 Task
```

既然全仓库零处 `co_await` 一个 `Task`，`Task` 就没有理由是可 await 的。删掉之后连带清掉的东西：

```
删  promise_type::FinalAwaiter（原 :20-43）    → final_suspend 改回 std::suspend_always
删  promise_type::continuation（原 :45）
删  await_ready / await_suspend / await_resume（原 :148-164）
留  initial_suspend = suspend_always（惰性：先建 Task，再由 Start() 启动）
留  return_value / unhandled_exception / ~Task / Start / Done / Result
```

**代价（认账）**：永久放弃「一个协程里等另一个协程函数」这个能力。本项目不需要 —— 在一个协程函数里顺序或并发发多个 RPC，用 `co_await RpcAwaiter` 就够了。**要加回来只是补 5 个方法**（`operator co_await` 那条路也行），成本很低。这正是它值得删的理由：不是"做不到"，是"用不上，而留着要付 bug 面"。

**删掉之后 `started_` 反而变干净了**：它唯一的职责变成「防止重复 `Start()` 把协程从挂起点再 resume 一次」，不再兼职 await 路径的状态。**这也说明之前的 `started_` 是个补丁标记 —— 它服务的两条驱动路径砍掉一条，它自己就单纯了。**

**`Task` 去掉 await 能力后，身份收窄成三件事**（理解它的正确框架）：

```
Task<T> = ① 编译器要求的壳（返回类型里必须有 promise_type）
        + ② 帧的生命周期契约（~Task → destroy；提前析构 = 取消 + 拆帧）
        + ③ 结果信箱（Result() 把 exception_ptr 重抛，是顶层唯一的错误出口）
```

注意 ② 的机制**不是**"Task 活着帧才活着"——帧是堆内存，没人 `destroy()` 就一直活着、`resume` 由完成回调驱动。**`Task` 是拔管的扳机，不是供氧机。** 所以纪律是「**别让 `~Task` 提前跑**」（别写成临时对象、别 `Start()` 后立刻出作用域），而不是"保管好 Task 好让帧别死"。

---

## 14. 去糟粕清单

| # | 位置 | 问题 | 正确做法 |
|---|---|---|---|
| 1 | 原文 2.1 `CoroRAII my_coroutine()` | 函数体里一个 `co_*` 都没有 → **它根本不是协程**；`CoroRAII` 那时也还没有 `promise_type`；`handle` 未初始化，`resume()` / `destroy()` 全是 UB | 换到 2.2 之后再写，并加上 `co_return;` |
| 2 | 原文 2.1 「`coroutine_handle` 会在内部创建存储空间」 | 分配帧的是**编译器**；handle 只是指向帧的轻量指针，不拥有、不释放 | 见本文第 5 节 |
| 3 | 原文 4.1 `Generator` 用 `suspend_never final_suspend` + 析构 `destroy()` | 协程自然结束时帧被自动销毁 → handle 悬空 → 析构再 `destroy()` = **双重释放**。靠 `while(true)` 掩盖了。**已实测**：有限协程跑到 `final_suspend` 后析构 → `0xC0000005` 访问违例 | `std::suspend_always final_suspend()`，见第 11 节 |
| 4 | 原文 4.2 `AsyncSleep` 用 `std::thread` 等待后 resume | 这是「把阻塞挪到另一个线程」，不是异步；每次等待一个线程的开销比等待本身还贵 | 注册进事件循环定时器，见 12.2 |
| 5 | 原文 2.2 / 2.3 / 4.1 的 `unhandled_exception` 只打日志；4.2 里直接 `std::exit(1)` | 打日志 = 异常被静默吞掉，调用者永远不知道失败；`exit(1)` = 直接杀进程，没法处理 | 存 `std::exception_ptr`，在取结果处 `rethrow_exception`。见 `rpc_task.h:44-47`、`124-132` |
| 6 | 原文 4.2 `EventLoop` 把裸 handle 塞队列、`resume` 后 `destroy` | 协程中途被丢弃时帧永久泄漏（没人管）；且释放时机和业务生命周期耦合 | 用 RAII 宿主（`Task`）持有帧，见第 5 节 |
| 7 | 原文 2.2 `CoroRAII` 只有 `resume()`，没有移动/拷贝语义处理 | 默认拷贝会复制 handle → 两个对象指向同一帧 → 双重 `destroy` | 删拷贝 + 移动时 `std::exchange` 置空，见第 5 节 |
| 8 | 原文 4.2 「提升线程的整体利用效率」配合 `task1(); task2();` | 例子是串行的，总耗时没变（8 秒）。原文没点破「协程省的是等待期空转，不是总时长」 | 见 12.3 |

另外两处可以补强、但不算错的地方：

- 原文 2.4 讲 `co_yield` 时没点出 `co_yield v;` ≡ `co_await promise.yield_value(v);`——点破这一句，`co_yield` 就不用单独记了（第 7 节）。
- 原文 2.5 讲完 awaitable 三件套后没提 `operator co_await()`——让 X 可被 `co_await` 其实有三条路（第 8 节末尾）。

---

## 15. 概念 → 本仓库代码 映射表

| 知识点 | 本仓库位置 | 备注 |
|---|---|---|
| `promise_type` 五钩子 | `nebula/rpc/rpc_task.h:18-48` | `Task<T>` 的 promise |
| `initial_suspend` = `suspend_always` | `nebula/rpc/rpc_task.h:28` | 惰性：创建后不跑，等 `Start()` |
| `final_suspend` = `suspend_always` | `nebula/rpc/rpc_task.h:34` | 只保住帧，不做对称转移（见 13.6） |
| `return_value(T)` | `nebula/rpc/rpc_task.h:39` | 值存进 `promise.value` |
| `unhandled_exception` | `nebula/rpc/rpc_task.h:44` | 存 `exception_ptr`，不吞不杀进程 |
| `coroutine_handle` 成员 | `nebula/rpc/rpc_task.h:134` | 帧的唯一凭证 |
| RAII 持有 + 移动置空 | `nebula/rpc/rpc_task.h:57-88` | `~Task` destroy / 删拷贝 / `exchange` |
| `await_ready/suspend/resume`（形状 A：等协程） | 曾 `nebula/rpc/rpc_task.h:148-164` | **已按 13.6 移除** |
| `continuation`（写 / 读） | 曾 `nebula/rpc/rpc_task.h:154-158` / `:20-43` | **已按 13.6 移除**，本仓库不做嵌套 await |
| 三件套齐全才算 awaiter | `nebula/rpc/rpc_awaiter.h:69-125` | 本仓库唯一可 `co_await` 的对象 |
| 删掉三件套 = 关闭 `co_await` 入口 | `nebula/rpc/rpc_task.h:12-13` | 编译期禁用，契约写在类头注释里 |
| `await_suspend` 返回 `void`（形状 B：等外部事件） | `nebula/rpc/rpc_awaiter.h:74-103` | 等网络回包，控制权还给 loop |
| `await_resume` 抛异常 | `nebula/rpc/rpc_awaiter.h:105-125` | 按终态造 `RpcError` |
| awaiter 无帧、住在别人帧里 | `nebula/rpc/rpc_awaiter.h` 全文件 | 库内零 `co_*` |
| 帧销毁连坐析构 → 取消信号 | `nebula/rpc/rpc_awaiter.h:61-67` | 析构即取消 |
| 队列延迟恢复（避免挂起前 resume） | `nebula/rpc/rpc_awaiter.h:88-100` | 见 `07_Coroutine_RPC.md` |
| `co_yield` / `yield_value` | 未使用 | Generator 场景本仓库没有 |
| `await_transform` | 未使用 | promise 里没定义 |
| 自定义帧分配 | 未使用 | 走默认 `new` / `delete` |
| `Task<void>` | 未实现 | promise 不能同时声明 `return_value` 和 `return_void` |
| `when_all` / `gather` | 未实现 | 上层编排能力，目前只能串行 `co_await` |

---

## 16. 自检题

1. 一个函数返回类型里有 `promise_type`，但函数体里没有 `co_*`。它是协程吗？调用它会拿到什么？
2. `await_ready()` 返回 `true` 时，`await_suspend()` 会被调用吗？
3. `await_suspend()` 返回 `false` 是什么意思？（提示：不是"挂起失败"）
4. `co_return 100;` 执行时，`final_suspend()` 和 `return_value(100)` 谁先被调用？
5. 为什么 `final_suspend` 不能返回 `std::suspend_never`，却又在宿主析构里写 `handle.destroy()`？
6. 一个 awaiter（比如 `RpcAwaiter`）有没有自己的协程帧？为什么？
7. `await_suspend` 返回 `handle`（对称转移）和返回 `void`（还给事件循环）这两种写法，差别意味着什么？（原 `Task` 用前者，已按 13.6 移除）
8. 如果 `final_suspend` 从 `FinalAwaiter` 换成 `std::suspend_always`（**本仓库现即如此**），嵌套的 `co_await` 会出什么问题？（提示：帧还在，但没人叫醒等待者）
9. `handle_.done()` 返回 `true` 究竟表示什么？惰性协程刚创建、还没 `Start()` 时它是 `true` 还是 `false`？如果 `final_suspend` 用 `suspend_never`，它还有意义吗？
10. 一个 `Task` 已经 `Start()` 过、正挂在 `co_await RpcAwaiter` 上，此时第二个协程 `co_await` 它，会发生什么？
11. 两个协程同时 `co_await` 同一个 `Task`，会怎样？（提示：看 `promise_type` 里 `continuation` 是几个槽）
12. 想让一个类型**没法**被 `co_await`，最干净的做法是什么？它比运行期 `assert` 好在哪？
13. `co_await X` 时，编译器按什么顺序找 awaiter？走「X 自己就是 awaiter」这条时，少写一个方法会怎样？
14. `await_suspend` 还没返回，另一个线程就 `resume()` 了这个协程 —— 是未定义行为吗？「协程何时算挂起」这个分界点落在哪一刻？
15. 那「在 `await_suspend` 里**同步** resume 自己」的风险是什么？为什么一旦把句柄发布出去，`await_suspend` 就不能再访问 `*this`？

<details>
<summary>参考答案</summary>

1. 不是协程，是普通函数。`get_return_object()` 不会被调用，拿到的是默认构造/未初始化的宿主对象，对它 `resume()` / `destroy()` 是未定义行为。
2. 不会。`await_ready() == true` 表示不用挂起，编译器直接跳到 `await_resume()`。
3. 表示**协程不挂起**，控制权留在协程里继续往下执行到 `await_resume()`。`true` 才是挂起。
4. 先 `return_value(100)`，再 `final_suspend()`。`co_return` 负责交付值，挂起发生在之后。
5. 因为两者冲突：`suspend_never final_suspend` 让帧在协程结束时**自动销毁**，之后宿主析构再 `destroy()` 就是双重释放。要让宿主拥有帧，`final_suspend` 就必须真的挂起（`suspend_always` 或自定义 awaiter）。
6. 没有。它是普通类，一个 `co_*` 都没有。它被编译器**物化进调用者的帧**里一个槽位，只借住，不拥有。
7. `handle_` 表示「接下来去跑另一个协程」——对称转移，`await_suspend` 返回 handle 后编译器直接跳进去；`void` 表示「接下来没有协程要跑，控制权还给事件循环」，等外部事件（定时器 / 完成回调）来 `resume`。
8. 内层协程跑完后帧确实保住了，但 `suspend_always::await_suspend` 返回 `void`，没有人去回答「接下来该谁跑」，控制权回到事件循环 → **外层协程永远停在 `co_await` 那一行**，后面的代码一行都不执行。
9. `done() == true` 的准确含义是**停在 final suspend point**，不是「最后一行语句执行过」。惰性协程刚创建时停在 `initial_suspend` → `false`。若 `final_suspend` 用 `suspend_never`，协程结束时帧被自动销毁、句柄悬空，`done()` 已无意义（调用是 UB）。所以「`done()` 等于跑完了」这个等式**只在宿主写法强制 `final_suspend` 挂起时才成立**。
10. 会**提前 resume**：`await_suspend` 无条件 `return handle_`，协程从 `RpcAwaiter` 挂起点蹿出去 → `await_resume()` 在响应未到时执行 → 返回空响应（不抛异常）→ 紧接着 `~RpcAwaiter` 把在途请求 `StartCancel()` 掉。见 13.4。**本仓库最终不是修它，而是把这条路径整块删掉（13.6）。**
11. 后到者覆盖先到者：`continuation` 只有一个槽（原 `rpc_task.h:45`），第一个等待者的 handle 被冲掉，协程跑完时只叫醒最后一个 → **先到者永久挂起**。要支持多等待者得把它换成 handle 列表。**（该路径已按 13.6 删除。）**
12. 不提供 `await_ready` / `await_suspend` / `await_resume` 三件套，也不提供 `operator co_await` —— `co_await X` 直接编译失败。好处是**编译期挡住 + 零运行时成本 + 零状态**；`assert` 在 `NDEBUG`（Release）下是空操作，违约会静默走进错误语义。见第 8 节末尾。
13. 顺序是 ① 成员 `operator co_await()` → ② 自由 `operator co_await(X)` → ③ X 自己就是 awaiter。走 ③ 时**三个方法必须齐全**（它们没有默认实现），少一个就报"缺成员"、编译不过。
14. **不是 UB。** 分界点在 `await_ready()` 返回 `false` 的那一刻 —— 标准原文是「the coroutine is considered suspended」，早于 `await_suspend` 被求值。`resume()` 的 UB 只有两条：目标协程**没在挂起**、或它**停在 final suspend point**；「await_suspend 正在跑」两条都不满足。跨线程这么做的代价是**数据竞争**（发布侧 release / 恢复侧 acquire），不是 UB。
15. 风险是**重入**：协程在 `await_suspend` 的栈上接着跑，`await_resume()` 会在它返回前执行；若协程走完这个 full-expression，awaiter 临时对象（住在帧里）会被析构，此后 `await_suspend` 再碰 `*this` 就是访问已析构对象（cppreference 示例注释原话：accessing potentially destroyed *this）。所以纪律是：**句柄发布之后不再访问成员**。本仓库用 `QueueInLoop` 从根上避开这条。

</details>
