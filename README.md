# NebulaRPC

当前最小骨架只支持 Linux。

## Debug

```bash
cmake --preset linux-debug
cmake --build --preset build-linux-debug
ctest --preset test-linux-debug
```

## Release

```bash
cmake --preset linux-release
cmake --build --preset build-linux-release
```

## ASan

```bash
cmake --preset linux-asan
cmake --build --preset build-linux-asan
ctest --preset test-linux-asan
```

## UBSan

```bash
cmake --preset linux-ubsan
cmake --build --preset build-linux-ubsan
ctest --preset test-linux-ubsan
```

当前 Stage 1 的第一个实现目标：

```text
epoll_wait
  -> EventLoop
  -> Channel::HandleEvent
  -> TcpConnection::HandleRead
  -> message callback
```
