# Git / GitHub 代理故障排查与固定配置

## 1. 问题现象

浏览器可以正常打开 GitHub：

```text
https://github.com
```

但命令行访问失败：

```cmd
curl -I https://github.com
```

可能出现：

```text
curl: (28) Failed to connect to github.com port 443
```

或者：

```text
curl: (56) Recv failure: Connection was reset
```

同时：

```cmd
git push
```

也无法连接 GitHub。

---

## 2. 初步排查 Git 自身代理

查看远程仓库：

```cmd
git remote -v
```

示例：

```text
origin  https://github.com/ruirui-y/NebulaRPC.git (fetch)
origin  https://github.com/ruirui-y/NebulaRPC.git (push)
```

查看 Git 全局代理：

```cmd
git config --global --get http.proxy
git config --global --get https.proxy
```

查看当前 CMD 环境变量：

```cmd
set | findstr /I proxy
```

本次结果均为空，说明 Git 没有配置代理。

---

## 3. 验证命令行是否在直连 GitHub

执行：

```cmd
curl -4 -v https://github.com
```

本次输出：

```text
Host github.com:443 was resolved.
IPv4: 20.205.243.166
Trying 20.205.243.166:443...
connect ... failed: Timed out
```

这说明：

```text
curl
  ↓
直接连接 GitHub
  ↓
TCP 443 超时
```

而不是通过 VPN 本地代理访问。

---

## 4. 检查 Windows 代理状态

查看 WinHTTP：

```cmd
netsh winhttp show proxy
```

本次：

```text
直接访问(没有代理服务器)
```

再查看 Windows 当前用户系统代理：

```cmd
reg query "HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings" /v ProxyEnable

reg query "HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings" /v ProxyServer

reg query "HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings" /v AutoConfigURL
```

本次关键结果：

```text
ProxyEnable    REG_DWORD    0x1
ProxyServer    REG_SZ       127.0.0.1:7899
```

说明 Windows 浏览器实际通过：

```text
127.0.0.1:7899
```

进入 VPN。

---

## 5. 为什么浏览器能打开 GitHub，但 Git 不行

实际网络路径不同。

浏览器：

```text
Chrome
  ↓
Windows 系统代理
  ↓
127.0.0.1:7899
  ↓
VPN
  ↓
GitHub
```

Git / curl 原始状态：

```text
git / curl
  ↓
直接连接 GitHub
  ↓
443 超时 / reset
```

因此：

> VPN 本身没有坏，真正的问题是 Git 没有走 VPN 暴露出来的本地 HTTP 代理。

---

## 6. 验证本地代理

执行：

```cmd
curl -x http://127.0.0.1:7899 -I https://github.com
```

成功返回：

```text
HTTP/1.1 200 Connection established
HTTP/1.1 200 OK
```

这一步可以确认：

```text
VPN 正常
本地代理正常
GitHub 正常
```

问题只剩 Git 代理配置。

---

## 7. 给 Git 配置代理

执行：

```cmd
git config --global http.proxy http://127.0.0.1:7899
git config --global https.proxy http://127.0.0.1:7899
```

为了减少部分代理环境下 HTTP/2 的兼容问题，可以固定使用 HTTP/1.1：

```cmd
git config --global http.version HTTP/1.1
```

确认：

```cmd
git config --global --get http.proxy
git config --global --get https.proxy
git config --global --get http.version
```

应看到：

```text
http://127.0.0.1:7899
http://127.0.0.1:7899
HTTP/1.1
```

---

## 8. 验证 GitHub 仓库访问

先不要直接 push，先验证远程读取：

```cmd
git ls-remote https://github.com/ruirui-y/NebulaRPC.git
```

如果能看到：

```text
<commit hash>    HEAD
<commit hash>    refs/heads/master
```

说明 Git 已经成功通过代理连接 GitHub。

然后：

```cmd
git push
```

即可正常推送。

---

## 9. 后续再次出现同类问题时

如果以后再次出现：

```text
浏览器 GitHub 正常
git push 超时
```

优先检查：

```cmd
git config --global --get http.proxy
git config --global --get https.proxy
```

再检查 Windows 当前代理端口：

```cmd
reg query "HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings" /v ProxyServer
```

如果 VPN 端口从：

```text
127.0.0.1:7899
```

变成例如：

```text
127.0.0.1:7890
```

只需要更新 Git：

```cmd
git config --global http.proxy http://127.0.0.1:7890
git config --global https.proxy http://127.0.0.1:7890
```

---

## 10. 临时取消 Git 代理

如果以后切换成 TUN 模式，或者网络环境不再需要显式代理，可以删除：

```cmd
git config --global --unset http.proxy
git config --global --unset https.proxy
```

查看是否清除成功：

```cmd
git config --global --get http.proxy
git config --global --get https.proxy
```

无输出即表示已经删除。

---

## 11. 常用快速诊断命令

```cmd
git remote -v

git config --global --get http.proxy
git config --global --get https.proxy

set | findstr /I proxy

curl -I https://github.com
curl -4 -v https://github.com

netsh winhttp show proxy

reg query "HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings" /v ProxyEnable
reg query "HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings" /v ProxyServer

curl -x http://127.0.0.1:7899 -I https://github.com

git ls-remote https://github.com/ruirui-y/NebulaRPC.git
```

---

## 12. 本次最终配置

```text
Git Remote:
https://github.com/ruirui-y/NebulaRPC.git

HTTP Proxy:
http://127.0.0.1:7899

HTTPS Proxy:
http://127.0.0.1:7899

Git HTTP Version:
HTTP/1.1
```

最终网络路径：

```text
Git
  ↓
127.0.0.1:7899
  ↓
VPN
  ↓
GitHub
```

GitHub push 恢复正常。
