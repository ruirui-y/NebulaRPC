# VMware NAT 网络恢复与 Ubuntu 固定 IP 配置

## 1. 背景

NebulaRPC 使用以下远程开发环境：

```text
Windows
  ↓
Visual Studio
  ↓ SSH
VMware Workstation
  ↓ NAT
Ubuntu 22.04
  ↓
CMake / Ninja / GDB
```

曾出现的问题：

```text
Visual Studio 无法连接 Ubuntu
ssh port 22 timeout
Ubuntu ens33 DOWN / NO-CARRIER
VMware NAT Service 启动失败
Ubuntu 重启后 ens33 不自动获取 IP
VMware DHCP 地址变化导致 VS 远程地址不稳定
```

最终目标：

```text
VMware 使用 NAT
Ubuntu ens33 开机自动启用
固定 IP：192.168.184.128
网关：192.168.184.2
VS 永久连接固定地址
```

---

## 2. 最初现象

Windows：

```powershell
ssh yjj@192.168.31.51
```

出现：

```text
ssh: connect to host 192.168.31.51 port 22: Connection timed out
```

Ubuntu SSH 服务本身正常：

```bash
sudo systemctl status ssh
```

结果：

```text
Active: active (running)
```

SSH 监听也正常：

```bash
sudo ss -lntp | grep :22
```

结果：

```text
0.0.0.0:22
[::]:22
```

因此问题不在 SSH，而在 VMware 网络。

---

## 3. 判断 VMware 网络问题

Ubuntu：

```bash
ip link show ens33
```

曾出现：

```text
<NO-CARRIER,BROADCAST,MULTICAST,UP>
state DOWN
```

含义：

```text
UP
= Linux 已经启用了网卡

NO-CARRIER
= VMware 没有给虚拟网卡提供有效链路
```

此时继续修改 SSH、CMake、VS 都没有意义。

---

## 4. VMware 网络恢复

### 4.1 改用 NAT

VMware：

```text
虚拟机设置
→ 网络适配器
→ NAT
→ 已连接
→ 启动时连接
```

### 4.2 VMware NAT Service 1067

Windows 曾出现：

```text
VMware NAT Service
错误 1067：进程意外终止
```

处理方式：

```text
VMware Workstation
→ 编辑
→ 虚拟网络编辑器
→ 更改设置
→ 还原默认设置
```

恢复默认网络：

```text
VMnet1 -> Host Only
VMnet8 -> NAT
```

然后确认 Windows 服务：

```text
VMware DHCP Service
VMware NAT Service
```

处于运行状态。

---

## 5. Ubuntu 临时恢复网络

VMware NAT 恢复后：

```bash
sudo ip link set ens33 up
```

检查：

```bash
ip link show ens33
```

正常状态：

```text
<BROADCAST,MULTICAST,UP,LOWER_UP>
```

关键：

```text
NO-CARRIER 消失
LOWER_UP 出现
```

然后手动 DHCP：

```bash
sudo dhclient -v ens33
```

成功时：

```text
DHCPOFFER of 192.168.184.128
DHCPACK of 192.168.184.128
bound to 192.168.184.128
```

检查：

```bash
hostname -I
```

得到：

```text
192.168.184.128 172.17.0.1
```

其中：

```text
192.168.184.128
```

是 VMware NAT 地址。

```text
172.17.0.1
```

是 Docker 网桥地址，不用于 SSH。

---

## 6. 解决 Ubuntu 重启后 ens33 不自动启动

重启后曾出现：

```bash
hostname -I
```

只有：

```text
172.17.0.1
```

并且：

```bash
nmcli device status
```

显示：

```text
ens33 ethernet 未托管
```

### 6.1 Netplan 配置

原始配置：

```text
/etc/netplan/01-network-manager-all.yaml
```

内容：

```yaml
network:
  version: 2
  renderer: NetworkManager
```

新增：

```text
/etc/netplan/01-ens33.yaml
```

最初使用 DHCP：

```yaml
network:
  version: 2
  renderer: NetworkManager
  ethernets:
    ens33:
      dhcp4: true
      optional: true
```

修正权限：

```bash
sudo chmod 600 /etc/netplan/01-ens33.yaml
sudo chmod 600 /etc/netplan/01-network-manager-all.yaml
```

应用：

```bash
sudo netplan generate
sudo netplan apply
```

---

## 7. NetworkManager 未托管 ens33

检查：

```bash
cat /etc/NetworkManager/NetworkManager.conf
```

曾为：

```ini
[main]
plugins=ifupdown,keyfile

[ifupdown]
managed=false

[device]
wifi.scan-rand-mac-address=no
```

同时：

```bash
grep -R "managed\|unmanaged" \
/etc/NetworkManager \
/usr/lib/NetworkManager 2>/dev/null
```

发现：

```text
/usr/lib/NetworkManager/conf.d/10-globally-managed-devices.conf:
unmanaged-devices=*,except:type:wifi,except:type:gsm,except:type:cdma
```

这会导致 Ethernet `ens33` 被标记为未托管。

---

## 8. 修复 NetworkManager

修改：

```text
/etc/NetworkManager/NetworkManager.conf
```

为：

```ini
[main]
plugins=ifupdown,keyfile

[ifupdown]
managed=true

[device]
wifi.scan-rand-mac-address=no
```

创建覆盖配置：

```bash
sudo tee /etc/NetworkManager/conf.d/10-globally-managed-devices.conf > /dev/null <<'EOF'
[keyfile]
unmanaged-devices=
EOF
```

设置权限：

```bash
sudo chmod 600 \
/etc/NetworkManager/conf.d/10-globally-managed-devices.conf
```

注意：本机 NetworkManager 不接受将该配置文件做成 `/dev/null` 的符号链接，否则会报：

```text
不是普通文件
```

并导致 `NetworkManager.service` 启动失败。

恢复：

```bash
sudo systemctl reset-failed NetworkManager
sudo systemctl restart NetworkManager
sudo nmcli networking on
```

验证：

```bash
systemctl is-active NetworkManager
nmcli device status
```

正常：

```text
active

ens33 ethernet 已连接 netplan-ens33
```

---

## 9. 配置固定 IP

VMware NAT 当前网络：

```text
网段：192.168.184.0/24
网关：192.168.184.2
DNS：192.168.184.2
```

确认方式：

```bash
ip route
```

以及：

```bash
nmcli -f IP4.GATEWAY,IP4.DNS device show ens33
```

结果：

```text
IP4.GATEWAY: 192.168.184.2
IP4.DNS[1]:  192.168.184.2
```

### 9.1 最终 Netplan

覆盖写入：

```bash
sudo tee /etc/netplan/01-ens33.yaml > /dev/null <<'EOF'
network:
  version: 2
  renderer: NetworkManager
  ethernets:
    ens33:
      dhcp4: false
      addresses:
        - 192.168.184.128/24
      routes:
        - to: default
          via: 192.168.184.2
      nameservers:
        addresses:
          - 192.168.184.2
          - 8.8.8.8
      optional: true
EOF
```

应用：

```bash
sudo chmod 600 /etc/netplan/01-ens33.yaml
sudo netplan generate
sudo netplan apply
```

---

## 10. 最终状态

```bash
hostname -I
```

结果：

```text
192.168.184.128 172.17.0.1
```

路由：

```bash
ip route
```

结果：

```text
default via 192.168.184.2 dev ens33 proto static metric 100
192.168.184.0/24 dev ens33 proto kernel scope link src 192.168.184.128
172.17.0.0/16 dev docker0 ...
```

NetworkManager：

```bash
nmcli device status
```

结果：

```text
ens33 ethernet 已连接 netplan-ens33
```

最终开发环境：

```text
Ubuntu SSH IP : 192.168.184.128
SSH Port      : 22
User          : yjj
VMware Mode   : NAT
Gateway       : 192.168.184.2
```

Visual Studio 后续固定连接：

```text
192.168.184.128
```

---

## 11. 快速排障命令

以后如果突然无法连接，可以按顺序检查：

```bash
hostname -I
ip -br addr
ip route
nmcli device status
systemctl is-active NetworkManager
systemctl is-active ssh
sudo ss -lntp | grep :22
```

正常情况下至少应满足：

```text
NetworkManager = active
ens33 = UP / 已连接
ens33 IP = 192.168.184.128
default gateway = 192.168.184.2
ssh = active
port 22 = LISTEN
```

Windows 验证：

```powershell
ping 192.168.184.128
ssh yjj@192.168.184.128
```

---

## 12. 最终结论

本次问题实际经历了三层：

```text
VMware NAT
    ↓
Ubuntu 网卡 ens33
    ↓
NetworkManager / Netplan
```

SSH 本身一直不是主要问题。

最终通过：

```text
恢复 VMware NAT
→ 恢复 ens33 carrier
→ 修复 NetworkManager unmanaged 配置
→ Netplan 自动管理 ens33
→ 配置静态 IP
```

完成稳定的远程 Linux C++ 开发环境。
