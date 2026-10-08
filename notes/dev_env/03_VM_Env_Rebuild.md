# Linux 开发环境重建实录（新机器）

> 本文记录**第二次重建**一台 Ubuntu 开发机的完整过程。
> 与 `01_VMware_NAT_And_Static_IP.md` 是**两台不同的机器**，网络参数、用户名、磁盘布局**全部不同**，
> 因此单独成篇。看本文时**不要**套用 01 里的 `192.168.184.128` / 用户 `yjj`。

---

## 1. 为什么单独开一篇

| 项 | 01 文档（机器 A） | 本文档（机器 B） |
|---|---|---|
| VMware NAT 网段 | `192.168.184.0/24` | **`192.168.56.0/24`** |
| NAT 网关 | `192.168.184.2` | **`192.168.56.2`** |
| 主机 VMnet8 网卡 | — | `192.168.56.1` |
| Ubuntu 固定 IP | `192.168.184.128` | **`192.168.56.100`** |
| 登录用户 | `yjj` | **`yujingjing`** |
| 主机名 | — | `yujingjing-virtual-machine` |
| 磁盘布局 | （未特别记录） | **非 LVM**：`sda3` 裸 ext4 直接挂 `/` |
| 虚拟规格 | 2GB / 2 核 / 50GB | **4GB / 4 核 / 100GB** |

共同点：Windows + Visual Studio `↓ SSH` VMware NAT `↓` Ubuntu 22.04 `↓` CMake / Ninja / GDB。

**还原点**：两台机器都遵循同一条原则 ——
```text
Visual Studio 通过「固定 IP」远程连接，IP 不随重启变化。
```

---

## 2. 重建的三层结构

本次实际走的三道关，自底向上：

```text
① VMware / Windows 侧   虚拟硬件规格 + 虚拟网络服务
        ↓
② Linux 磁盘            物理磁盘扩容后，分区 + 文件系统要各自跟上
        ↓
③ Linux 网络            netplan 静态 IP
```

层次搞反会反复返工。**必须先 ① 后 ②③**，否则虚拟磁盘没扩，Linux 里无从扩起。

---

## 3. 第 ① 层：VMware / Windows 侧

### 3.1 虚拟硬件规格

> 关键约束：**内存与 CPU 只有在虚拟机「彻底关机」状态下才能改**；挂起 / 开机时输入框是灰的。
> 彻底关机：
> ```bash
> sudo poweroff
> ```

目标规格（针对 NebulaRPC：C++20 + protobuf 模板 + ASan/UBSan 构建）：

| 项目 | 原值 | 新值 | 说明 |
|---|---|---|---|
| 内存 | 2 GB | **4 GB** | 2GB 编译大 TU 或开 ASan 会 OOM |
| 处理器 | 2 | **4** | 决定 `ninja -j` 并行度 |
| 硬盘 | 50 GB | **100 GB** | 瘦分配下「当前大小」≈ 实际已写满 |

路径：`虚拟机(M)` → `设置(E)`。

开机后在 VM 内验证：

```bash
free -h          # Mem 总量应为 ~3.8Gi
nproc            # 应为 4
lscpu | grep -E '^CPU\(s\)|Model name'
```

### 3.2 磁盘扩容（VMware 侧）

1. 虚拟机**彻底关机**。
2. **先处理快照**：存在任何快照时，`扩展(E)` 按钮是**灰色不可点**的。先把快照全部删除/合并。
3. `虚拟机设置` → 选中硬盘 → `扩展(E)` → 输入新容量 `100`。
4. **扩容不可逆**：只能变大，不能改小。分卷（多个 2GB 文件）不影响扩展。

> 判据：拆分磁盘界面上的 **「当前大小 vs 最大大小」**。
> 瘦分配（勾了「没有为此硬盘预分配磁盘空间」）时，`当前大小` 就是 guest 里**真实写入的数据量**。
> 本例中扩前是 `48.8GB / 50GB` —— 已经濒临写满，属于**必须扩**，不是「可选」。

### 3.3 开不了机：VMware 服务被批量禁用

**现象**：VM 设置改完，点开机无反应 / 报错。

**根因**：VMware 的若干 Windows 服务被「优化软件 / 开机加速」类工具批量设为 **Disabled**。

| 服务 | 作用 | 故障时状态 |
|---|---|---|
| **`VMAuthdService`**（VMware Authorization Service） | **启动 VM 的必需服务** | Stopped + **Disabled** ← 元凶 |
| `VMwareHostd` | 主机守护 | Stopped + Disabled |
| `VMUSBArbService` | USB 仲裁 | Stopped + Disabled |
| `VMnetDHCP` | NAT 网络的 DHCP | 正常（Running + Automatic） |
| `VMware NAT Service` | NAT 转发 | 正常（Running + Automatic） |

**修复**（需要**管理员**权限；普通用户会报「拒绝访问」）：

界面方式：`Win + R` → `services.msc` → 双击 `VMware Authorization Service`
→ 启动类型改 **自动** → 点 **启动** → 确定。
（`VMwareHostd`、`VMware USB Arbitration Service` 同理。）

管理员命令行方式：

```cmd
sc config VMAuthdService start= auto
sc start VMAuthdService
sc config VMwareHostd start= auto
sc start VMwareHostd
sc config VMUSBArbService start= auto
sc start VMUSBArbService
```

> 若 `sc start` 报 1067（进程意外终止）/ 1069（依赖服务失败），说明不只是被禁用而是**安装损坏**，
> 需要用 VMware 安装包跑一次「修复」。本例只是被禁用，直接启动即恢复。

### 3.4 虚拟网络编辑器「还原默认设置」——并**重新确认网段**

当 `netmap.conf` 里只剩 `vmnet0`、主机上没有 VMnet 虚拟网卡、`VMware NAT Service` 起不来时，
走这条路（**属于「安装类」动作，本机由用户本人执行**）：

`编辑` → `虚拟网络编辑器` → `更改设置`（需管理员）→ **`还原默认设置`**

它会重建：

```text
VMnet1 -> Host-Only
VMnet8 -> NAT
```

并装回主机虚拟网卡、启用 `VMnetDHCP` 与 `VMware NAT Service`。

> ⚠️ **最关键的一步**：还原后 **NAT 网段会变**，不一定还是之前的 184 段。
> 本例中还原后变成了 `192.168.56.0/24`。
> 必须**立刻记录** VMnet8 的子网 / 网关，后面 netplan 静态 IP 全部以它为准。

在 Windows 上确认：

```cmd
ipconfig
```

本例结果（节选）：

```text
以太网适配器 VMware Network Adapter VMnet1:
   IPv4 地址 . . . . . . . . . . . . : 192.168.119.1

以太网适配器 VMware Network Adapter VMnet8:
   IPv4 地址 . . . . . . . . . . . . : 192.168.56.1
```

VMware 约定：

```text
VMnet8 主机网卡 = 192.168.56.1
NAT 网关        = 192.168.56.2      ← guest 的默认网关
网段            = 192.168.56.0/24
```

---

## 4. 第 ② 层：Linux 文件系统扩容

### 4.1 先侦察，不要直接敲扩容命令

```bash
lsblk
df -h /
```

**重点看三件事**：
1. `sda` 是否已经是 **100G**（VMware 扩的是否被内核认到）
2. 上层分区是否**还卡在旧值**（如 49.5G）
3. 分区下面**有没有 `lvm` 那一层** —— 决定走哪套命令

本例实测：

```text
sda      8:0    0   100G  0 disk
├─sda2   8:2    0   513M  0 part /boot/efi
└─sda3   8:3    0  49.5G  0 part /
```

```text
文件系统        大小  已用  可用  已用% 挂载点
/dev/sda3        49G   45G  2.1G    96% /
```

结论：`sda` 已经是 100G ✅，但 `sda3` 还卡在 49.5G，`/` 只剩 2.1G（**96%**）。
且 `sda3` 下面**没有 lvm 行** → 本例是**非 LVM**。

> 若 `lsblk` 里 `sda` 仍显示 50G（VMware 扩的没生效），让内核重新读盘：
> ```bash
> echo 1 | sudo tee /sys/class/block/sda/device/rescan
> ```

### 4.2 为什么要分「分区」和「文件系统」两层

```text
物理磁盘 sda       ← VMware 扩的是这一层（100G）
    ↓
分区 sda3          ← growpart   扩这一层（只改分区表）
    ↓
文件系统 ext4      ← resize2fs  扩这一层（真正让 / 变大）
```

漏掉任何一层，`df -h /` 都不会变。

### 4.3 非 LVM 方案（本例）

```bash
sudo growpart /dev/sda 3
sudo resize2fs /dev/sda3
df -h /
```

- `growpart`：把**最后一个分区**的结束位置顶到磁盘末尾，起始位置不动，**不丢数据**。
- `resize2fs`：ext4 支持**在线扩容**，`/` 挂着不用卸载，直接跑。

本例结果：

```text
sda3   8:3    0  99.5G  0 part
文件系统        大小  已用  可用  已用% 挂载点
/dev/sda3        98G   44G   50G   47% /
```

从「49G / 可用 2.1G」变成「98G / 可用 50G」，彻底解除压力。

#### `growpart: command not found` 的三种解法

`growpart` 由 `cloud-guest-utils` 这个小包提供，**Ubuntu 默认不装**。本例就没装。

**方案 A：有网 → 装包（推荐，顺带验证网络）**

```bash
sudo apt update
sudo apt install -y cloud-guest-utils
sudo growpart /dev/sda 3
sudo resize2fs /dev/sda3
```

**方案 B：不装包 → 系统自带的 `parted`**

```bash
sudo parted -s /dev/sda resizepart 3 100%
sudo resize2fs /dev/sda3
```

`-s` 是脚本模式，不弹 Yes/No。

**方案 C：连 parted 都没有 → `sfdisk`（util-linux 自带，几乎必定存在）**

```bash
sudo sfdisk --force -N 3 /dev/sda <<< ", +"
sudo partprobe /dev/sda
sudo resize2fs /dev/sda3
```

> 三种都可以。若网络还没配好，B / C 可**离线完成**扩容，不必先联网。

### 4.4 LVM 方案（对照备用）

若 `lsblk` 里 `sda3` 下面**有** `└─ubuntu--vg-ubuntu--lv ... lvm`，则走四条命令（顺序不能乱）：

```bash
sudo growpart /dev/sda 3          # ① 撑大分区（3 = 实际分区号）
sudo pvresize /dev/sda3           # ② 让 PV 认识新空间
sudo lvextend -l +100%FREE /dev/mapper/ubuntu--vg-ubuntu--lv   # ③ 喂给 LV
sudo resize2fs /dev/mapper/ubuntu--vg-ubuntu--lv               # ④ 文件系统跟上
```

> LV 路径以 `lsblk` 实际输出为准，也可能是 `ubuntu-vg/root`。
> **先 `lsblk` 再动手**，路径写错就得重新对齐。

---

## 5. 第 ③ 层：Linux 网络（netplan 静态 IP）

### 5.1 先侦察

```bash
ip -br a
nmcli device status
ip route
```

本例实测：

```text
ens33    UP    192.168.56.128/24  fe80::20c:29ff:fe31:c572/64
docker0  DOWN  172.17.0.1/16

DEVICE   TYPE      STATE   CONNECTION
ens33    ethernet  已连接  netplan-ens33
docker0  bridge    连接（外部）  docker0
```

```text
default via 192.168.56.2 dev ens33 proto dhcp metric 100
```

解读：

- 网卡名 **`ens33`**；DHCP 拿到 `.128` → **NAT 链路已通**。
- NM 连接名 `netplan-ens33`（由 netplan 生成）→ 说明**已存在 netplan 配置**。
- **默认网关 `192.168.56.2` 已确认**（与 VMware 约定一致）。
- `docker0` 是 Docker 自带网桥，忽略。
- `.128` 落在 VMware NAT 默认 **DHCP 池 `56.128~56.254`** 内 → **重启会变**，必须改静态。

### 5.2 坑：netplan 文件名的**合并优先级**

先看现有配置：

```bash
ls -l /etc/netplan/
sudo cat /etc/netplan/*.yaml
```

本例既有文件是 **`/etc/netplan/01-network-manager-all.yaml`**（146B，root:root 644），内容：

```yaml
network:
  version: 2
  renderer: NetworkManager
  ethernets:
    ens33:
      dhcp4: true
```

> ⚠️ **不能**直接新建 `01-ens33.yaml`（01 文档里的做法）。
> netplan 按**文件名字母序**合并，**后面的覆盖前面的**：
> `01-ens33.yaml` 排在 `01-network-manager-all.yaml` **前面**（`e` < `n`），
> 新建的那个会被后者覆盖，`dhcp4: true` 照样生效 —— 白折腾。
>
> 因此本例做法：**这个文件既然已经定义 ens33，就直接改写它本身**。

### 5.3 最终静态配置

```bash
sudo cp /etc/netplan/01-network-manager-all.yaml /etc/netplan/01-network-manager-all.yaml.bak

sudo tee /etc/netplan/01-network-manager-all.yaml > /dev/null <<'EOF'
network:
  version: 2
  renderer: NetworkManager
  ethernets:
    ens33:
      dhcp4: false
      addresses:
        - 192.168.56.100/24
      routes:
        - to: default
          via: 192.168.56.2
      nameservers:
        addresses:
          - 192.168.56.2
          - 8.8.8.8
      optional: true
EOF

sudo chmod 600 /etc/netplan/01-network-manager-all.yaml
sudo netplan generate
sudo netplan apply
```

**IP 选址原则**：`192.168.56.100` 是**刻意**避开 VMware NAT 的 DHCP 池 `56.128~56.254`，
免得和自动分配撞车。取 `.100` 附近（`.3 ~ .127`）最安全。

### 5.4 验证清单

```bash
ip -br a                 # ens33 应只剩 192.168.56.100/24，.128 消失
ip route                 # default via 192.168.56.2 dev ens33
ping -c 2 192.168.56.2   # 通 = 到网关
ping -c 2 8.8.8.8        # 通 = 出公网
systemctl is-active ssh  # active
sudo ss -lntp | grep :22 # 有 LISTEN
```

本例全部通过：

```text
ens33    UP    192.168.56.100/24
default via 192.168.56.2 dev ens33 proto static metric 100
PING 192.168.56.2 ... 0% packet loss
PING 8.8.8.8 ... 0% packet loss（46ms）
active
```

> 若 `.128` 和 `.100` **同时存在**（DHCP 老地址没释放）：
> ```bash
> sudo nmcli con down netplan-ens33 && sudo nmcli con up netplan-ens33
> ```
> 或直接 `sudo reboot`，起来就干净了。

### 5.5 老坑备忘：`ens33 未托管`

若 `nmcli device status` 显示 **「未托管」**，见 `01` 文档第 7、8 节
（`10-globally-managed-devices.conf` 把以太网设为 unmanaged）。修复要点：

```bash
sudo sed -i 's/^managed=false/managed=true/' /etc/NetworkManager/NetworkManager.conf

sudo tee /etc/NetworkManager/conf.d/10-globally-managed-devices.conf > /dev/null <<'EOF'
[keyfile]
unmanaged-devices=
EOF

sudo chmod 600 /etc/NetworkManager/conf.d/10-globally-managed-devices.conf
sudo systemctl reset-failed NetworkManager
sudo systemctl restart NetworkManager
sudo nmcli networking on
```

> 记住那个坑：该文件**不能**做成 `/dev/null` 的软链接，否则 NetworkManager
> 会因「不是普通文件」起不来。**本例没有踩到这个坑**，属备用。

---

## 6. Windows 侧回连验证

```cmd
ping 192.168.56.100
ssh yujingjing@192.168.56.100
```

本例通过：

```text
来自 192.168.56.100 的回复: 字节=32 时间<1ms TTL=64
数据包: 已发送 = 4，已接收 = 4，丢失 = 0 (0% 丢失)
```

```text
Welcome to Ubuntu 22.04.4 LTS (GNU/Linux 6.8.0-94-generic x86_64)
```

> 首次连接会有 host key 指纹确认，输入 `yes` 即可。
> 若之前同一 IP 被别的机器用过，可能提示 key 冲突，删掉对应 `known_hosts` 行再连。

**记住用户名是 `yujingjing`**（`01` 文档写的是 `yjj`，那是机器 A 的）。
