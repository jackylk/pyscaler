# pyscaler

[English](README.en.md) · 中文

把你的单机 Python 数据处理代码变成分布式脚本。目前支持 **Ray**，即将支持 **Aura**。可以跑在你自己的集群上，也可以跑在 DBay 上。

## 为什么做这个

你可能有一段 Python 代码，处理数据湖上的大文件，跑得慢。你想让它并行起来，但：

- 手写 Ray/Dask 代码容易踩坑，不知道选哪种模式
- 改完不知道对不对，也不知道真正能加速多少
- 希望转换完能跑在自己的集群上，不想被某个平台绑定

`pyscaler` 专注做好一件事：**把 Python 变成可验证的分布式脚本**，运行在哪儿是你的事。

## 安装

```bash
# 从 PyPI 安装（发布后）
pip install pyscaler

# 带上框架运行时
pip install "pyscaler[ray]"        # Ray 支持
pip install "pyscaler[aura]"       # Aura 支持（规划中）
pip install "pyscaler[llm]"        # 启用 LLM 辅助转换

# 从源码开发安装
git clone https://github.com/jackylk/pyscaler
cd pyscaler
pip install -e ".[ray,dev]"
```

## 快速上手

### 先看效果：3 条命令看加速比

仓库里的 `examples/07_demo_compute_stats.py` 会自动生成 20 个 JSON 文件、每个文件带 CPU 负载，**直接测得真实加速比**：

```bash
git clone https://github.com/jackylk/pyscaler && cd pyscaler
pip install -e ".[ray]"

cd examples
pyscaler convert 07_demo_compute_stats.py --workers 4
python 07_demo_compute_stats.py          # 原版：~4-6s
rm -rf data/out
python 07_demo_compute_stats_dist.py     # Ray 版：~1.5-2.5s  → 2.5-3× 加速
```

### 四个命令的完整工作流

```bash
# 1. 分析 —— 找瓶颈、识别模式、预估加速比
pyscaler analyze ./process.py

# 2. 转换 —— 生成 process_dist.py + diff
pyscaler convert ./process.py --framework ray --workers 8

# 3. 验证 —— 跑两个版本对比正确性和实测加速
pyscaler verify ./process_dist.py --input ./data/ --sample 0.05

# 4. 执行 —— 选择后端（三种模式，详见下表）
pyscaler run ./process_dist.py --backend local             --input ./data/
pyscaler run ./process_dist.py --backend auto              --input /abs/data/
pyscaler run ./process_dist.py --backend ray://head:10001  --input obs://bkt/d/
pyscaler run ./process_dist.py --backend dbay              --input obs://bkt/d/
```

### 三种 Ray 执行模式

| 模式 | 何时用 | 怎么启动 |
|---|---|---|
| **Ray 本地 embedded** | 开发、小数据 | `pyscaler run ... --backend local` |
| **本地 Ray 集群** | 你自己机器上的多进程集群 | 先 `ray start --head`，再 `pyscaler run ... --backend auto` |
| **DBay 远程 Ray** | 大数据、无需自己维护集群 | `export PYSCALER_DBAY_TOKEN=...; pyscaler run ... --backend dbay` |

> ⚠️ 集群模式下**输入/输出路径必须是绝对路径或云存储 URI**（obs:// / s3://），相对路径只在 local embedded 模式可靠。这是 Ray 集群的通用限制，不是 pyscaler 独有。

## 5 分钟上手教程

这个教程用仓库里自带的示例代码，走一遍完整流程。

### 第 1 步：准备环境

```bash
git clone https://github.com/jackylk/pyscaler
cd pyscaler
pip install -e ".[ray,dev]"     # 装 pyscaler + Ray 运行时
```

### 第 2 步：看一下你要转换的代码

```bash
cat examples/01_file_loop.py
```

这是一段典型的"遍历文件目录，逐个处理"的代码：

```python
files = sorted(glob.glob("./data/input/*.json"))
for f in files:
    process_file(f)    # ← 瓶颈：串行 I/O
```

### 第 3 步：让 pyscaler 分析它

```bash
pyscaler analyze examples/01_file_loop.py
```

pyscaler 会告诉你：
- 瓶颈在哪一行
- 推荐用什么分布式模式（这里是 Ray 文件级并行）
- 预估能加速多少倍

### 第 4 步：转换

```bash
pyscaler convert examples/01_file_loop.py --framework ray --workers 8
```

生成两个文件：
- `examples/01_file_loop_dist.py` —— 转换后的 Ray 版本
- 终端打印 diff，让你一眼看到改了什么

### 第 5 步：验证正确性 + 测实际加速比

先造一点测试数据（示例）：

```bash
mkdir -p data/input && for i in $(seq 1 50); do
  echo "{\"records\": [1,2,3]}" > data/input/f$i.json
done
```

再跑验证：

```bash
pyscaler verify examples/01_file_loop_dist.py --input ./data/input --sample 0.2
```

pyscaler 会：
1. 取 20% 样本
2. 同时跑原版和 Ray 版
3. 对比两个版本输出是否一致
4. 告诉你实测加速比

### 第 6 步：在真实数据上跑

```bash
# 本地（ray.init() 用本机所有核心）
pyscaler run examples/01_file_loop_dist.py --backend local --input ./data/input

# 你自己的 Ray 集群
pyscaler run examples/01_file_loop_dist.py --backend ray://head:10001 --input ./data/input

# 或提交给 DBay
pyscaler run examples/01_file_loop_dist.py --backend dbay --input obs://my-bucket/data/
```

### 常见问题

**Q: 我的代码 pyscaler 说不能并行化怎么办？**
看 `examples/03_blocked_by_state.py`，是共享可变状态的典型反例。pyscaler 会指出问题点，按提示重构（通常是把全局变量改成函数参数）就行。

**Q: 一定要用 Ray 吗？**
目前是。下一个支持的框架是 Aura（规划中），用 `--framework aura` 启用。转换后的脚本是标准框架代码，pyscaler 不会锁定你。

**Q: 转换用 LLM 吗？**
默认不用，纯模板转换，离线可跑。加 `--llm-assist` 才会用 LLM 帮忙填一些边角情况。

## 核心概念

pyscaler 有两层插件，**分别正交**：

| 层 | 职责 | 例子 |
|---|---|---|
| **Framework**（框架） | 决定代码怎么改 | `ray` · `aura`（规划中） |
| **Backend**（后端） | 决定脚本在哪执行 | `local` · `ray://集群地址` · `dbay` |

一份 Ray 脚本可以跑在本地、自己的 Ray 集群，或 DBay 的 Ray 服务。**转换后的代码不绑定任何执行环境**。

## 四个命令

| 命令 | 输入 | 输出 |
|---|---|---|
| `analyze` | `.py` 文件或仓库 | 瓶颈报告 + 推荐框架 + 预估加速比 |
| `convert` | `.py` 文件 | 分布式版本脚本 + 统一 diff + 改动摘要 JSON |
| `verify` | 生成的脚本 + 输入目录 | 样本对比报告 + 实测加速比 |
| `run` | 生成的脚本 + 输入路径 | 交给所选后端执行 |

每一步产物都是下一步的输入，也都可以单独停下。比如你可以 `convert` 完把脚本拿到别处跑，不必走完全链路。

## 任务目录

每个任务有独立工作目录，记录来源、产物、运行日志：

```
.pyscaler/tasks/{task_id}/
├── meta.json              # 命令、参数、commit、时间戳
├── source/                # 原始代码快照
├── converted/             # 生成的分布式脚本 + diff
├── verification/          # 样本验证产物
└── runs/{run_id}/         # 每次执行的日志和输出
```

默认写在当前目录 `./.pyscaler/`，也可以指向 OBS/S3 共享给团队。

## 设计边界

### 做什么

- 识别数据并行模式（文件级循环、DataFrame map/filter、批处理管道）
- 用模板生成分布式代码，LLM 只补缺口（可选）
- 验证正确性 + 测真实加速比
- 本地 / 用户集群 / DBay 三种后端

### 不做什么

- 不做集群管理（用 `ray up`、`dask-scheduler`）
- 不做通用 Python 重写器（只针对数据并行）
- 不绑定 DBay（DBay 只是后端之一）

## 状态

早期实现。CLI 形态已定型。已支持 5 种模式（4 种 fan-out 变体 + DataFrame apply），对接 Ray 本地 / Ray 本地集群 / DBay 远程集群三种后端。

📖 **详细文档**
- **[支持的代码形态](docs/SUPPORTED_PATTERNS.md)** — 哪些代码能转、哪些不能、遇到不能转怎么办
- [用户指南](docs/USER_GUIDE.md) — CLI 每个命令的完整参数和使用场景
- [设计文档](docs/DESIGN.md) — 架构、插件层、模板方法
- [测试报告](docs/TEST_REPORT.md) — 测试覆盖范围和复现步骤

## 贡献

欢迎 Issue 和 PR。添加新框架：在 `src/pyscaler/frameworks/` 下新建一个继承 `Framework` 的类，在 `registry.py` 注册即可。

## 许可

Apache-2.0
