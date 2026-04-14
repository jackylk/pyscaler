# distify

[English](README.en.md) · 中文

把你的单机 Python 数据处理代码变成分布式脚本。框架无关（支持 Ray，后续 Dask 等），可以跑在你自己的集群上，也可以跑在 DBay 上。

## 为什么做这个

你可能有一段 Python 代码，处理数据湖上的大文件，跑得慢。你想让它并行起来，但：

- 手写 Ray/Dask 代码容易踩坑，不知道选哪种模式
- 改完不知道对不对，也不知道真正能加速多少
- 希望转换完能跑在自己的集群上，不想被某个平台绑定

`distify` 专注做好一件事：**把 Python 变成可验证的分布式脚本**，运行在哪儿是你的事。

## 安装

```bash
# 从 PyPI 安装（发布后）
pip install distify

# 带上框架运行时
pip install "distify[ray]"        # Ray 后端
pip install "distify[dask]"       # Dask 后端（规划中）
pip install "distify[llm]"        # 启用 LLM 辅助转换

# 从源码开发安装
git clone https://github.com/jackylk/distify
cd distify
pip install -e ".[ray,dev]"
```

## 快速上手

```bash
# 1. 分析 —— 找瓶颈、推荐框架、预估加速比
distify analyze ./process.py

# 2. 转换 —— 生成 process_dist.py + diff
distify convert ./process.py --framework ray --workers 8

# 3. 验证 —— 取 5% 样本跑两个版本，对比正确性和实测加速
distify verify ./process_dist.py --input ./data/ --sample 0.05

# 4. 执行 —— 选择后端
distify run ./process_dist.py --backend local             --input ./data/
distify run ./process_dist.py --backend ray://head:10001  --input ./data/
distify run ./process_dist.py --backend dbay              --input obs://bucket/data/
```

## 核心概念

distify 有两层插件，**分别正交**：

| 层 | 职责 | 例子 |
|---|---|---|
| **Framework**（框架） | 决定代码怎么改 | `ray` · `dask` · 未来 `spark` |
| **Backend**（后端） | 决定脚本在哪执行 | `local` · `ray://集群地址` · `dbay` |

一份 Ray 脚本可以跑在本地、自己的 Ray 集群，或 DBay 的 Ray 服务；一份 Dask 脚本同理。**转换后的代码不绑定任何执行环境**。

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
.distify/tasks/{task_id}/
├── meta.json              # 命令、参数、commit、时间戳
├── source/                # 原始代码快照
├── converted/             # 生成的分布式脚本 + diff
├── verification/          # 样本验证产物
└── runs/{run_id}/         # 每次执行的日志和输出
```

默认写在当前目录 `./.distify/`，也可以指向 OBS/S3 共享给团队。

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

早期脚手架。CLI 形态已定型，分析/转换/验证的实现陆续填充中。详见 `DESIGN.md`。

## 贡献

欢迎 Issue 和 PR。添加新框架：在 `src/distify/frameworks/` 下新建一个继承 `Framework` 的类，在 `registry.py` 注册即可。

## 许可

Apache-2.0
