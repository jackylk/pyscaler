# xscale CLI 用户指南

适用版本：0.0.1+

## 安装

```bash
pip install "xscale[ray]"            # 推荐：包含 Ray 运行时
pip install "xscale[ray,llm]"        # 启用 LLM 辅助转换
pip install -e ".[ray,dev]"           # 源码开发
```

安装后 `xscale` 命令即可使用。

## 全局选项

```
xscale --version        # 显示版本
xscale --help           # 命令总览
xscale <cmd> --help     # 单个子命令的选项
```

## 子命令

### `xscale analyze <path>`

**作用：** 静态分析 Python 文件，找出可并行的模式，指出阻塞点。

```bash
xscale analyze ./process.py
```

**输出**：
- `Pattern`：识别到的模式。支持 `parallel_loop`、`dataframe_apply`；`unknown` 表示没发现可并行点
- `Parallel-loop candidate` / `DataFrame apply`：定位到具体行
- `Blockers`：会阻碍并行化的代码问题（如函数修改了全局变量）
- `Notes`：注解说明
- 结尾：`✓ Ready to convert` 或 `⚠ Not ready`

**退出码**：
- `0`：分析完成（不代表一定能并行，看 Notes）
- `1`：文件不存在

---

### `xscale convert <path>`

**作用：** 把 Python 脚本转成指定框架的分布式版本。

```bash
xscale convert ./process.py \
  --framework ray \
  --workers 8 \
  --output ./process_ray.py
```

**参数**：

| 参数 | 简写 | 默认 | 说明 |
|---|---|---|---|
| `--framework` | `-f` | `ray` | 目标框架，见 `xscale frameworks` |
| `--workers` | | `8` | 并发数（影响 `ray.init` 与分片数量） |
| `--output` | | `<原名>_dist.py` | 输出文件 |

**行为**：
1. 先跑 `analyze`。如果分析器判"不适合"，拒绝转换并退出码 `2`
2. 基于模式选择模板，AST 级改写
3. 标准输出打印 diff 和 summary
4. 写入目标文件

**支持的模式**：

| Pattern | 源 | 目标 |
|---|---|---|
| `parallel_loop` | `for x in items: f(x)` | `ray.get([f.remote(x) for x in items])` + `@ray.remote` |
| `dataframe_apply` | `df[col] = df.apply(func, axis=1)` | 切片 → `ray.remote` 分片 apply → `pd.concat` |

**退出码**：
- `0`：转换成功
- `1`：输入文件不存在
- `2`：分析器阻止转换（查看错误输出的 blockers）

---

### `xscale verify <script>`

**作用：** 同时跑原版和分布式版，对比实际加速比。

```bash
xscale verify ./process_dist.py \
  --original ./process.py \
  --input ./data/
```

**参数**：

| 参数 | 简写 | 默认 | 说明 |
|---|---|---|---|
| `--input` | `-i` | （必填） | 输入数据目录 |
| `--original` | | 自动（去 `_dist` 后缀） | 原始脚本路径 |
| `--sample` | | `0.2` | 样本比例（当前版本尚未实际抽样，全量对比） |

**输出示例**：
```
running original ./process.py
  state=succeeded duration=2.14s
running distributed ./process_dist.py
  state=succeeded duration=0.42s

Measured speedup: 5.10×
```

**注意**：当前版本不会自动 diff 输出目录，需要用户自查。输出等价性会在后续版本加进来。

---

### `xscale run <script>`

**作用：** 选择后端执行分布式脚本。

```bash
xscale run ./process_dist.py --backend local --input ./data/
```

**参数**：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--backend` | `local` | 执行后端：`local` 已实现；`ray://HOST:PORT` 和 `dbay` 在开发中 |
| `--input` / `-i` | 必填 | 输入路径（local dir / s3:// / obs://） |
| `--output` / `-o` | | 输出路径 |

**退出码**：脚本的返回码（成功为 `0`，失败会透传）

---

### `xscale frameworks`

列出已注册的框架。

```
$ xscale frameworks
  • ray
  • aura
```

---

## 常见工作流

### 场景 1：文件循环加速

```bash
xscale analyze process.py          # 1. 分析
xscale convert process.py -f ray   # 2. 转换 → process_dist.py
xscale verify process_dist.py -i data/   # 3. 验证
xscale run process_dist.py -i data/      # 4. 执行
```

### 场景 2：DataFrame 计算加速

```bash
cat > score.py <<EOF
import pandas as pd

def compute_score(row):
    return row["a"] * 2 + row["b"] ** 0.5

if __name__ == "__main__":
    df = pd.read_parquet("./events.parquet")
    df["score"] = df.apply(compute_score, axis=1)
    df.to_parquet("./events_scored.parquet")
EOF

xscale analyze score.py            # 识别为 dataframe_apply
xscale convert score.py --workers 4
xscale run score_dist.py -i .      # 在本地 Ray 上跑
```

### 场景 3：代码被阻塞

```bash
xscale analyze bad.py
# Output:
# Pattern: unknown
# Blockers:
#   • function `process` mutates module-level `counter` at line 7
# ⚠ Not ready to convert as-is
```

解决：把 `counter` 改成函数参数/返回值，去掉隐式共享状态。

---

## 工作目录

每个任务产出默认写在输入文件旁边（`process_dist.py`）。未来会引入 `.xscale/tasks/{id}/` 目录保存元数据、日志、验证报告。

---

## 退出码速查

| 码 | 含义 |
|---|---|
| `0` | 成功 |
| `1` | 输入路径无效 |
| `2` | 分析器拒绝（代码有阻塞点） |
| `3` | 验证失败 |
| `4` | 请求的后端未实现 |

---

## 排错

**Q: `not implemented yet` 错误？**
当前版本仅实现了 `local` backend 和 `ray` framework。其他选项会明确告诉你。

**Q: 转换后的脚本 docstring/注释丢了？**
`ast.unparse` 不保留原文格式。后续版本会用 `libcst` 保留源码结构。

**Q: 能转换我的代码吗？**
先跑 `xscale analyze`。它会直接告诉你模式是什么、有没有阻塞。
