# pyscaler 支持的代码形态

本文档说明：pyscaler 当前能把哪些 Python 代码转成 Ray 分布式版本，哪些不能，以及遇到不支持的代码要怎么办。

**核心原则**：pyscaler 的转换是 **AST 模板匹配 + AST 改写**，纯确定性、可审计、离线可跑。只处理预先建模过的模式——没模板就不硬转（避免生成错误代码）。

---

## ✅ 支持的模式

### 1. For 循环 fan-out

```python
# 源
for x in items:
    f(x)

# 转换后
ray.get([f.remote(x) for x in items])
```

**要求**：
- 循环体只有一行，且是函数调用
- 函数名是顶层定义的 name（不支持 `obj.method(x)`）
- 循环变量出现在函数参数里

### 2. Zip 多序列同步遍历

```python
for path, tag in zip(files, tags):
    merge(path, tag)

# → ray.get([merge.remote(path, tag) for path, tag in zip(files, tags)])
```

**要求**：
- 迭代器必须是 `zip(...)` 调用
- 所有解包变量都必须传给函数

### 3. 列表推导

```python
results = [f(x) for x in items]

# → results = ray.get([f.remote(x) for x in items])
```

**要求**：
- 单 generator，单循环变量
- `elt` 是纯函数调用
- 不带 `if` 过滤、不是 async

### 4. `list(map(...))`

```python
results = list(map(f, items))

# → results = ray.get([f.remote(x) for x in items])
```

**要求**：
- 外层必须是 `list(...)` 包裹（保证收集成 list）
- `map()` 只接 2 个参数

### 5. DataFrame 按行 apply

```python
df["score"] = df.apply(compute_score, axis=1)

# → 切分 df 成 N 块 → ray.remote 并行 apply → pd.concat 合并
# 插入 @ray.remote 的 _pyscaler_apply_chunk helper
```

**要求**：
- 形如 `df[col] = df.apply(func, axis=1)`
- col 是字符串字面量
- `func` 是 Name（不能是 lambda）

---

## ❌ 不支持 / 会拒绝的代码

### 1. 共享可变状态

```python
counter = {"total": 0}

def process(x):
    counter["total"] += 1   # ← analyzer 会报 blocker
    return x * 2
```

**怎么办**：把全局状态改成函数参数/返回值，然后手工从 worker 汇总：

```python
def process(x):
    return x * 2, 1   # 返回值 + 本地增量

results = ray.get([process.remote(x) for x in xs])
total = sum(delta for _, delta in results)
```

### 2. 方法调用（非顶层函数）

```python
class Proc:
    def run(self, x): ...

p = Proc()
for x in xs:
    p.run(x)            # ← 不识别：p.run 不是 Name
```

**怎么办**：把方法抽成模块级函数，或把 `Proc` 做成 `@ray.remote` 的 actor（手工）。

### 3. 多行循环体

```python
for x in xs:
    y = prepare(x)      # ← 不识别：循环体不是单次调用
    result = process(y)
    save(result, x)
```

**怎么办**：把三步合并成一个 `process_all(x)` 函数：

```python
def process_all(x):
    y = prepare(x)
    result = process(y)
    save(result, x)

for x in xs:
    process_all(x)
```

### 4. 嵌套循环

```python
for group in groups:
    for item in group.items:
        process(item)   # ← 不识别
```

**怎么办**：手工 flatten，然后走 for-loop 模式：

```python
items = [item for g in groups for item in g.items]
for item in items:
    process(item)
```

### 5. 有过滤的列表推导 / 带多 generator

```python
results = [f(x) for x in xs if x.valid]                 # 有 if
results = [f(x, y) for x in xs for y in ys]             # 多 generator
```

**怎么办**：第一种把过滤拆出来；第二种先 flatten 成单 generator。

### 6. `lambda` 作为处理函数

```python
results = [compute(x) for x in xs]                      # ✅ 支持
results = [(lambda v: v*2)(x) for x in xs]              # ❌ 不支持
df["y"] = df.apply(lambda r: r.a*2, axis=1)             # ❌ 不支持
```

**怎么办**：把 lambda 提升为具名函数。

### 7. 迭代 `enumerate` / `items()` / 自定义生成器

```python
for i, x in enumerate(xs):     # ← 不识别 enumerate 解包
for k, v in d.items():         # ← 不识别 items 解包
```

**怎么办**：只有 `zip(...)` 解包被支持，其他 2-元素解包暂不支持。临时可用 `range(len(xs))` + 索引访问手工改写。

### 8. Groupby 场景

```python
for key, group in df.groupby("k"):
    analyze(group)              # ← 不识别
```

**怎么办**：目前没有 `groupby` 模板；如果改成按独立 group 的 list 做 for-loop 可以走通：

```python
groups = [g for _, g in df.groupby("k")]
for g in groups:
    analyze(g)
```

### 9. 异步代码

```python
async for x in stream:
    await f(x)
```

不支持——并行化路径不同（需要 `ray.async`）。暂未覆盖。

### 10. 已经用了 multiprocessing / concurrent.futures

```python
from multiprocessing import Pool
with Pool(4) as p:
    p.map(f, xs)
```

pyscaler 不会自动把这种改成 Ray。需要手工先拆回单线程 for-loop，再让 pyscaler 转。

---

## 🟡 灰色地带（不保证正确）

### 转换后行为变了的场景

1. **循环体里写日志/监控**：Ray worker 的 stdout 和 driver 不同，日志位置会变
2. **依赖循环顺序**：`ray.get([...])` 结果顺序等同输入顺序，但副作用（比如写同一个文件）会竞争
3. **大对象按值传递**：`ray.remote(f).remote(big_df)` 会把 `big_df` 序列化到每个 worker；对大对象应该用 `ray.put` 预先共享（pyscaler 目前不自动做）

### 包含 I/O 的函数

写 `./data/output/` 这种**相对路径**在：
- Ray 本地 embedded 模式 ✅ 正常
- Ray 本地集群 / DBay 远程集群 ❌ worker 的 CWD 不一样，要改成绝对路径或 OBS URI

详见 [USER_GUIDE.md](./USER_GUIDE.md) 的"三种执行模式"。

---

## 🔍 怎么知道我的代码能不能转？

**先跑 `analyze`，一眼就知道**：

```bash
pyscaler analyze my_script.py
```

输出会告诉你：
- `Pattern: parallel_loop · for_loop` → 可以转，走 for-loop 模板
- `Pattern: dataframe_apply` → 可以转，走 apply 模板
- `Pattern: unknown` → 没匹配到任何模板，不会转
- `Blockers: ...` → 识别了但有阻塞点（如全局状态），列出具体位置

对于 `unknown`：通常原因是代码形态超出了上面 4 个模式。把脚本简化成"纯 for-loop + 纯函数"形式最容易让 pyscaler 接住。

---

## 🗺 路线图

- **Phase 1（已完成）**：4 种 fan-out 变体 + DataFrame apply
- **Phase 2（规划中）**：groupby + enumerate + items() 解包
- **Phase 3（规划中）**：类方法模式（转 ray.actor）
- **Phase 4（长期）**：`--llm-assist` 补缝，处理模板外的形态（在 AST 校验保护下）

我们不做：完全 LLM 生成 Ray 代码。核心原则是可预测、可审计、可离线。LLM 只会作为**模板内的参数修正**，不会主导生成。

---

## 反馈与贡献

遇到 `analyze` 报 `unknown` 但你觉得应该支持的模式，欢迎：
- 在 https://github.com/jackylk/pyscaler/issues 提 Issue，附一份最小 repro 脚本
- 或直接 PR 加新模板（`src/pyscaler/analyzer.py` 加匹配 + `src/pyscaler/frameworks/ray.py` 加改写 + `tests/` 加 E2E）
