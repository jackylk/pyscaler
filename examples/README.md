# 示例

| 文件 | 场景 | pyscaler 能否处理 |
|---|---|---|
| `01_file_loop.py` | 目录下多文件串行处理 (`for x in xs: f(x)`) | ✅ ray.remote 文件级并行 |
| `02_dataframe_map.py` | DataFrame 按行计算 (`df.apply`) | ✅ 分片并行 apply |
| `03_blocked_by_state.py` | 依赖全局状态的循环 | ❌ 不能直接并行化 |
| `04_zip_loop.py` | 多序列同步遍历 (`for a,b in zip(...)`) | ✅ |
| `05_list_comp.py` | 列表推导 (`[f(x) for x in xs]`) | ✅ |
| `06_map_call.py` | `list(map(f, xs))` | ✅ |

用法：

```bash
pyscaler analyze examples/01_file_loop.py
pyscaler convert examples/01_file_loop.py --framework ray
```
