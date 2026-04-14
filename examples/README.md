# 示例

| 文件 | 场景 | pyscaler 能否处理 |
|---|---|---|
| `01_file_loop.py` | 目录下多文件串行处理 | ✅ 适合 ray.remote 文件级并行 |
| `02_dataframe_map.py` | DataFrame 按行计算 | ✅ 适合 ray.data map |
| `03_blocked_by_state.py` | 依赖全局状态的循环 | ❌ 不能直接并行化 |

用法：

```bash
pyscaler analyze examples/01_file_loop.py
pyscaler convert examples/01_file_loop.py --framework ray
```
