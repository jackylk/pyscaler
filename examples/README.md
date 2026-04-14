# 示例

| 文件 | 场景 | pyscaler 能否处理 |
|---|---|---|
| `01_file_loop.py` | 目录下多文件串行处理 (`for x in xs: f(x)`) | ✅ ray.remote 文件级并行 |
| `02_dataframe_map.py` | DataFrame 按行计算 (`df.apply`) | ✅ 分片并行 apply |
| `03_blocked_by_state.py` | 依赖全局状态的循环 | ❌ 不能直接并行化 |
| `04_zip_loop.py` | 多序列同步遍历 (`for a,b in zip(...)`) | ✅ |
| `05_list_comp.py` | 列表推导 (`[f(x) for x in xs]`) | ✅ |
| `06_map_call.py` | `list(map(f, xs))` | ✅ |
| `07_demo_compute_stats.py` | **端到端演示**：自动造数据、CPU 密集、可测加速比 | ✅ |

用法：

```bash
pyscaler analyze examples/01_file_loop.py
pyscaler convert examples/01_file_loop.py --framework ray
```

## 🚀 5 分钟试用（推荐用例 07）

`07_demo_compute_stats.py` 会自动生成 20 个测试文件、每个文件带 CPU 负载，能直观看到加速比。

```bash
git clone https://github.com/jackylk/pyscaler && cd pyscaler
pip install -e ".[ray]"

cd examples

# 1. 分析（期望识别到 parallel_loop · for_loop）
pyscaler analyze 07_demo_compute_stats.py

# 2. 转换（生成 07_demo_compute_stats_dist.py）
pyscaler convert 07_demo_compute_stats.py --workers 4

# 3. 跑原版（首次运行会自动生成 data/in/）
python 07_demo_compute_stats.py
# → Processed 20 files in ~4-6s

# 4. 清输出，跑 Ray 版
rm -rf data/out
python 07_demo_compute_stats_dist.py
# → Processed 20 files in ~1.5-2.5s（含 Ray 冷启）

# 5. 对比几个输出确保一致
diff <(cat data/out/f00.json) <(cat data/out/f00.json)
```

预期：**2.5-3× 加速**（4 workers，理想 4× 因 Ray 冷启打折）。机器核心数 ≥ 4 效果更明显。
