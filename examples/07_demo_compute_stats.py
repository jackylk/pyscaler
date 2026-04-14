"""pyscaler 演示：处理 20 个 JSON 文件，每个文件做一些 CPU 计算。

用这个脚本在本地试用 pyscaler 的完整流程：

    pyscaler analyze examples/07_demo_compute_stats.py
    pyscaler convert examples/07_demo_compute_stats.py --workers 4
    python examples/07_demo_compute_stats.py                    # 跑原版
    rm -rf data/out
    python examples/07_demo_compute_stats_dist.py               # 跑 Ray 版

这个脚本会：
1. 第一次运行自动生成 ./data/in/*.json 测试数据（20 个文件）
2. 逐个读取、计算统计（带 CPU 开销）
3. 写入 ./data/out/

单机串行大约 4-6 秒；pyscaler 化后 4 workers 约 1.5-2.5 秒（含 Ray 冷启）。
"""
import glob
import json
import math
import os
import time


def analyze_file(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)
    nums = data["numbers"]

    # 一点 CPU 开销：大量 sqrt/log 调用，模拟特征提取
    score = 0.0
    for _ in range(150_000):
        for n in nums[:10]:
            score += math.sqrt(n) * math.log(n + 1)

    result = {
        "file": os.path.basename(path),
        "n": len(nums),
        "sum": sum(nums),
        "mean": sum(nums) / len(nums),
        "score": round(score, 3),
    }
    out_path = path.replace("/in/", "/out/")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(result, f)
    return result


def _ensure_inputs() -> None:
    """Auto-generate test inputs if missing."""
    if os.path.isdir("./data/in") and glob.glob("./data/in/*.json"):
        return
    import random
    os.makedirs("./data/in", exist_ok=True)
    random.seed(42)
    for i in range(20):
        nums = [random.randint(1, 1000) for _ in range(100)]
        with open(f"./data/in/f{i:02d}.json", "w") as fh:
            json.dump({"numbers": nums}, fh)
    print("Generated 20 input files in ./data/in/")


if __name__ == "__main__":
    _ensure_inputs()
    files = sorted(glob.glob("./data/in/*.json"))
    start = time.time()
    for f in files:
        analyze_file(f)
    elapsed = time.time() - start
    print(f"Processed {len(files)} files in {elapsed:.2f}s")
