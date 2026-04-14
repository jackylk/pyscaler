"""
示例 1：文件级循环处理

典型场景：一个目录下若干数据文件，逐个读取、处理、写出。
pyscaler 会把这种串行 for 循环转成 Ray 的文件级并行。

运行原始版本：
    python examples/01_file_loop.py

pyscaler 化之后：
    pyscaler analyze examples/01_file_loop.py
    pyscaler convert examples/01_file_loop.py --framework ray
"""
import glob
import json
import os
import time


def process_file(path: str) -> dict:
    """读取 JSON 文件，简单处理后写回。典型的 I/O + 轻量 CPU 场景。"""
    with open(path) as f:
        data = json.load(f)
    # 模拟一些处理开销
    time.sleep(0.05)
    result = {"file": os.path.basename(path), "n_records": len(data.get("records", []))}
    out_path = path.replace("/input/", "/output/")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(result, f)
    return result


if __name__ == "__main__":
    files = sorted(glob.glob("./data/input/*.json"))
    for f in files:
        process_file(f)
    print(f"processed {len(files)} files")
