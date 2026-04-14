"""示例 5：列表推导

`results = [f(x) for x in xs]` 本质是 for-loop 的语法糖，
pyscaler 会转成 `results = ray.get([f.remote(x) for x in xs])`。
"""
import glob
import json
import os


def label(path: str) -> dict:
    with open(path) as f:
        d = json.load(f)
    d["labeled"] = True
    out = path.replace("/input/", "/output/")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(d, f)
    return d


if __name__ == "__main__":
    files = sorted(glob.glob("./data/input/*.json"))
    results = [label(p) for p in files]
    print(f"labeled {len(results)} files")
