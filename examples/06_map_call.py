"""示例 6：list(map(f, xs))

`list(map(f, xs))` 与列表推导等价，pyscaler 同样转成
`results = ray.get([f.remote(x) for x in xs])`。
"""
import glob
import json
import os


def annotate(path: str) -> dict:
    with open(path) as f:
        d = json.load(f)
    d["annotated"] = True
    out = path.replace("/input/", "/output/")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(d, f)
    return d


if __name__ == "__main__":
    files = sorted(glob.glob("./data/input/*.json"))
    results = list(map(annotate, files))
    print(f"annotated {len(results)} files")
