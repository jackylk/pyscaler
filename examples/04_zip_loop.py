"""示例 4：zip 同步遍历两个序列

`for path, tag in zip(files, tags): ...` 这种多序列同步遍历，
pyscaler 会转成 `ray.get([f.remote(path, tag) for path, tag in zip(...)])`。
"""
import glob
import json
import os


def merge(path: str, tag: str) -> None:
    with open(path) as f:
        data = json.load(f)
    data["user_tag"] = tag
    out = path.replace("/input/", "/output/")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(data, f)


if __name__ == "__main__":
    files = sorted(glob.glob("./data/input/*.json"))
    tags = [f"user-{i}" for i in range(len(files))]
    for path, tag in zip(files, tags):
        merge(path, tag)
