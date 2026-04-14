"""
示例 2：DataFrame 上的 apply/map

典型场景：一个大 DataFrame，按行或按列做计算。
xscale 会把这种模式转成 ray.data 的并行 map。
"""
import pandas as pd


def compute_score(row):
    # 模拟一些 CPU 开销
    return row["a"] * 2 + row["b"] ** 0.5


if __name__ == "__main__":
    df = pd.read_parquet("./data/events.parquet")
    df["score"] = df.apply(compute_score, axis=1)
    df.to_parquet("./data/events_scored.parquet")
    print(f"scored {len(df)} rows")
