"""
Example target: a typical single-machine data processing loop.
This is the kind of code Rayify should detect and convert.
"""
import glob
import pandas as pd


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def process_file(path: str) -> str:
    df = pd.read_parquet(path)
    df = clean(df)
    out = path.replace("/raw/", "/cleaned/")
    df.to_parquet(out)
    return out


if __name__ == "__main__":
    files = sorted(glob.glob("./data/raw/*.parquet"))
    for f in files:
        process_file(f)
    print(f"processed {len(files)} files")
