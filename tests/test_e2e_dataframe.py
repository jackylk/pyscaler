"""E2E for DataFrame apply pattern: df[col] = df.apply(func, axis=1)."""
from __future__ import annotations

import textwrap
from dataclasses import asdict
from pathlib import Path

import pytest

ray = pytest.importorskip("ray")
pd = pytest.importorskip("pandas")

from pyscaler.analyzer import analyze_file
from pyscaler.backends.local import LocalBackend
from pyscaler.frameworks.registry import get_framework


FIXTURE = textwrap.dedent("""
    import pandas as pd


    def compute_score(row):
        return row["a"] * 2 + row["b"] ** 0.5


    if __name__ == "__main__":
        df = pd.read_parquet("./events.parquet")
        df["score"] = df.apply(compute_score, axis=1)
        df.to_parquet("./events_scored.parquet")
""").lstrip()


def test_analyzer_detects_dataframe_apply(tmp_path: Path):
    src = tmp_path / "orig.py"
    src.write_text(FIXTURE)
    a = analyze_file(src)
    assert a.pattern == "dataframe_apply"
    assert a.df_apply is not None
    assert a.df_apply.df_name == "df"
    assert a.df_apply.target_col == "score"
    assert a.df_apply.func_name == "compute_score"
    assert a.supports_parallel


def test_end_to_end_dataframe_ray(tmp_path: Path):
    # 1) fixture + input data
    src = tmp_path / "orig.py"
    src.write_text(FIXTURE)
    df = pd.DataFrame({"a": range(50), "b": [x * 0.5 for x in range(50)]})
    df.to_parquet(tmp_path / "events.parquet")

    # 2) run original
    backend = LocalBackend()
    rid1 = backend.submit(src, "", "", cwd=str(tmp_path))
    s1 = backend.status(rid1)
    assert s1["state"] == "succeeded", backend.logs(rid1)
    orig = pd.read_parquet(tmp_path / "events_scored.parquet")

    # 3) convert
    a = analyze_file(src)
    ad = asdict(a)
    ad["path"] = str(a.path)
    result = get_framework("ray").convert(FIXTURE, ad, workers=2)
    dist = tmp_path / "orig_dist.py"
    dist.write_text(result.converted)

    # Sanity on generated code
    assert "_pyscaler_apply_chunk" in result.converted
    assert "_chunk_size" in result.converted
    assert "pd.concat" in result.converted

    # 4) remove output so Ray run must recreate it
    (tmp_path / "events_scored.parquet").unlink()
    (tmp_path / "events.parquet").unlink()
    df.to_parquet(tmp_path / "events.parquet")

    # 5) run dist
    rid2 = backend.submit(dist, "", "", cwd=str(tmp_path))
    s2 = backend.status(rid2)
    assert s2["state"] == "succeeded", backend.logs(rid2)
    dist_out = pd.read_parquet(tmp_path / "events_scored.parquet")

    # 6) Score column matches row-for-row
    pd.testing.assert_series_equal(
        orig["score"].reset_index(drop=True),
        dist_out["score"].reset_index(drop=True),
        check_dtype=False,
    )
