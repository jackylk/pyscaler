"""End-to-end: analyze → convert → run both versions → compare outputs."""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ray = pytest.importorskip("ray")

from xscale.analyzer import analyze_file
from xscale.backends.local import LocalBackend
from xscale.frameworks.registry import get_framework


FIXTURE = textwrap.dedent("""
    import glob
    import json
    import os


    def process_file(path: str) -> dict:
        with open(path) as f:
            data = json.load(f)
        result = {"file": os.path.basename(path), "n": len(data.get("records", []))}
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
""").lstrip()


def _write_sample_data(root: Path, n: int) -> None:
    (root / "data" / "input").mkdir(parents=True, exist_ok=True)
    for i in range(n):
        (root / "data" / "input" / f"f{i}.json").write_text(
            json.dumps({"records": list(range(i + 1))})
        )


def _read_outputs(root: Path) -> dict[str, dict]:
    out = {}
    out_dir = root / "data" / "output"
    if out_dir.exists():
        for f in sorted(out_dir.glob("*.json")):
            out[f.name] = json.loads(f.read_text())
    return out


def test_analyzer_detects_parallel_loop(tmp_path: Path):
    src = tmp_path / "orig.py"
    src.write_text(FIXTURE)
    a = analyze_file(src)
    assert a.pattern == "parallel_loop"
    assert a.loop is not None
    assert a.loop.call_func == "process_file"
    assert a.loop.var_name == "f"
    assert not a.blockers
    assert a.supports_parallel


def test_analyzer_blocks_on_global_mutation(tmp_path: Path):
    src = tmp_path / "bad.py"
    src.write_text(textwrap.dedent("""
        counter = {"total": 0}
        def process(x):
            counter["total"] += 1
            return x * 2
        if __name__ == "__main__":
            for x in range(10):
                process(x)
    """).lstrip())
    a = analyze_file(src)
    assert not a.supports_parallel
    assert a.blockers


def test_converter_produces_valid_ray_script(tmp_path: Path):
    src = tmp_path / "orig.py"
    src.write_text(FIXTURE)
    a = analyze_file(src)
    from dataclasses import asdict
    ad = asdict(a)
    ad["path"] = str(a.path)
    fw = get_framework("ray")
    result = fw.convert(FIXTURE, ad, workers=2)

    # Syntax-valid
    import ast
    ast.parse(result.converted)
    # Contains expected Ray idioms
    assert "import ray" in result.converted
    assert "@ray.remote" in result.converted
    assert "ray.get(" in result.converted
    assert ".remote(f)" in result.converted


def test_end_to_end_local_run(tmp_path: Path):
    # 1) write fixture source
    src = tmp_path / "orig.py"
    src.write_text(FIXTURE)

    # 2) sample data
    _write_sample_data(tmp_path, n=6)

    # 3) run original
    backend = LocalBackend()
    rid1 = backend.submit(src, "", "", cwd=str(tmp_path))
    s1 = backend.status(rid1)
    assert s1["state"] == "succeeded", backend.logs(rid1)
    orig_out = _read_outputs(tmp_path)
    assert len(orig_out) == 6

    # 4) clear outputs so we know the Ray run produced them
    shutil.rmtree(tmp_path / "data" / "output")

    # 5) convert → write dist script
    a = analyze_file(src)
    from dataclasses import asdict
    ad = asdict(a)
    ad["path"] = str(a.path)
    result = get_framework("ray").convert(FIXTURE, ad, workers=2)
    dist = tmp_path / "orig_dist.py"
    dist.write_text(result.converted)

    # 6) run dist
    rid2 = backend.submit(dist, "", "", cwd=str(tmp_path))
    s2 = backend.status(rid2)
    assert s2["state"] == "succeeded", backend.logs(rid2)
    dist_out = _read_outputs(tmp_path)

    # 7) outputs identical
    assert dist_out == orig_out


def test_cli_analyze_and_convert(tmp_path: Path):
    """Invoke the CLI end-to-end via subprocess to catch wiring bugs."""
    src = tmp_path / "orig.py"
    src.write_text(FIXTURE)

    cmd = [sys.executable, "-m", "xscale.cli", "analyze", str(src)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert "parallel_loop" in r.stdout

    cmd = [sys.executable, "-m", "xscale.cli", "convert", str(src), "--workers", "2"]
    r = subprocess.run(cmd, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert (tmp_path / "orig_dist.py").exists()
