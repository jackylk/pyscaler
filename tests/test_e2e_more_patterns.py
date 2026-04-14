"""E2E for the additional fan-out patterns: zip-loop, list-comp, map-call.

Each test: write fixture, run original, clear outputs, convert, run Ray version,
assert outputs identical and generated code contains the expected Ray idioms.
"""
from __future__ import annotations

import json
import shutil
import textwrap
from dataclasses import asdict
from pathlib import Path

import pytest

ray = pytest.importorskip("ray")

from pyscaler.analyzer import analyze_file
from pyscaler.backends.local import LocalBackend
from pyscaler.frameworks.registry import get_framework


def _run_and_collect(tmp_path: Path, src: Path, fixture: str) -> dict[str, dict]:
    (tmp_path / "data" / "input").mkdir(parents=True, exist_ok=True)
    for i in range(6):
        (tmp_path / "data" / "input" / f"f{i}.json").write_text(
            json.dumps({"n": i + 1, "tag": f"t{i}"})
        )
    backend = LocalBackend()
    rid = backend.submit(src, "", "", cwd=str(tmp_path))
    assert backend.status(rid)["state"] == "succeeded", backend.logs(rid)
    out = {}
    for f in sorted((tmp_path / "data" / "output").glob("*.json")):
        out[f.name] = json.loads(f.read_text())
    return out


def _compare_paths(tmp_path: Path, fixture: str) -> tuple[dict, dict]:
    """Run fixture (original + Ray) and return (orig_out, dist_out)."""
    src = tmp_path / "orig.py"
    src.write_text(fixture)
    orig = _run_and_collect(tmp_path, src, fixture)
    shutil.rmtree(tmp_path / "data" / "output")

    a = analyze_file(src)
    ad = asdict(a)
    ad["path"] = str(a.path)
    result = get_framework("ray").convert(fixture, ad, workers=2)
    dist = tmp_path / "orig_dist.py"
    dist.write_text(result.converted)

    backend = LocalBackend()
    rid = backend.submit(dist, "", "", cwd=str(tmp_path))
    assert backend.status(rid)["state"] == "succeeded", backend.logs(rid)
    out = {}
    for f in sorted((tmp_path / "data" / "output").glob("*.json")):
        out[f.name] = json.loads(f.read_text())
    return orig, out, result.converted


# ---------- zip loop ----------

ZIP_FIXTURE = textwrap.dedent("""
    import glob, json, os


    def merge(path, tag):
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
""").lstrip()


def test_analyzer_detects_zip_loop(tmp_path: Path):
    src = tmp_path / "orig.py"
    src.write_text(ZIP_FIXTURE)
    a = analyze_file(src)
    assert a.pattern == "parallel_loop"
    assert a.loop is not None
    assert a.loop.kind == "zip_loop"
    assert a.loop.var_names == ["path", "tag"]
    assert a.loop.call_func == "merge"


def test_e2e_zip_loop(tmp_path: Path):
    orig, dist, code = _compare_paths(tmp_path, ZIP_FIXTURE)
    assert dist == orig
    assert "@ray.remote" in code
    assert "ray.get(" in code
    assert ".remote(path, tag)" in code


# ---------- list comprehension ----------

LISTCOMP_FIXTURE = textwrap.dedent("""
    import glob, json, os


    def label(path):
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
        print(f"labeled {len(results)}")
""").lstrip()


def test_analyzer_detects_list_comp(tmp_path: Path):
    src = tmp_path / "orig.py"
    src.write_text(LISTCOMP_FIXTURE)
    a = analyze_file(src)
    assert a.pattern == "parallel_loop"
    assert a.loop.kind == "list_comp"
    assert a.loop.assign_target == "results"
    assert a.loop.call_func == "label"


def test_e2e_list_comp(tmp_path: Path):
    orig, dist, code = _compare_paths(tmp_path, LISTCOMP_FIXTURE)
    assert dist == orig
    assert "results = ray.get(" in code
    assert ".remote(p)" in code


# ---------- list(map(...)) ----------

MAPCALL_FIXTURE = textwrap.dedent("""
    import glob, json, os


    def annotate(path):
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
        print(f"annotated {len(results)}")
""").lstrip()


def test_analyzer_detects_map_call(tmp_path: Path):
    src = tmp_path / "orig.py"
    src.write_text(MAPCALL_FIXTURE)
    a = analyze_file(src)
    assert a.pattern == "parallel_loop"
    assert a.loop.kind == "map_call"
    assert a.loop.assign_target == "results"
    assert a.loop.call_func == "annotate"


def test_e2e_map_call(tmp_path: Path):
    orig, dist, code = _compare_paths(tmp_path, MAPCALL_FIXTURE)
    assert dist == orig
    assert "results = ray.get(" in code
    assert ".remote(" in code
