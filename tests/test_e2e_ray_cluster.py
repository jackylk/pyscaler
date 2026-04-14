"""End-to-end test against a real Ray cluster (local head node).

Mode: "Ray local cluster" — we start a standalone cluster via `ray start --head`,
set RAY_ADDRESS, run the generated script, verify outputs, then shut the cluster down.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
from dataclasses import asdict
from pathlib import Path

import pytest

ray = pytest.importorskip("ray")

from pyscaler.analyzer import analyze_file
from pyscaler.backends.local import LocalBackend
from pyscaler.frameworks.registry import get_framework


FIXTURE_TEMPLATE = textwrap.dedent('''
    import glob
    import json
    import os


    def process_file(path: str) -> dict:
        with open(path) as f:
            data = json.load(f)
        result = {{"file": os.path.basename(path), "n": len(data.get("records", []))}}
        out_path = path.replace("/input/", "/output/")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(result, f)
        return result


    if __name__ == "__main__":
        files = sorted(glob.glob("{input_glob}"))
        for f in files:
            process_file(f)
        print(f"processed {{len(files)}} files")
''').lstrip()


# Used by smoke tests that don't actually execute the script
FIXTURE = FIXTURE_TEMPLATE.format(input_glob="./data/input/*.json")


@pytest.fixture(scope="module")
def ray_local_cluster():
    """Start a local Ray head node for the duration of this module; tear down after."""
    # Stop any lingering cluster first
    subprocess.run(["ray", "stop", "--force"], capture_output=True)

    start = subprocess.run(
        ["ray", "start", "--head", "--num-cpus=2",
         "--dashboard-host=127.0.0.1", "--disable-usage-stats"],
        capture_output=True, text=True, timeout=60,
    )
    if start.returncode != 0:
        pytest.skip(f"could not start local Ray cluster: {start.stderr}")

    # Use `auto` → driver attaches to the running head node directly (not via ray client),
    # so user code defined in __main__ is resolved normally without cross-process serialization.
    address = "auto"
    yield address

    subprocess.run(["ray", "stop", "--force"], capture_output=True, timeout=30)


def _write_inputs(root: Path, n: int) -> None:
    (root / "data" / "input").mkdir(parents=True, exist_ok=True)
    for i in range(n):
        (root / "data" / "input" / f"f{i}.json").write_text(
            json.dumps({"records": list(range(i + 1))})
        )


def _read_outputs(root: Path) -> dict:
    out = {}
    d = root / "data" / "output"
    if d.exists():
        for f in sorted(d.glob("*.json")):
            out[f.name] = json.loads(f.read_text())
    return out


def test_generated_script_contains_ray_address_branch():
    """The emitted init block should auto-detect RAY_ADDRESS — no manual edit needed."""
    src_path = Path(__file__).parent / "_tmp_fixture.py"
    src_path.write_text(FIXTURE)
    try:
        a = analyze_file(src_path)
        ad = asdict(a)
        ad["path"] = str(a.path)
        result = get_framework("ray").convert(FIXTURE, ad, workers=2)
        assert 'RAY_ADDRESS' in result.converted
        assert 'ray.init(address=_ray_addr' in result.converted
        assert 'num_cpus=2' in result.converted  # local fallback
    finally:
        src_path.unlink(missing_ok=True)


def test_e2e_against_local_ray_cluster(tmp_path: Path, ray_local_cluster: str):
    """Generate a Ray script and run it against a real local Ray cluster.

    Uses ABSOLUTE paths in the script so worker sandboxes write back to the
    shared local filesystem — cluster mode requires absolute or cloud-URI paths.
    """
    abs_glob = str(tmp_path / "data" / "input" / "*.json")
    fixture = FIXTURE_TEMPLATE.format(input_glob=abs_glob)
    src = tmp_path / "orig.py"
    src.write_text(fixture)
    _write_inputs(tmp_path, n=6)

    # Reference run — original, single-process
    backend = LocalBackend()
    rid1 = backend.submit(src, "", "", cwd=str(tmp_path))
    s1 = backend.status(rid1)
    assert s1["state"] == "succeeded", backend.logs(rid1)
    reference = _read_outputs(tmp_path)
    assert len(reference) == 6
    shutil.rmtree(tmp_path / "data" / "output")

    # Convert
    a = analyze_file(src)
    ad = asdict(a)
    ad["path"] = str(a.path)
    result = get_framework("ray").convert(fixture, ad, workers=2)
    dist = tmp_path / "orig_dist.py"
    dist.write_text(result.converted)

    # Run against the real cluster
    env = os.environ.copy()
    env["RAY_ADDRESS"] = ray_local_cluster
    rid2 = backend.submit(dist, "", "", cwd=str(tmp_path), env=env)
    s2 = backend.status(rid2)
    assert s2["state"] == "succeeded", backend.logs(rid2)

    actual = _read_outputs(tmp_path)
    assert actual == reference, "cluster run produced different outputs from reference"


@pytest.mark.skipif(
    not os.environ.get("XSCALE_DBAY_TEST"),
    reason="set XSCALE_DBAY_TEST=1 (and DBAY_* creds) to run remote DBay Ray test",
)
def test_e2e_against_dbay_remote_cluster(tmp_path: Path):
    """Placeholder for DBay remote Ray cluster E2E. Skipped until DBay backend implemented."""
    pytest.skip("DBay backend not implemented yet — this test reserves the slot.")
