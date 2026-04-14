"""Local backend — run the generated script as a subprocess in this process's Python."""
from __future__ import annotations

import subprocess
import sys
import time
import uuid
from pathlib import Path

from .base import Backend


class LocalBackend(Backend):
    name = "local"

    def __init__(self) -> None:
        self._runs: dict[str, dict] = {}

    def submit(self, script: Path, input_path: str, output_path: str, **kwargs) -> str:
        run_id = uuid.uuid4().hex[:12]
        start = time.time()
        proc = subprocess.run(
            [sys.executable, str(script)],
            cwd=kwargs.get("cwd"),
            capture_output=True,
            text=True,
            env=kwargs.get("env"),
        )
        self._runs[run_id] = {
            "state": "succeeded" if proc.returncode == 0 else "failed",
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
            "duration": time.time() - start,
            "script": str(script),
        }
        return run_id

    def status(self, run_id: str) -> dict:
        info = self._runs.get(run_id)
        if not info:
            return {"state": "unknown"}
        return {
            "state": info["state"],
            "returncode": info["returncode"],
            "duration": info["duration"],
        }

    def logs(self, run_id: str, tail: int = 100) -> list[str]:
        info = self._runs.get(run_id, {})
        lines = (info.get("stdout", "") + info.get("stderr", "")).splitlines()
        return lines[-tail:]
