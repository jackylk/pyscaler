"""DBay remote Ray cluster backend.

Submits a generated script to a DBay-hosted Ray cluster via HTTP.

Required environment:
- XSCALE_DBAY_ENDPOINT: base URL of DBay orchestrator (e.g. https://api.dbay.cloud:8443)
- XSCALE_DBAY_TOKEN:    DBay API key (lk_...)

Server-side contract (the API pyscaler expects DBay to expose):

  POST {endpoint}/v1/ray-jobs
    headers: Authorization: Bearer {token}
    form/multipart:
      script:  the generated distributed .py file
      input:   input path (obs:// or s3://)
      output:  output path (obs:// or s3://)
    → 202 Accepted, body: {"job_id": "..."}

  GET  {endpoint}/v1/ray-jobs/{job_id}
    → 200, body: {"state": "pending|running|succeeded|failed",
                  "duration": 12.3, "returncode": 0}

  GET  {endpoint}/v1/ray-jobs/{job_id}/logs?tail=N
    → 200, body: {"lines": ["..."]}

This backend is forward-compatible with that contract. Until the server side
is implemented, `submit()` raises a clear error; the server-side endpoint can
be built independently of pyscaler.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from .base import Backend


class DBayBackend(Backend):
    name = "dbay"

    def __init__(
        self,
        endpoint: str | None = None,
        token: str | None = None,
        poll_interval: float = 2.0,
        timeout: float = 3600.0,
    ) -> None:
        self.endpoint = (endpoint or os.environ.get("XSCALE_DBAY_ENDPOINT", "")).rstrip("/")
        self.token = token or os.environ.get("XSCALE_DBAY_TOKEN", "")
        self.poll_interval = poll_interval
        self.timeout = timeout
        if not self.endpoint:
            raise RuntimeError(
                "DBay backend requires XSCALE_DBAY_ENDPOINT (or endpoint=) — e.g. "
                "https://api.dbay.cloud:8443"
            )
        if not self.token:
            raise RuntimeError(
                "DBay backend requires XSCALE_DBAY_TOKEN (or token=) — your DBay API key"
            )

    def _http(self):
        try:
            import httpx  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "DBay backend needs httpx. Install with `pip install 'pyscaler[dbay]'`."
            ) from e
        return httpx.Client(
            base_url=self.endpoint,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=60.0,
        )

    def submit(self, script: Path, input_path: str, output_path: str, **kwargs) -> str:
        """Upload script + register run. Returns job_id once accepted."""
        with self._http() as http:
            resp = http.post(
                "/v1/ray-jobs",
                files={"script": (script.name, script.read_bytes(), "text/x-python")},
                data={"input": input_path, "output": output_path},
            )
            resp.raise_for_status()
            return resp.json()["job_id"]

    def status(self, run_id: str) -> dict:
        with self._http() as http:
            resp = http.get(f"/v1/ray-jobs/{run_id}")
            resp.raise_for_status()
            return resp.json()

    def logs(self, run_id: str, tail: int = 100) -> list[str]:
        with self._http() as http:
            resp = http.get(f"/v1/ray-jobs/{run_id}/logs", params={"tail": tail})
            resp.raise_for_status()
            return resp.json().get("lines", [])

    def wait(self, run_id: str) -> dict:
        """Block until job reaches terminal state. Returns final status."""
        deadline = time.time() + self.timeout
        while time.time() < deadline:
            s = self.status(run_id)
            if s.get("state") in ("succeeded", "failed"):
                return s
            time.sleep(self.poll_interval)
        raise TimeoutError(f"DBay job {run_id} did not finish in {self.timeout}s")
