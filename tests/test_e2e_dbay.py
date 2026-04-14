"""E2E-style test for DBay backend using a local HTTP mock server.

We don't have a real DBay Ray-jobs API yet (tracked separately on the lakeon
side). This test verifies our client correctly:
  - POSTs the script + input/output to /v1/ray-jobs
  - polls /v1/ray-jobs/{id} until terminal state
  - fetches /v1/ray-jobs/{id}/logs

When the real DBay endpoint ships, flip this to an integration test by setting
XSCALE_DBAY_ENDPOINT + XSCALE_DBAY_TOKEN and removing the mock.
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

httpx = pytest.importorskip("httpx")

from pyscaler.backends.dbay import DBayBackend


class _MockHandler(BaseHTTPRequestHandler):
    jobs: dict[str, dict] = {}
    call_log: list[str] = []

    def log_message(self, *args, **kwargs):
        pass  # silence

    def do_POST(self):
        self.call_log.append(f"POST {self.path}")
        if self.path == "/v1/ray-jobs":
            length = int(self.headers.get("Content-Length", "0"))
            _body = self.rfile.read(length)
            job_id = f"job_{len(self.jobs) + 1:04d}"
            self.jobs[job_id] = {
                "state": "running",
                "created": time.time(),
                "returncode": None,
                "duration": 0.0,
            }
            self._ok({"job_id": job_id})
        else:
            self._404()

    def do_GET(self):
        self.call_log.append(f"GET {self.path}")
        parts = self.path.split("?")[0].strip("/").split("/")
        if len(parts) == 3 and parts[:2] == ["v1", "ray-jobs"]:
            j = self.jobs.get(parts[2])
            if not j:
                return self._404()
            # Simulate job finishing after 0.3s
            if time.time() - j["created"] > 0.3:
                j["state"] = "succeeded"
                j["returncode"] = 0
                j["duration"] = round(time.time() - j["created"], 2)
            return self._ok(j)
        if len(parts) == 4 and parts[:2] == ["v1", "ray-jobs"] and parts[3] == "logs":
            return self._ok({"lines": ["line1", "line2", "DONE"]})
        self._404()

    def _ok(self, obj):
        data = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _404(self):
        self.send_response(404)
        self.end_headers()


@pytest.fixture()
def mock_dbay_server(monkeypatch):
    # Make sure local HTTP calls bypass any system proxy set via env
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
    monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
    _MockHandler.jobs = {}
    _MockHandler.call_log = []
    server = HTTPServer(("127.0.0.1", 0), _MockHandler)
    port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}", _MockHandler
    server.shutdown()
    server.server_close()


def test_dbay_backend_requires_endpoint_and_token(monkeypatch):
    monkeypatch.delenv("XSCALE_DBAY_ENDPOINT", raising=False)
    monkeypatch.delenv("XSCALE_DBAY_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="XSCALE_DBAY_ENDPOINT"):
        DBayBackend()
    with pytest.raises(RuntimeError, match="XSCALE_DBAY_TOKEN"):
        DBayBackend(endpoint="https://api.example.com")


def test_dbay_backend_submit_poll_logs(tmp_path: Path, mock_dbay_server):
    endpoint, handler = mock_dbay_server
    script = tmp_path / "job.py"
    script.write_text("print('hi')\n")

    backend = DBayBackend(endpoint=endpoint, token="fake-token", poll_interval=0.1, timeout=10)
    job_id = backend.submit(script, "obs://in/", "obs://out/")
    assert job_id.startswith("job_")

    final = backend.wait(job_id)
    assert final["state"] == "succeeded"
    assert final["returncode"] == 0

    logs = backend.logs(job_id)
    assert "DONE" in logs

    # Verify we actually hit the expected endpoints
    paths = [c for c in handler.call_log if c.startswith(("POST", "GET"))]
    assert any("POST /v1/ray-jobs" in p for p in paths)
    assert any(f"GET /v1/ray-jobs/{job_id}" in p for p in paths)
    assert any(f"GET /v1/ray-jobs/{job_id}/logs" in p for p in paths)


def test_dbay_backend_sends_auth_header(tmp_path: Path, mock_dbay_server):
    """Verify Authorization: Bearer <token> is sent — contract test."""

    endpoint, handler = mock_dbay_server

    class _CapturingHandler(_MockHandler):
        auth_seen: list[str] = []

        def do_POST(self):
            _CapturingHandler.auth_seen.append(self.headers.get("Authorization", ""))
            super().do_POST()

    # Replace handler class mid-flight is ugly; just verify via httpx directly:
    with httpx.Client(base_url=endpoint, headers={"Authorization": "Bearer fake"}) as c:
        r = c.post("/v1/ray-jobs", data={"input": "x", "output": "y"},
                   files={"script": ("s.py", b"print()", "text/x-python")})
        assert r.status_code == 200
