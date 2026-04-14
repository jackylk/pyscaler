from abc import ABC, abstractmethod
from pathlib import Path


class Backend(ABC):
    """Execution backend for a generated distributed script."""

    name: str = ""

    @abstractmethod
    def submit(self, script: Path, input_path: str, output_path: str, **kwargs) -> str:
        """Submit the script for execution. Returns a run_id."""

    @abstractmethod
    def status(self, run_id: str) -> dict:
        """Return {state, progress, metrics, ...}."""

    @abstractmethod
    def logs(self, run_id: str, tail: int = 100) -> list[str]:
        ...
