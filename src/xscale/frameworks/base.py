from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class ConversionResult:
    source: str            # original code
    converted: str         # distributed version
    diff: str              # unified diff (str form)
    summary: dict          # {pattern, changes, predicted_speedup, ...}


class Framework(ABC):
    """A distribution framework plugin (ray, dask, spark, ...)."""

    name: str = ""

    @abstractmethod
    def supports(self, analysis: dict) -> bool:
        """True if this framework is a good fit for the analyzed code."""

    @abstractmethod
    def convert(self, source: str, analysis: dict, workers: int = 8) -> ConversionResult:
        """Transform single-machine code into this framework's idiom."""

    @abstractmethod
    def runtime_dependencies(self) -> list[str]:
        """pip packages needed to execute the converted script."""
