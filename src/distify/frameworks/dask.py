"""Dask framework plugin. Placeholder for future implementation."""
from .base import ConversionResult, Framework


class DaskFramework(Framework):
    name = "dask"

    def supports(self, analysis: dict) -> bool:
        return False  # not implemented yet

    def convert(self, source: str, analysis: dict, workers: int = 8) -> ConversionResult:
        raise NotImplementedError("Dask framework is planned, not implemented.")

    def runtime_dependencies(self) -> list[str]:
        return ["dask[complete]>=2024.1"]
