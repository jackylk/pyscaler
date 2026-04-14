"""Ray framework plugin. Stub — to be filled in during Phase 2."""
from .base import ConversionResult, Framework


class RayFramework(Framework):
    name = "ray"

    def supports(self, analysis: dict) -> bool:
        return analysis.get("pattern") in ("parallel_loop", "map_filter", "file_fanout")

    def convert(self, source: str, analysis: dict, workers: int = 8) -> ConversionResult:
        raise NotImplementedError("Ray conversion templates not implemented yet.")

    def runtime_dependencies(self) -> list[str]:
        return ["ray[default]>=2.9"]
