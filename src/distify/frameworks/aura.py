"""Aura framework plugin. Placeholder — implementation coming when Aura API stabilizes."""
from .base import ConversionResult, Framework


class AuraFramework(Framework):
    name = "aura"

    def supports(self, analysis: dict) -> bool:
        return False  # not implemented yet

    def convert(self, source: str, analysis: dict, workers: int = 8) -> ConversionResult:
        raise NotImplementedError("Aura framework is planned, not implemented.")

    def runtime_dependencies(self) -> list[str]:
        return ["aura-sdk"]  # placeholder name
