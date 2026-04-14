"""Execution backends: where the generated script runs.

Orthogonal to framework — a Ray script can run on local ray,
a remote ray cluster, or DBay's ray service.
"""
from .base import Backend

__all__ = ["Backend"]
