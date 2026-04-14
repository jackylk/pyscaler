"""Framework plugins: how code is transformed.

Each framework knows how to turn common single-machine patterns into
its own distributed idiom (ray.remote, dask.delayed, spark RDD, etc.).
"""
from .base import Framework

__all__ = ["Framework"]
