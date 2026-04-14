"""Framework registry — add new frameworks here."""
from .base import Framework
from .ray import RayFramework
from .dask import DaskFramework


_REGISTRY: dict[str, type[Framework]] = {
    "ray": RayFramework,
    "dask": DaskFramework,
}


def get_framework(name: str) -> Framework:
    if name not in _REGISTRY:
        raise ValueError(f"Unknown framework: {name}. Available: {list(_REGISTRY)}")
    return _REGISTRY[name]()


def available_frameworks() -> list[str]:
    return list(_REGISTRY.keys())
