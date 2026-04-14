"""Framework registry — add new frameworks here."""
from .base import Framework
from .ray import RayFramework
from .aura import AuraFramework


_REGISTRY: dict[str, type[Framework]] = {
    "ray": RayFramework,
    "aura": AuraFramework,
}


def get_framework(name: str) -> Framework:
    if name not in _REGISTRY:
        raise ValueError(f"Unknown framework: {name}. Available: {list(_REGISTRY)}")
    return _REGISTRY[name]()


def available_frameworks() -> list[str]:
    return list(_REGISTRY.keys())
