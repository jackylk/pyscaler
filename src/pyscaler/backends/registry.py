"""Backend registry."""
from .base import Backend
from .local import LocalBackend


def get_backend(name: str, **kwargs) -> Backend:
    if name == "local":
        return LocalBackend()
    if name == "dbay":
        from .dbay import DBayBackend  # lazy: needs httpx
        return DBayBackend(**kwargs)
    if name.startswith("ray://") or name == "auto":
        # User's own Ray cluster — use LocalBackend but expect RAY_ADDRESS env to route.
        return LocalBackend()
    raise ValueError(f"Unknown backend: {name}")


def available_backends() -> list[str]:
    return ["local", "ray://<addr>", "dbay"]
