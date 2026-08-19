from __future__ import annotations

from .base import AgentDriver
from .claude import ClaudeDriver
from .opencode import OpencodeDriver

_DEFAULT_MODELS = {
    "opencode": "openrouter/deepseek/deepseek-v4-flash",
    "claude": "haiku",
}


def make_driver(name: str, *, model: str | None = None, **opts) -> AgentDriver:
    name = name.lower()
    model = model or _DEFAULT_MODELS.get(name)
    if name == "opencode":
        return OpencodeDriver(model=model, **opts)
    if name == "claude":
        return ClaudeDriver(model=model, **opts)
    raise ValueError(f"unknown agent driver {name!r}; choose from opencode|claude")
