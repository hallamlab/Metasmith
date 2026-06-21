"""Transitional shim — the container abstraction moved to `metasmith.env`.

`Container`/`ContainerRuntime` were renamed to `Environment`/`Runtime` and
relocated to the sealed `env` package. This module re-exports the old names
so existing import sites keep working while they migrate. It will be deleted
once nothing imports from `metasmith.coms.containers`.
"""

from ..env.environment import Environment as Container, Runtime as ContainerRuntime

__all__ = ["Container", "ContainerRuntime"]
