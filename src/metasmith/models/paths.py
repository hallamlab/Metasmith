from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Literal

from ..constants import AgentPaths


DEFERRED_ROOT = Path("/msm_deferred")
"""Reserved root every deferred path is minted under.

Absolute, and deliberately so. `ops.data.repoint_item` -- the operation that
fills a deferred path in -- reads `is_absolute()` with the opposite meaning to
everywhere else: a *relative* manifest entry is library-owned, so re-pointing it
delegates to `Rename`, a real file move. A relative fake path would therefore be
refused outright (the route asserts old and new agree on absoluteness) and, if
it slipped past, would try to move a file that never existed. The reserved root
also makes the stage refusal legible: the message names a path a reader can see
is not theirs.
"""


class _DeferredPath(Enum):
    DEFERRED = "deferred"

    def __str__(self) -> str:
        return "DEFERRED"

    __repr__ = __str__


DEFERRED = _DeferredPath.DEFERRED
"""An input whose path is not known yet.

Passed in place of a path -- ``lib.AddItem(DEFERRED, "ns::type")`` -- and that
is the whole caller-facing surface: a constant, with nothing to name or invent,
because a caller who had a value to pass would not be deferring. Such a row
plans normally and is refused at stage.

The value that gets *stored* is not this constant but a distinct path minted by
:func:`mint_deferred_path` on receipt, because the manifest is a dict keyed by
path and identity derives from path -- one shared value would collapse two
deferred rows onto one entry and then raise `already added` on the second.
"""


class DeferredPathError(Exception):
    pass
def mint_deferred_path() -> Path:
    return DEFERRED_ROOT / uuid.uuid4().hex


def is_deferred(path) -> bool:
    if path is DEFERRED:
        return True
    if not isinstance(path, (str, Path)):
        return False
    p = Path(path)
    return p.is_absolute() and p != DEFERRED_ROOT and p.is_relative_to(DEFERRED_ROOT)


RenderDialect = Literal["bash", "brace", "groovy"]
"""Token-substitution dialect for :meth:`PathMap.Render`.

* ``bash``   — shell variable form, ``$AGENT_HOME``.
* ``brace``  — Python format-string form, ``{agent_home}``.
* ``groovy`` — Nextflow params form, ``${params.home}``.
"""

_DIALECT_TOKENS: dict[RenderDialect, str] = {
    "bash": "$AGENT_HOME",
    "brace": "{agent_home}",
    "groovy": "${params.home}",
}


def reroot_in_text(content: str, old_root: Path | str, new_root: Path | str) -> str:
    pattern = r"(?<![\w./])" + re.escape(str(old_root)) + r"(?=/|\s|$|[\"'])"
    return re.sub(pattern, str(new_root), content)


_PHANTOM_GUARD_SENTINEL = "# msm-phantom-guard"
_ARRAY_CHILD_INVOCATION = re.compile(
    r"^([ \t]*)(bash \$\{?nxf_array_task_dir\}?/\.command\.run\b.*)$",
    re.MULTILINE,
)


def inject_array_child_guard(content: str) -> str:
    if _PHANTOM_GUARD_SENTINEL in content:
        return content
    m = _ARRAY_CHILD_INVOCATION.search(content)
    if m is None:
        return content
    indent = m.group(1)
    guard = (
        f"{indent}{_PHANTOM_GUARD_SENTINEL}\n"
        f"{indent}msm_phantom_guard() {{\n"
        f"{indent}    __msm_rc=$?\n"
        f'{indent}    [ -e "$nxf_array_task_dir/.command.begin" ] || : > "$nxf_array_task_dir/.command.begin"\n'
        f'{indent}    [ -e "$nxf_array_task_dir/.exitcode" ] || echo "${{__msm_rc:-126}}" > "$nxf_array_task_dir/.exitcode"\n'
        f"{indent}}}\n"
        f"{indent}trap msm_phantom_guard EXIT\n"
    )
    return content[:m.start()] + guard + content[m.start():]


def _strip_dotdot(p: Path) -> Path:
    parts: list[str] = []
    for seg in p.parts:
        if seg == "..":
            if not parts or parts[-1] in ("/", ""):
                raise ValueError(f"path walks above its root: {p}")
            parts.pop()
        else:
            parts.append(seg)
    return Path(*parts) if parts else Path(".")


@dataclass(frozen=True)
class ContextPath:
    local: Path
    external: Path
    container: Path

    def __post_init__(self) -> None:
        for name, p in (("local", self.local), ("external", self.external), ("container", self.container)):
            if not isinstance(p, Path):
                raise TypeError(f"ContextPath.{name} must be a Path, got {type(p).__name__}: {p!r}")
            if not p.is_absolute():
                raise ValueError(f"ContextPath.{name} must be absolute: {p}")
            if ".." in p.parts:
                raise ValueError(f"ContextPath.{name} must not contain '..': {p}")


    @classmethod
    def FromLocal(
        cls,
        local: Path,
        path_map: "PathMap",
        *,
        container: Path | None = None,
    ) -> "ContextPath":
        local = _strip_dotdot(Path(local))
        external = path_map.LocalToExternal(local)
        container = container if container is not None else local
        return cls(local=local, external=external, container=_strip_dotdot(Path(container)))

    @classmethod
    def FromExternal(
        cls,
        external: Path,
        path_map: "PathMap",
        *,
        container: Path | None = None,
    ) -> "ContextPath":
        external = _strip_dotdot(Path(external))
        local = path_map.ExternalToLocal(external)
        container = container if container is not None else local
        return cls(local=local, external=external, container=_strip_dotdot(Path(container)))

    @classmethod
    def ForOutput(cls, name: str, path_map: "PathMap") -> "ContextPath":
        if "/" in name or name in ("", ".", ".."):
            raise ValueError(f"ForOutput expects a bare filename, got: {name!r}")
        external_base = path_map.extern_cwd if path_map.extern_cwd is not None else path_map.extern_work
        external = external_base / name
        if path_map.host_local:
            return cls(local=external, external=external, container=external)
        container = AgentPaths.WORK_ROOT / name
        return cls(local=container, external=external, container=container)


@dataclass
class PathMap:
    extern_home: Path
    task_key: str
    extern_cwd: Path | None = None
    host_local: bool = False
    extern_work: Path = field(init=False)

    def __post_init__(self) -> None:
        self.extern_home = _strip_dotdot(Path(self.extern_home))
        if not self.extern_home.is_absolute():
            raise ValueError(f"extern_home must be absolute: {self.extern_home}")
        if not self.task_key or "/" in self.task_key:
            raise ValueError(f"task_key must be a bare key, got: {self.task_key!r}")
        self.extern_work = self.extern_home / AgentPaths.STAGED / self.task_key
        if self.extern_cwd is not None:
            self.extern_cwd = _strip_dotdot(Path(self.extern_cwd))
            if not self.extern_cwd.is_absolute():
                raise ValueError(f"extern_cwd must be absolute: {self.extern_cwd}")


    @classmethod
    def FromAgent(cls, agent, task_key: str) -> "PathMap":
        return cls(extern_home=Path(str(agent.home.GetPath())), task_key=task_key)

    @classmethod
    def FromExternalCwd(cls, cwd: Path, agent) -> "PathMap":
        cwd = _strip_dotdot(Path(cwd))
        extern_home = Path(str(agent.home.GetPath()))
        staged_root = extern_home / AgentPaths.STAGED
        if not cwd.is_relative_to(staged_root):
            raise ValueError(
                f"cwd [{cwd}] is not under agent's STAGED root [{staged_root}]; "
                "FromExternalCwd cannot identify the task key"
            )
        rel = cwd.relative_to(staged_root)
        if not rel.parts:
            raise ValueError(f"cwd [{cwd}] is the STAGED root itself; no task key")
        task_key = rel.parts[0]
        return cls(extern_home=extern_home, task_key=task_key)


    def LocalToExternal(self, p: Path) -> Path:
        p = _strip_dotdot(Path(p))
        if not p.is_absolute():
            raise ValueError(f"LocalToExternal expects absolute, got: {p}")
        if p.is_relative_to(AgentPaths.HOME_ROOT):
            return self.extern_home / p.relative_to(AgentPaths.HOME_ROOT)
        if p.is_relative_to(AgentPaths.WORK_ROOT):
            return self.extern_work / p.relative_to(AgentPaths.WORK_ROOT)
        return p

    def ExternalToLocal(self, p: Path) -> Path:
        p = _strip_dotdot(Path(p))
        if not p.is_absolute():
            raise ValueError(f"ExternalToLocal expects absolute, got: {p}")
        if p.is_relative_to(self.extern_home):
            return AgentPaths.HOME_ROOT / p.relative_to(self.extern_home)
        return p

    def LocalToContainer(self, p: Path) -> Path:
        p = _strip_dotdot(Path(p))
        if not p.is_absolute():
            raise ValueError(f"LocalToContainer expects absolute, got: {p}")
        return p

    def ContainerToLocal(self, p: Path) -> Path:
        p = _strip_dotdot(Path(p))
        if not p.is_absolute():
            raise ValueError(f"ContainerToLocal expects absolute, got: {p}")
        if p.is_relative_to(AgentPaths.WORK_ROOT):
            tail = p.relative_to(AgentPaths.WORK_ROOT)
            return AgentPaths.HOME_ROOT / AgentPaths.STAGED / self.task_key / tail
        return p


    def Parse(self, p: Path, *, container_override: Path | None = None) -> ContextPath:
        p = Path(p)

        if p.is_absolute() and p.is_relative_to(AgentPaths.WORK_ROOT):
            tail = p.relative_to(AgentPaths.WORK_ROOT)
            local = AgentPaths.HOME_ROOT / AgentPaths.STAGED / self.task_key / tail
            external = self.extern_work / tail
            container = container_override if container_override is not None else local
            return ContextPath(local=local, external=external, container=_strip_dotdot(Path(container)))

        ws_marker = AgentPaths.WORK_ROOT.name
        if (
            not p.is_absolute()
            and len(p.parts) >= 2
            and p.parts[0] == ".."
            and p.parts[1] == ws_marker
        ):
            tail = Path(*p.parts[2:]) if len(p.parts) > 2 else Path(".")
            local = AgentPaths.HOME_ROOT / AgentPaths.STAGED / self.task_key
            external = self.extern_work
            if str(tail) != ".":
                local = local / tail
                external = external / tail
            container = container_override if container_override is not None else local
            return ContextPath(local=local, external=external, container=_strip_dotdot(Path(container)))

        if p.is_symlink():
            if not p.is_absolute():
                raise ValueError(f"Parse received non-absolute symlink: {p!r}")
            link_target = Path(str(p.readlink()))
            if link_target.is_absolute() and link_target.is_relative_to(AgentPaths.HOME_ROOT):
                tail = link_target.relative_to(AgentPaths.HOME_ROOT)
                external = self.extern_home / tail
                local = AgentPaths.HOME_ROOT / tail
                container = container_override if container_override is not None else local
                return ContextPath(local=local, external=external, container=_strip_dotdot(Path(container)))
            external = link_target
            container = container_override if container_override is not None else external
            return ContextPath(
                local=link_target,
                external=link_target,
                container=_strip_dotdot(Path(container)),
            )

        if not p.is_absolute():
            raise ValueError(
                f"Parse received a non-absolute, non-symlink, non-../ws path: {p!r}. "
                f"This is the ad-hoc-concat case the overhaul forbids; the caller "
                f"should either anchor the path or pass a symlink / absolute form."
            )

        if p.is_relative_to(AgentPaths.HOME_ROOT):
            tail = p.relative_to(AgentPaths.HOME_ROOT)
            container = container_override if container_override is not None else p
            return ContextPath(
                local=p,
                external=self.extern_home / tail,
                container=_strip_dotdot(Path(container)),
            )

        container = container_override if container_override is not None else p
        return ContextPath(local=p, external=p, container=_strip_dotdot(Path(container)))


    def Render(self, p: Path | str, dialect: RenderDialect) -> str:
        token = _DIALECT_TOKENS[dialect]
        p_path = Path(p) if not isinstance(p, Path) else p
        if p_path.is_absolute() and p_path.is_relative_to(self.extern_home):
            tail = p_path.relative_to(self.extern_home)
            if str(tail) in (".", ""):
                return token
            return f"{token}/{tail}"
        return str(p)
