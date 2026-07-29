"""Path translation for metasmith.

Metasmith runs jobs across three filesystem views:

  * ``local``      — the bootstrap container's view (mounted at
                     ``AgentPaths.HOME_ROOT`` = ``/msm_home``).
  * ``external``   — the host filesystem, rooted at the agent's
                     ``extern_home`` (e.g. ``/scratch/agent``).
  * ``container``  — the per-step task container's view (workdir
                     bound at ``AgentPaths.WORK_ROOT`` = ``/ws``).

This module centralises every conversion between those three views.
Two classes:

  * :class:`PathMap`     — per-execution context. Built once per
                            ``ExecuteStep`` / ``sbatch`` entry; carries
                            ``extern_home``, ``extern_work``, and
                            ``task_key``.
  * :class:`ContextPath` — value type. Enforces that the three views
                            are absolute, ``..``-free, and mutually
                            consistent.

The intentional design property: **invariants raise**, they do not
silently normalise. Every historical bug in this surface stemmed from
"be forgiving" — relative ``../ws/...`` shapes from apptainer slipping
through ``str.replace`` rewrites with no validation. Raising at the
boundary forces callers to handle the cases explicitly.

Empirically known stringification shapes for upstream process outputs
on a Docker workdir bound at ``/ws`` vs an apptainer workdir bound at
``/ws``:

  * Docker:    ``/ws/work/<hash>/<file>``    (absolute, ``..``-free)
  * Apptainer: ``../ws/work/<hash>/<file>``  (relative-to-pwd)

The Docker shape is exercised by
``tests/integration/test_e2e_orchestrator.py::TestPathStringification``.
The apptainer shape is reproduced as a unit test in
``tests/path_overhaul/test_parse_path_apptainer_relative.py``.

This module also owns :data:`DEFERRED`, the fourth thing a path can be: not
known yet. It lives here rather than beside the code that mints identity from
it because every path consumer has to be able to ask what kind of path it is
holding, and this is the module they already import.
"""
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
    """The type behind :data:`DEFERRED`; one member, which is the constant."""

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
    """A deferred path reached something that needs a real file."""


def mint_deferred_path() -> Path:
    """A fresh, distinct, absolute stand-in path under :data:`DEFERRED_ROOT`.

    Minted once, at `AddItem` time, and persisted in the manifest thereafter --
    never re-derived on load. Identity is a function of path, so regenerating
    would give the same workflow a different task key every time it was opened.
    """
    return DEFERRED_ROOT / uuid.uuid4().hex


def is_deferred(path) -> bool:
    """Whether `path` is the constant or a path minted from it.

    Accepts the rendered string form too, so the check survives the round trip
    through yaml that every manifest and every spec makes.
    """
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
    """Replace ``old_root`` with ``new_root`` everywhere it appears as a
    PATH PREFIX inside ``content``.

    A match counts as a prefix only when BOTH boundaries are satisfied:

      * The character immediately before ``old_root`` is not a word
        character, ``.``, or ``/`` (so inner occurrences like
        ``/data/msm_home_old_backup`` are not matched at their inner
        ``/msm_home`` substring, and tokens like ``/wsadm/ws/...`` are
        not matched at their inner ``/ws`` substring).
      * The character immediately after ``old_root`` is ``/``,
        whitespace, end-of-string, or a quote (so a prefix like ``/ws``
        does not match the standalone token ``/wsadm``).

    Used by ``bin/sbatch.fix_paths`` to rewrite the path roots inside a
    Nextflow-generated ``.command.run`` script. For single-path
    translations (one ``Path``, not a multi-line text blob), prefer
    :meth:`PathMap.LocalToExternal` / :meth:`PathMap.Render`.
    """
    pattern = r"(?<![\w./])" + re.escape(str(old_root)) + r"(?=/|\s|$|[\"'])"
    return re.sub(pattern, str(new_root), content)


_PHANTOM_GUARD_SENTINEL = "# msm-phantom-guard"
_ARRAY_CHILD_INVOCATION = re.compile(
    r"^([ \t]*)(bash \$\{?nxf_array_task_dir\}?/\.command\.run\b.*)$",
    re.MULTILINE,
)


def inject_array_child_guard(content: str) -> str:
    """Ensure every SLURM array child leaves ``.command.begin`` + ``.exitcode``.

    Nextflow moves an array child ``SUBMITTED -> RUNNING`` only when it sees the
    child's ``.command.begin``, then reads ``.exitcode`` to finalize it. There
    is **no fallback start-timeout for array children**, so a child that dies at
    the container/exec layer (SLURM exit 126) *before* ``.command.run`` writes
    ``.command.begin`` leaves neither file -> Nextflow waits on it forever and
    the whole run hangs ~1h from done. This injects an EXIT-trap guard into the
    array dispatcher (``.command.sh``) that, after the child returns,
    force-creates ``.command.begin`` and ``.exitcode`` (with the child's
    captured exit code) ONLY if the child did not already write them -- so a
    normal success or late (in-container) failure is never clobbered, matching
    the proven manual recovery (``: > .command.begin; echo 126 > .exitcode``).

    An EXIT trap (installed *before* the child invocation) is required because
    the dispatcher runs under ``set -e`` (shebang ``#!/bin/bash -ue``): a
    failing ``bash .command.run`` exits the script before any *following* line
    could run, but the EXIT trap still fires and still sees the child's ``$?``.

    Idempotent (sentinel-guarded) and a no-op for non-array scripts (those
    without the ``bash $nxf_array_task_dir/.command.run`` invocation line).
    """
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
    """Pure-string ``..`` collapse, no FS access.

    Walks the path's parts left-to-right; each ``..`` pops the last
    accumulated part. Leading absolute marker is preserved.
    """
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
    """A path observed from all three views simultaneously.

    Invariants (enforced in ``__post_init__``):

      * ``local``, ``external``, ``container`` are all absolute.
      * None contain ``..`` segments.
      * If ``local`` is under :data:`AgentPaths.HOME_ROOT`, the
        path-map's external prefix must match
        (verified at construction via :meth:`FromLocal` /
        :meth:`FromExternal`; the bare dataclass form does not have
        a path-map handle so it only checks shape).
      * If no explicit ``container`` override, ``container == local``.

    Violations raise :class:`ValueError`. The dataclass is frozen to
    keep the invariants intact across the value's lifetime.
    """

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

    # -- classmethods ------------------------------------------------------

    @classmethod
    def FromLocal(
        cls,
        local: Path,
        path_map: "PathMap",
        *,
        container: Path | None = None,
    ) -> "ContextPath":
        """Build from a ``local`` view; derive ``external`` from the path map."""
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
        """Build from an ``external`` view; derive ``local`` from the path map."""
        external = _strip_dotdot(Path(external))
        local = path_map.ExternalToLocal(external)
        container = container if container is not None else local
        return cls(local=local, external=external, container=_strip_dotdot(Path(container)))

    @classmethod
    def ForOutput(cls, name: str, path_map: "PathMap") -> "ContextPath":
        """Build a path for a freshly-produced output file in the
        bootstrap container's cwd (which is bound to ``/ws``).

        Both the bootstrap container and the inner task container see
        the output at ``/ws/<name>`` via their respective workdir
        binds — ``local`` and ``container`` are identical. The
        ``external`` view is the host filesystem location, which is
        ``path_map.extern_cwd`` (the per-step nxf_work dir) when set,
        or ``path_map.extern_work`` (the task workspace) when not.

        With no container boundary (``path_map.host_local``) all three
        views collapse onto the host path: nothing is bound at ``/ws``,
        so a protocol interpolating ``out.container`` would otherwise
        hand the tool a directory that does not exist.
        """
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
    """Per-execution mapping between the three path views.

    Carries the roots needed to reroot a path from one view to another:

      * ``extern_home`` — the host-side absolute path that the agent
        binds at :data:`AgentPaths.HOME_ROOT` (``/msm_home``).
      * ``task_key`` — the canonical run key used in path layouts.
      * ``extern_cwd`` (optional) — the host-side absolute path of the
        step's nxf_work directory (deeper than ``extern_work``). When
        set, :meth:`ForOutput` uses it as the external prefix so a
        bare-filename output resolves to the correct per-step location
        on disk. When unset, ``ForOutput`` falls back to ``extern_work``.
      * ``extern_work`` (derived) — the host-side absolute path of the
        current task's workspace (``extern_home / runs / <task_key>``),
        which is bound at :data:`AgentPaths.WORK_ROOT` (``/ws``).

    Construction goes through :meth:`FromAgent` (the bootstrap entry
    point) or :meth:`FromExternalCwd` (the sbatch entry point); the
    bare ``__init__`` form is fine for tests.
    """

    extern_home: Path
    task_key: str
    extern_cwd: Path | None = None
    # True when this execution crosses NO container boundary, so cwd is a plain
    # host directory and nothing is bound at /ws or /msm_home. Two paths reach
    # it: direct-run (never containerized), and a nextflow run on a mamba/native
    # agent (Environment.needs_relay is False). It is emphatically not a
    # direct-run-only flag -- reading it as one is how the mamba path came to
    # hand tools unwritable /ws paths in the first place.
    #
    # ForOutput's views depend on it: with a boundary the bootstrap runs
    # containerized with cwd bound to /ws, so local==container==/ws/<name>;
    # without one all three views are the host path, or every
    # `output.local.exists()` success check reads False for a step that in fact
    # succeeded and `out.container` names a directory that does not exist.
    # Default False keeps the containerized nextflow path byte-identical.
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

    # -- factories ---------------------------------------------------------

    @classmethod
    def FromAgent(cls, agent, task_key: str) -> "PathMap":
        """Build from an :class:`Agent` plus the task key."""
        return cls(extern_home=Path(str(agent.home.GetPath())), task_key=task_key)

    @classmethod
    def FromExternalCwd(cls, cwd: Path, agent) -> "PathMap":
        """Build from a host-side cwd inside the agent's STAGED tree.

        Used by ``bin/sbatch`` when a bounce script invokes
        ``sbatch ...`` from inside a Nextflow work directory. The cwd
        must contain the run key as a path segment under the agent's
        ``runs/`` directory, e.g.::

            <extern_home>/runs/<task_key>/nxf_work/<hash>/...

        The run key is identified by walking up from cwd to the
        agent's STAGED dir (``<extern_home>/runs``) rather than by
        regex matching of the segment shape — this avoids the
        ``r"/\\w*/nxf_work/.*"`` bug where ``\\w*`` silently picks a
        sample dir named ``ws`` or any other segment between the run
        key and ``nxf_work/``.
        """
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

    # -- view conversions --------------------------------------------------

    def LocalToExternal(self, p: Path) -> Path:
        """Map ``p`` from the local (HOME_ROOT) view to the host
        (extern_home) view. Foreign-absolute paths pass through unchanged.
        """
        p = _strip_dotdot(Path(p))
        if not p.is_absolute():
            raise ValueError(f"LocalToExternal expects absolute, got: {p}")
        if p.is_relative_to(AgentPaths.HOME_ROOT):
            return self.extern_home / p.relative_to(AgentPaths.HOME_ROOT)
        if p.is_relative_to(AgentPaths.WORK_ROOT):
            return self.extern_work / p.relative_to(AgentPaths.WORK_ROOT)
        return p  # foreign-absolute (e.g. /project/...) — identity

    def ExternalToLocal(self, p: Path) -> Path:
        """Inverse of :meth:`LocalToExternal`."""
        p = _strip_dotdot(Path(p))
        if not p.is_absolute():
            raise ValueError(f"ExternalToLocal expects absolute, got: {p}")
        if p.is_relative_to(self.extern_home):
            return AgentPaths.HOME_ROOT / p.relative_to(self.extern_home)
        return p

    def LocalToContainer(self, p: Path) -> Path:
        """A local-view path under HOME_ROOT, observed through the
        per-step container's ``/ws`` workdir bind, is the same path
        (the container sees its own workdir as ``/ws`` but reads
        HOME_ROOT-rooted paths verbatim).
        """
        p = _strip_dotdot(Path(p))
        if not p.is_absolute():
            raise ValueError(f"LocalToContainer expects absolute, got: {p}")
        return p

    def ContainerToLocal(self, p: Path) -> Path:
        """Inverse of :meth:`LocalToContainer`. A container-view ``/ws``
        path maps to ``HOME_ROOT/runs/<task_key>/...``.
        """
        p = _strip_dotdot(Path(p))
        if not p.is_absolute():
            raise ValueError(f"ContainerToLocal expects absolute, got: {p}")
        if p.is_relative_to(AgentPaths.WORK_ROOT):
            tail = p.relative_to(AgentPaths.WORK_ROOT)
            return AgentPaths.HOME_ROOT / AgentPaths.STAGED / self.task_key / tail
        return p

    # -- the consolidator --------------------------------------------------

    def Parse(self, p: Path, *, container_override: Path | None = None) -> ContextPath:
        """Resolve a FILES-entry path into a (local, external, container) view.

        Handles four input shapes:

          1. ``/ws/...`` absolute — the Docker stringification of an
             upstream process output. Rerouted to
             ``HOME_ROOT/runs/<task_key>/<tail>`` on the local view.
          2. ``../ws/...`` relative — the apptainer-local stringification
             of the same shape. Anchored against the agent's STAGED
             root, normalised, then routed identically to (1).
          3. Symlink whose target sits under HOME_ROOT. Rerouted via
             :meth:`LocalToExternal`; local view stays under HOME_ROOT.
          4. Symlink whose target sits outside HOME_ROOT (e.g. a
             ``/project/...`` reference DB). Container view uses the
             absolute target path so :class:`ExecutionContext.GetContainerModel`
             emits an identity bind instead of mapping the source to
             ``/ws``.

        Anything else falls through with the absolute-input assertion.
        """
        p = Path(p)

        # (1) /ws/<tail> — Docker absolute.
        if p.is_absolute() and p.is_relative_to(AgentPaths.WORK_ROOT):
            tail = p.relative_to(AgentPaths.WORK_ROOT)
            local = AgentPaths.HOME_ROOT / AgentPaths.STAGED / self.task_key / tail
            external = self.extern_work / tail
            container = container_override if container_override is not None else local
            return ContextPath(local=local, external=external, container=_strip_dotdot(Path(container)))

        # (2) ../ws/<tail> — apptainer-local relative form.
        #
        # Apptainer-local Nextflow emits upstream-process outputs as
        # ``Path("../ws/work/<hash>/<file>")`` rather than Docker's
        # absolute ``/ws/<tail>``. The two forms denote the same logical
        # file: an entry inside the consumer's task workspace under the
        # ``work/`` subdirectory. ``../ws/<tail>`` is semantically the
        # apptainer transcription of ``/ws/<tail>``, not a literal
        # "walk up one then into ws/" — naively collapsing ``..`` against
        # the consumer's cwd resolves to the wrong segment (it pops the
        # run key, leaving ``runs/ws/...``).
        #
        # Route it through the same canonicalisation as case-1.
        ws_marker = AgentPaths.WORK_ROOT.name  # "ws"
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

        # (3) and (4): symlinks.
        if p.is_symlink():
            if not p.is_absolute():
                raise ValueError(f"Parse received non-absolute symlink: {p!r}")
            link_target = Path(str(p.readlink()))
            # (3) HOME_ROOT-rooted target: reroute to extern_home.
            if link_target.is_absolute() and link_target.is_relative_to(AgentPaths.HOME_ROOT):
                tail = link_target.relative_to(AgentPaths.HOME_ROOT)
                external = self.extern_home / tail
                local = AgentPaths.HOME_ROOT / tail
                container = container_override if container_override is not None else local
                return ContextPath(local=local, external=external, container=_strip_dotdot(Path(container)))
            # (4) Foreign-target symlink — identity bind. The container
            # view defaults to the external absolute target so
            # ``ExecutionContext.GetContainerModel`` emits an identity
            # bind instead of mapping to /ws. The local view is the
            # foreign path; the bootstrap container reads through the
            # symlink either way, so we anchor on the absolute target.
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

        # (5) Absolute path under HOME_ROOT (the agent home, container-bound at
        # /msm_home): a canonical upstream output referenced by its home-view
        # path -- e.g. a reference DB produced by a sibling process and consumed
        # by a transform that binds it explicitly (diamond's UniRef50 db -> /db).
        # Its host path is `extern_home/<tail>`. Without this branch the identity
        # fallthrough below leaks the container-only `/msm_home` prefix into
        # apptainer --bind SOURCES, and the mount fails on the host with
        # "mount source ... doesn't exist".
        if p.is_relative_to(AgentPaths.HOME_ROOT):
            tail = p.relative_to(AgentPaths.HOME_ROOT)
            container = container_override if container_override is not None else p
            return ContextPath(
                local=p,
                external=self.extern_home / tail,
                container=_strip_dotdot(Path(container)),
            )

        # Fallthrough: absolute paths under neither WORK_ROOT nor HOME_ROOT
        # (foreign references, e.g. /project/refdb -- identity bind).
        container = container_override if container_override is not None else p
        return ContextPath(local=p, external=p, container=_strip_dotdot(Path(container)))

    # -- token rendering ---------------------------------------------------

    def Render(self, p: Path | str, dialect: RenderDialect) -> str:
        """Render an absolute host path as a placeholder-prefixed string.

        If ``p`` is under ``extern_home``, the host prefix is replaced
        with the dialect's placeholder; otherwise ``str(p)`` is
        returned unchanged.

        Replaces the scattered ``str.replace(extern_home, "${params.home}")``
        and friends at ``workflow.py:1183``, ``bootstrap.py:124``, and
        ``agents.py:256``. Unlike ``str.replace``, only the prefix is
        rewritten — inner occurrences of ``str(extern_home)`` are left
        intact.
        """
        token = _DIALECT_TOKENS[dialect]
        p_path = Path(p) if not isinstance(p, Path) else p
        if p_path.is_absolute() and p_path.is_relative_to(self.extern_home):
            tail = p_path.relative_to(self.extern_home)
            if str(tail) in (".", ""):
                return token
            return f"{token}/{tail}"
        return str(p)
