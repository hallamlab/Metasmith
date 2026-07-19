"""Bubblewrap (``bwrap``) filesystem jail for the per-cell agent invocation.

The token benchmark runs a real coding agent (``claude``) with
``--permission-mode=bypassPermissions`` — the agent's shell tool can run any
command. The redirection-only isolation in :mod:`.sandbox` (private ``HOME``,
spoofed ``.condarc``) keeps the agent's *conda/apptainer state* inside the
sandbox but does **not** stop the agent from *reading* host paths. This module
adds a real filesystem jail on top: a ``bwrap`` namespace that

  * binds the sandbox root read-write at its real absolute path (so apptainer
    bind-mounts inside the jail resolve to identical paths — no remapping);
  * binds the core system trees read-only (``/usr`` ``/bin`` ``/lib*`` ``/etc``
    …) so tools resolve but the host ``$HOME`` / project trees are invisible;
  * binds a small allow-list of extra read-only paths the run genuinely needs
    from outside the sandbox (the bootstrap conda env, the materials root, the
    apptainer install prefix);
  * binds the host ``~/.claude`` directory **read-write and live** at the
    in-jail HOME so every concurrent cell shares the one rotating credential
    file (this is the user's shared-subscription-login requirement — a single
    literal auth file, not per-cell copies).

Design constraints that shaped this (see plan T1):

  * **Do not ``--unshare-user`` by default.** apptainer sets up its own user
    namespace; a user-unsharing bwrap breaks the nested runtime with a userns /
    setgroups error. We unshare mount/PID/IPC/UTS (the filesystem jail + a
    private process table) but leave the user namespace to apptainer. The
    ``unshare_user`` knob exists so the micb0 spike can measure whether a
    user-unshared jail composes with apptainer on that host; the default stays
    off.
  * **Keep the network.** The agent must reach the Anthropic API and the public
    conda channels, so we never ``--unshare-net``.
  * **A bwrap jail composes with apptainer, not with a docker daemon** (the
    daemon resolves ``-v`` paths in the host mount namespace, not the jail). So
    the jail is applied on the apptainer hosts (micb0 / chamois — the actual
    sweep); the Cosmos docker dev box keeps redirection-only isolation.

The argv builder (:func:`build_bwrap_argv`) is a pure function — unit-tested
without ``bwrap`` installed. :func:`wrap` assembles the concrete bind set from a
:class:`~tests.e2e_agentic.harness.sandbox.SandboxLayout` plus the agent env.
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path


# System trees every jail binds read-only so ordinary tools resolve. Optional
# ones (``-try``) tolerate a host that lacks them.
_SYSTEM_RO = (
    ("/usr", False),
    ("/bin", True),
    ("/sbin", True),
    ("/lib", True),
    ("/lib64", True),
    ("/lib32", True),
    ("/etc", False),
    ("/opt", True),
    ("/var/lib", True),      # apptainer / squashfuse state on some hosts
    ("/run/systemd/resolve", True),  # DNS on systemd hosts (net stays up)
)


@dataclass(frozen=True)
class BindSpec:
    """One bwrap bind. ``mode`` ∈ {ro, rw, dev}; ``optional`` uses the ``-try``
    variant so a missing source is skipped rather than fatal."""
    src: str
    dst: str
    mode: str = "ro"
    optional: bool = False

    def to_argv(self) -> list[str]:
        flag = {"ro": "--ro-bind", "rw": "--bind", "dev": "--dev-bind"}[self.mode]
        if self.optional:
            flag += "-try"
        return [flag, self.src, self.dst]


# ---------------------------------------------------------------------------
# availability / enablement
# ---------------------------------------------------------------------------


def resolve_bwrap() -> str | None:
    """Locate the ``bwrap`` binary.

    ``MSM_E2E_BWRAP`` (an explicit path) wins so a host without a system bwrap
    can point at a shipped static binary (e.g. ``~/token-benchmark/bin/bwrap``
    on micb0). Otherwise fall back to ``bwrap`` on PATH.
    """
    explicit = os.environ.get("MSM_E2E_BWRAP")
    if explicit:
        p = Path(explicit)
        return str(p) if p.exists() else None
    found = shutil.which("bwrap")
    return found


def jail_enabled(env: dict[str, str] | None = None) -> bool:
    """Decide whether to jail this invocation.

    ``MSM_E2E_JAIL`` forces the decision when set: ``0``/``false``/``off`` →
    never jail (even if bwrap is present); ``1``/``true``/``on`` → jail (and a
    missing bwrap becomes a hard error at wrap time, not a silent bypass).

    Default (unset): jail iff ``bwrap`` is available AND the runtime is
    APPTAINER (read from ``MSM_E2E_RUNTIME`` in the agent env). Docker dev keeps
    redirection-only isolation because a bwrap jail does not compose with the
    docker daemon.
    """
    raw = os.environ.get("MSM_E2E_JAIL")
    if raw is not None:
        val = raw.strip().lower()
        if val in ("0", "false", "off", "no"):
            return False
        if val in ("1", "true", "on", "yes"):
            return True
    if resolve_bwrap() is None:
        return False
    runtime = ((env or {}).get("MSM_E2E_RUNTIME") or "").upper()
    return runtime == "APPTAINER"


def require_bwrap() -> str:
    """Return the bwrap path or raise (used when jailing is forced on)."""
    b = resolve_bwrap()
    if b is None:
        raise RuntimeError(
            "MSM_E2E_JAIL requests a bwrap jail but no bwrap binary was found "
            "(set MSM_E2E_BWRAP=/path/to/bwrap or install bwrap on PATH)."
        )
    return b


# ---------------------------------------------------------------------------
# pure argv builder
# ---------------------------------------------------------------------------


def build_bwrap_argv(
    inner_argv: list[str],
    *,
    binds: list[BindSpec],
    bwrap: str = "bwrap",
    unshare_user: bool = False,
    chdir: str | None = None,
    tmp_bind: BindSpec | None = None,
) -> list[str]:
    """Assemble a ``bwrap`` argv wrapping ``inner_argv``.

    Namespaces: mount + PID + IPC + UTS + cgroup are unshared (filesystem jail +
    private process table). The network is NOT unshared (the agent needs the
    Anthropic API + conda channels). The USER namespace is unshared only when
    ``unshare_user`` is true — off by default so a nested apptainer owns it.

    ``binds`` are applied in order (later binds override earlier — this is how
    the shared-auth ``~/.claude`` bind lands on top of the sandbox-root bind).
    ``tmp_bind`` supplies ``/tmp`` (a sandbox-local rw dir rather than a
    RAM-backed tmpfs, so a large apptainer scratch does not exhaust memory).
    """
    argv = [bwrap, "--die-with-parent", "--unshare-pid", "--unshare-ipc",
            "--unshare-uts", "--unshare-cgroup-try"]
    if unshare_user:
        argv.append("--unshare-user-try")
    argv += ["--proc", "/proc", "--dev", "/dev"]
    if tmp_bind is not None:
        argv += tmp_bind.to_argv()
    else:
        argv += ["--tmpfs", "/tmp"]
    for b in binds:
        argv += b.to_argv()
    if chdir is not None:
        argv += ["--chdir", chdir]
    argv += ["--"]
    argv += inner_argv
    return argv


# ---------------------------------------------------------------------------
# layout → bind set
# ---------------------------------------------------------------------------


_CLAUDE_DIR_REL = ".claude"


def default_binds(
    sandbox_root: Path,
    home: Path,
    *,
    extra_ro: list[Path] | None = None,
    share_claude_auth: bool = True,
    claude_bin: str | None = None,
) -> list[BindSpec]:
    """Compute the standard jail bind set.

    Order matters: system RO first, then the extra allow-list, then the sandbox
    root RW (so anything inside the sandbox is writable and wins over a system
    RO parent), then the shared ``~/.claude`` auth bind on top of the sandbox
    home.
    """
    binds: list[BindSpec] = [
        BindSpec(src, src, "ro", optional) for src, optional in _SYSTEM_RO
    ]
    # Extra read-only allow-list (bootstrap conda env, materials root, apptainer
    # install prefix, host claude install dir). De-duplicated, existing only.
    seen: set[str] = set()
    for p in (extra_ro or []):
        s = str(p)
        if s and s not in seen:
            seen.add(s)
            binds.append(BindSpec(s, s, "ro", optional=True))
    if claude_bin:
        # bind the directory holding the claude launcher so it + its siblings
        # (node shims, etc.) resolve.
        bindir = str(Path(claude_bin).resolve().parent)
        if bindir not in seen:
            seen.add(bindir)
            binds.append(BindSpec(bindir, bindir, "ro", optional=True))
    # The sandbox itself, read-write, at its real path.
    binds.append(BindSpec(str(sandbox_root), str(sandbox_root), "rw"))
    # Shared, live credentials: bind the host ~/.claude DIRECTORY (not just the
    # single file) rw onto the in-jail HOME, so token rotation that rewrites the
    # file by rename+replace survives, and every concurrent cell reads/writes the
    # one login. Also bind ~/.claude.json (top-level config) when present.
    if share_claude_auth:
        host_claude = Path.home() / _CLAUDE_DIR_REL
        if host_claude.exists():
            binds.append(BindSpec(str(host_claude),
                                  str(home / _CLAUDE_DIR_REL), "rw"))
        host_claude_json = Path.home() / ".claude.json"
        if host_claude_json.exists():
            binds.append(BindSpec(str(host_claude_json),
                                  str(home / ".claude.json"), "rw", optional=True))
    return binds


def wrap(
    inner_argv: list[str],
    *,
    sandbox: Path,
    home: Path,
    env: dict[str, str],
    extra_ro: list[Path] | None = None,
    claude_bin: str | None = None,
    chdir: Path | None = None,
    unshare_user: bool = False,
) -> list[str]:
    """Wrap ``inner_argv`` in a bwrap jail assembled from the sandbox layout.

    Raises if bwrap cannot be resolved (callers gate on :func:`jail_enabled`
    first; a forced ``MSM_E2E_JAIL=1`` with no bwrap is a hard error, not a
    silent bypass).
    """
    bwrap = require_bwrap()
    sandbox = sandbox.resolve()
    home = home.resolve()
    # /tmp → a sandbox-local dir so apptainer scratch is real disk + isolated.
    tmp_dir = sandbox / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_bind = BindSpec(str(tmp_dir), "/tmp", "rw")

    binds = default_binds(
        sandbox, home,
        extra_ro=extra_ro,
        claude_bin=claude_bin,
    )
    return build_bwrap_argv(
        inner_argv,
        binds=binds,
        bwrap=bwrap,
        unshare_user=unshare_user,
        chdir=str(chdir) if chdir is not None else str(sandbox),
        tmp_bind=tmp_bind,
    )


def extra_ro_from_env(env: dict[str, str]) -> list[Path]:
    """Derive the outside-the-sandbox read-only allow-list from the agent env.

    The bootstrap conda env (on PATH), the benchmark materials root
    (``BENCHMARK_MATERIALS_ROOT``), and common apptainer install prefixes are
    the paths a run legitimately reads from outside its sandbox.
    """
    out: list[Path] = []
    materials = env.get("BENCHMARK_MATERIALS_ROOT")
    if materials:
        out.append(Path(materials))
    # bootstrap env: the first PATH entry that is not inside the sandbox home is
    # typically the miniforge/bootstrap bin; bind its parent (the env prefix).
    for entry in (env.get("PATH") or "").split(os.pathsep):
        if not entry:
            continue
        p = Path(entry)
        if p.name == "bin" and p.parent.exists():
            out.append(p.parent)
    # apptainer install prefixes (best-effort; -try tolerates absence).
    apptainer = shutil.which("apptainer") or shutil.which("singularity")
    if apptainer:
        ap = Path(apptainer).resolve()
        out.append(ap.parent)               # .../bin
        out.append(ap.parent.parent)        # install prefix (libexec etc.)
    return out
