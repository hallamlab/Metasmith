from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path


_SYSTEM_RO = (
    ("/usr", False),
    ("/bin", True),
    ("/sbin", True),
    ("/lib", True),
    ("/lib64", True),
    ("/lib32", True),
    ("/etc", False),
    ("/opt", True),
    ("/var/lib", True),
    ("/run/systemd/resolve", True),
)


@dataclass(frozen=True)
class BindSpec:
    src: str
    dst: str
    mode: str = "ro"
    optional: bool = False

    def to_argv(self) -> list[str]:
        flag = {"ro": "--ro-bind", "rw": "--bind", "dev": "--dev-bind"}[self.mode]
        if self.optional:
            flag += "-try"
        return [flag, self.src, self.dst]


def resolve_bwrap() -> str | None:
    explicit = os.environ.get("MSM_E2E_BWRAP")
    if explicit:
        p = Path(explicit)
        return str(p) if p.exists() else None
    found = shutil.which("bwrap")
    return found


def jail_enabled(env: dict[str, str] | None = None) -> bool:
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
    b = resolve_bwrap()
    if b is None:
        raise RuntimeError(
            "MSM_E2E_JAIL requests a bwrap jail but no bwrap binary was found "
            "(set MSM_E2E_BWRAP=/path/to/bwrap or install bwrap on PATH)."
        )
    return b


def build_bwrap_argv(
    inner_argv: list[str],
    *,
    binds: list[BindSpec],
    bwrap: str = "bwrap",
    unshare_user: bool = False,
    chdir: str | None = None,
    tmp_bind: BindSpec | None = None,
) -> list[str]:
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


_CLAUDE_DIR_REL = ".claude"


def default_binds(
    sandbox_root: Path,
    home: Path,
    *,
    extra_ro: list[Path] | None = None,
    share_claude_auth: bool = True,
    claude_bin: str | None = None,
) -> list[BindSpec]:
    binds: list[BindSpec] = [
        BindSpec(src, src, "ro", optional) for src, optional in _SYSTEM_RO
    ]
    seen: set[str] = set()
    for p in (extra_ro or []):
        s = str(p)
        if s and s not in seen:
            seen.add(s)
            binds.append(BindSpec(s, s, "ro", optional=True))
    if claude_bin:
        bindir = str(Path(claude_bin).resolve().parent)
        if bindir not in seen:
            seen.add(bindir)
            binds.append(BindSpec(bindir, bindir, "ro", optional=True))
    binds.append(BindSpec(str(sandbox_root), str(sandbox_root), "rw"))
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
    bwrap = require_bwrap()
    sandbox = sandbox.resolve()
    home = home.resolve()
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
    out: list[Path] = []
    materials = env.get("BENCHMARK_MATERIALS_ROOT")
    if materials:
        out.append(Path(materials))
    for entry in (env.get("PATH") or "").split(os.pathsep):
        if not entry:
            continue
        p = Path(entry)
        if p.name == "bin" and p.parent.exists():
            out.append(p.parent)
    apptainer = shutil.which("apptainer") or shutil.which("singularity")
    if apptainer:
        ap = Path(apptainer).resolve()
        out.append(ap.parent)
        out.append(ap.parent.parent)
    return out
