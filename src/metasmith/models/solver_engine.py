"""Finding, checking, and deciding whether to use the Rust solver engine.

`msm_solver` is not `msm_relay`. The relay runs on the *agent host* and is baked
into the docker image, so `bootstrap.py` can copy the right one out at deploy
time. The solver runs *locally, at plan time*, in whatever process is doing the
planning -- the CLI, the GUI, a notebook -- and that process may never have seen
an agent. So the binaries ship inside the pip wheel and conda package as package
data, and this module is what picks one out.

Three things have to be true before a binary is used, and each has its own way
of failing quietly:

1. **It exists for this platform.** Absence is the normal case, not an error --
   a source checkout has no binaries at all, and everything still works. Absence
   means the Python solver runs.
2. **It answers `version` with versions this build agrees with.** The repo has
   scar tissue here: `LIN_PAYLOAD_VERSION` drifted from its Groovy emitter and
   failed every containerized task while the fast suite stayed green. A version
   is only worth having if it is exchanged and checked, so a mismatch here is a
   *refusal plus a warning*, never a shrug.
3. **It advertises the capability being asked for.** The port lands one piece at
   a time; a build that implements the decision contract but not the search says
   so, and the search falls back without anyone having to remember to.

`METASMITH_SOLVER_ENGINE` overrides the search: a path uses that binary, and
`0`/`off`/`python`/`none` forces the Python path. The forced-off value is what
lets the fallback be *exercised* rather than merely present.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

from ..logging import Log
from .solver_rng import SOLVER_RNG_VERSION

# The envelope: field names, framing, the shape of a request and a reply. It is
# deliberately *not* the same constant as SOLVER_RNG_VERSION, which covers the
# decision contract -- the two move for different reasons, and a single constant
# covering two independently-moving things is how the last desync went unseen.
SOLVER_WIRE_VERSION = 1

ENGINE_NAME = "msm_solver"
#: Where `main/solver_engine/dev.sh --stage` puts the binaries, and what
#: `setup.py`'s `engine/**` package-data entry ships.
ENGINE_DIR = Path(__file__).parent.parent/"engine"
#: Written by the staging step; the packaging guard reads it. Not used here --
#: a host-linked build is perfectly good to *run*, it is only wrong to ship.
BUILD_KIND_FILE = "BUILD_KIND"
ENV_OVERRIDE = "METASMITH_SOLVER_ENGINE"
_FORCE_PYTHON = {"0", "off", "no", "false", "python", "none"}

#: How long `version` gets to answer. It reads no input and writes one line; a
#: binary that cannot manage that is broken, and hanging the planner while it
#: fails to is worse than falling back.
HANDSHAKE_TIMEOUT = 10.0

def platform_slot(machine: str|None=None, system: str|None=None) -> str:
    """The `{architecture}-{system}` suffix for this host.

    Same naming as `bootstrap.py`'s relay slots, so there is one convention in
    the repo rather than two. `aarch64` is folded to `arm64` because that is the
    name the relay already uses and what `uname -m` reports on Apple silicon,
    while Linux reports `aarch64` for the same thing.
    """
    machine = (machine or platform.machine()).lower()
    system = (system or platform.system()).lower()
    machine = {"aarch64": "arm64", "amd64": "x86_64"}.get(machine, machine)
    return f"{machine}-{system}"

def packaged_engine_path(engine_dir: Path|None=None) -> Path|None:
    """The binary shipped for this platform, if one was."""
    p = (engine_dir or ENGINE_DIR)/f"{ENGINE_NAME}.{platform_slot()}"
    return p if p.is_file() else None

@dataclass(frozen=True)
class EngineInfo:
    """What a binary said about itself, once it was believed."""

    path: Path
    engine: str
    engine_version: str
    wire_version: int
    rng_version: int
    capabilities: frozenset[str] = field(default_factory=frozenset)

    def Supports(self, capability: str) -> bool:
        return capability in self.capabilities

def probe_engine(path: Path) -> EngineInfo|None:
    """Run `version` and decide whether to trust what came back.

    Returns `None` for every way this can go wrong, having said which one on the
    way out. The caller's job is to fall back, not to distinguish a missing
    binary from a mismatched one.
    """
    try:
        proc = subprocess.run(
            [str(path), "version"],
            capture_output=True, text=True, timeout=HANDSHAKE_TIMEOUT,
        )
    except (OSError, subprocess.SubprocessError) as e:
        Log.Warn(f"solver engine at [{path}] could not be run: {e}")
        return None
    if proc.returncode != 0:
        Log.Warn(f"solver engine at [{path}] failed its handshake: {proc.stderr.strip()}")
        return None
    try:
        reply = json.loads(proc.stdout)
        info = EngineInfo(
            path=path,
            engine=reply["engine"],
            engine_version=reply["engine_version"],
            wire_version=int(reply["wire_version"]),
            rng_version=int(reply["rng_version"]),
            capabilities=frozenset(reply.get("capabilities", ())),
        )
    except (ValueError, KeyError, TypeError) as e:
        Log.Warn(f"solver engine at [{path}] answered unintelligibly: {e}")
        return None
    if info.engine != ENGINE_NAME:
        Log.Warn(f"[{path}] is [{info.engine}], not [{ENGINE_NAME}]")
        return None
    # The loud half. Two independent implementations that disagree about the
    # contract do not produce a crash -- they produce different plans, which is
    # the failure mode this repo has already paid for once.
    if info.wire_version != SOLVER_WIRE_VERSION or info.rng_version != SOLVER_RNG_VERSION:
        Log.Warn(
            f"solver engine at [{path}] speaks wire v{info.wire_version}/rng"
            f" v{info.rng_version}; this metasmith speaks wire"
            f" v{SOLVER_WIRE_VERSION}/rng v{SOLVER_RNG_VERSION}. Falling back to"
            " the python solver. Rebuild the engine (./dev.sh -be) or reinstall."
        )
        return None
    return info

_cache: tuple[EngineInfo|None]|None = None

def ResetEngineCache():
    """Forget the probe. For tests, and for anything that moves the binary."""
    global _cache
    _cache = None

def GetEngine() -> EngineInfo|None:
    """The engine this process will use, probed once."""
    global _cache
    if _cache is not None: return _cache[0]

    override = os.environ.get(ENV_OVERRIDE, "").strip()
    if override.lower() in _FORCE_PYTHON and override != "":
        Log.Info(f"{ENV_OVERRIDE}=[{override}]: using the python solver")
        _cache = (None,)
        return None
    if override:
        path = Path(override)
        if not path.is_file():
            Log.Warn(f"{ENV_OVERRIDE} points at [{path}], which is not a file")
            _cache = (None,)
            return None
    else:
        path = packaged_engine_path()
        if path is None:
            _cache = (None,) # the normal case in a source checkout; not worth a line
            return None

    _cache = (probe_engine(path),)
    return _cache[0]

def EngineFor(capability: str) -> EngineInfo|None:
    """The engine, but only if it can do the thing being asked of it."""
    info = GetEngine()
    return info if info is not None and info.Supports(capability) else None

def Backend(capability: str) -> str:
    """`"rust"` or `"python"` -- what will actually run. For tests and reporting."""
    return "rust" if EngineFor(capability) is not None else "python"

class EngineError(RuntimeError):
    """The engine was believed, asked to work, and failed anyway."""

def CallEngine(info: EngineInfo, subcommand: str, payload: dict|None=None, timeout: float|None=None) -> dict:
    """One request, one JSON reply. Raises rather than falling back.

    Falling back *here* would hide a real defect: the handshake already decided
    this binary is the right version and can do this job, so a failure now is a
    bug in one of the two implementations and should be seen.
    """
    body = json.dumps(payload) if payload is not None else ""
    try:
        proc = subprocess.run(
            [str(info.path), subcommand],
            input=body, capture_output=True, text=True, timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as e:
        raise EngineError(f"[{info.path} {subcommand}] could not run: {e}") from e
    if proc.returncode != 0:
        raise EngineError(f"[{info.path} {subcommand}] failed: {proc.stderr.strip()}")
    try:
        return json.loads(proc.stdout)
    except ValueError as e:
        raise EngineError(f"[{info.path} {subcommand}] returned non-JSON: {e}") from e
