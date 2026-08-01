"""Finding a solver binary and deciding whether to trust it.

This module answers *is this binary trustworthy*. Which implementation actually
runs is `solver_backend`'s question, and that is the module everything outside
the solver should import.

`msm_solver` is not `msm_relay`. The relay runs on the *agent host* and is baked
into the docker image, so `bootstrap.py` can copy the right one out at deploy
time. The solver runs *locally, at plan time*, in whatever process is doing the
planning -- the CLI, the GUI, a notebook -- and that process may never have seen
an agent. So the binaries ship inside the pip wheel and conda package as package
data, and this module is what picks one out. There is exactly one place a binary
comes from: `<metasmith package>/engine/`, which `PYTHONPATH=src` makes the same
directory `./dev.sh -be` stages into, so a source checkout, the container image
and an installed conda package all resolve identically. PATH is deliberately not
consulted -- a second source of binaries is the thing this design is avoiding.

Three things have to be true before a binary is used, and each has its own way
of failing quietly:

1. **It exists for this platform.** Absence is recoverable but no longer silent:
   the Python solver takes over and `solver_backend` says so once, because a
   correct-and-fifteen-times-slower planner is not something anyone notices.
2. **It answers `version` with versions this build agrees with.** The repo has
   scar tissue here: `LIN_PAYLOAD_VERSION` drifted from its Groovy emitter and
   failed every containerized task while the fast suite stayed green. A version
   is only worth having if it is exchanged and checked, so a mismatch here is a
   *refusal plus a warning*, never a shrug.
3. **It advertises the capability being asked for.** The port lands one piece at
   a time; a build that implements the decision contract but not the search says
   so, and the search falls back -- saying so, per (1) -- without anyone having
   to remember to.

Nothing here reads the environment. The implementation this process uses is
pinned in code, via `solver_backend._set_solver_class`.
"""

from __future__ import annotations

import json
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
SOLVER_WIRE_VERSION = 2

ENGINE_NAME = "msm_solver"
#: Where `main/solver_engine/dev.sh --stage` puts the binaries, and what
#: `setup.py`'s `engine/**` package-data entry ships.
ENGINE_DIR = Path(__file__).parent.parent/"engine"
#: Written by the staging step; the packaging guard reads it. Not used here --
#: a host-linked build is perfectly good to *run*, it is only wrong to ship.
BUILD_KIND_FILE = "BUILD_KIND"

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
    """The engine this build ships for this platform, probed once."""
    global _cache
    if _cache is not None: return _cache[0]
    path = packaged_engine_path()
    if path is None:
        # Not a warning here. Absence is a resolution outcome, not a decision;
        # `solver_backend` is what knows whether anyone asked for this.
        _cache = (None,)
        return None
    _cache = (probe_engine(path),)
    return _cache[0]

def EngineFor(capability: str) -> EngineInfo|None:
    """The engine, but only if it can do the thing being asked of it."""
    info = GetEngine()
    return info if info is not None and info.Supports(capability) else None

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
