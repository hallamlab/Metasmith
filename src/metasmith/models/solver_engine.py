from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from ..caching.keys import content_multihash_key
from ..logging import Log
from .solver_rng import SOLVER_RNG_VERSION

SOLVER_WIRE_VERSION = 2

ENGINE_NAME = "msm_solver"
ENGINE_DIR = Path(__file__).parent.parent/"engine"
BUILD_KIND_FILE = "BUILD_KIND"

HANDSHAKE_TIMEOUT = 10.0

def platform_slot(machine: str|None=None, system: str|None=None) -> str:
    machine = (machine or platform.machine()).lower()
    system = (system or platform.system()).lower()
    machine = {"aarch64": "arm64", "amd64": "x86_64"}.get(machine, machine)
    return f"{machine}-{system}"

def packaged_engine_path(engine_dir: Path|None=None) -> Path|None:
    p = (engine_dir or ENGINE_DIR)/f"{ENGINE_NAME}.{platform_slot()}"
    return p if p.is_file() else None

def _stage_dir() -> Path:
    uid = getattr(os, "geteuid", lambda: "shared")()
    return Path(tempfile.gettempdir())/f"metasmith-engine-{uid}"

def _runnable_engine_path(path: Path) -> Path|None:
    if os.access(path, os.X_OK):
        return path
    try:
        digest = content_multihash_key(path).hex()
    except OSError as e:
        Log.Warn(f"solver engine at [{path}] could not be read: {e}")
        return None

    staged = _stage_dir()/f"{path.name}.{digest[:16]}"
    if os.access(staged, os.X_OK):
        return staged
    try:
        staged.parent.mkdir(parents=True, exist_ok=True)
        tmp = staged.with_name(f"{staged.name}.{os.getpid()}.part")
        shutil.copyfile(path, tmp)
        os.chmod(tmp, 0o755)
        os.replace(tmp, staged)
    except OSError as e:
        Log.Warn(
            f"solver engine at [{path}] is not executable and could not be"
            f" staged to [{staged}]: {e}. Falling back to the python solver,"
            " which is correct but roughly 15x slower."
        )
        return None
    Log.Info(f"staged a runnable copy of the solver engine at [{staged}]")
    return staged

@dataclass(frozen=True)
class EngineInfo:
    path: Path
    engine: str
    engine_version: str
    wire_version: int
    rng_version: int
    capabilities: frozenset[str] = field(default_factory=frozenset)

    def Supports(self, capability: str) -> bool:
        return capability in self.capabilities

def probe_engine(path: Path) -> EngineInfo|None:
    try:
        proc = subprocess.run(
            [str(path), "version"],
            capture_output=True, text=True, timeout=HANDSHAKE_TIMEOUT,
        )
    except PermissionError as e:
        mode = "?"
        try:
            mode = oct(path.stat().st_mode & 0o777)
        except OSError:
            pass
        Log.Warn(
            f"solver engine at [{path}] is not executable (mode {mode}): {e}."
            " A staged copy should have prevented this; see _runnable_engine_path."
        )
        return None
    except FileNotFoundError as e:
        Log.Warn(f"solver engine at [{path}] is gone: {e}. It was there a moment ago.")
        return None
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
    global _cache
    _cache = None

def GetEngine() -> EngineInfo|None:
    global _cache
    if _cache is not None: return _cache[0]
    path = packaged_engine_path()
    if path is None:
        _cache = (None,)
        return None
    runnable = _runnable_engine_path(path)
    if runnable is None:
        _cache = (None,)
        return None
    _cache = (probe_engine(runnable),)
    return _cache[0]

def EngineFor(capability: str) -> EngineInfo|None:
    info = GetEngine()
    return info if info is not None and info.Supports(capability) else None

class EngineError(RuntimeError):
    pass
def CallEngine(info: EngineInfo, subcommand: str, payload: dict|None=None, timeout: float|None=None) -> dict:
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
