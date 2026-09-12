from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable

import yaml

from ...coms.ipc import GenerateId
from ...coms.terminals import RemoveLeadingIndent
from ...constants import AgentPaths
from ...env import Environment, Rootfs, Runtime, Shell
from ...hashing import KeyGenerator
from ...logging import Log
from ...serialization import IsText
from ..solver import Dependency, Endpoint
from .resources import Gpus, Size

from ..paths import ContextPath, PathMap  # noqa: F401


class AmbiguousProvenance(Exception):
    pass
class AmbiguousSlotChannel(Exception):
    pass
@dataclass
class ContextData:
    input_group: list[ContextPath]
    endpoint: Endpoint
    type_name: str
    provenance: list[dict] = field(default_factory=list)
    path: ContextPath = field(init=False)

    def __post_init__(self) -> None:
        assert len(self.input_group)>0
        self.path = self.input_group[0]

@dataclass
class ExecutionResult:
    success: bool = True
    manifest: list[dict[Dependency, Path]] = field(default_factory=list)

class ExecutionFailed(Exception):
    pass

def ResolveEnvImage(content: str, runtime: Runtime, source: str|Path="<env>") -> str:
    try:
        parsed = yaml.safe_load(content)
    except yaml.YAMLError:
        parsed = None
    if isinstance(parsed, dict):
        key = "conda" if runtime == Runtime.MAMBA else "container"
        value = parsed.get(key)
        assert value, (
            f"env declaration [{source}] has no '{key}:' entry for runtime "
            f"[{runtime.value}] (keys present: {sorted(parsed)})"
        )
        return str(value).strip()
    return content.strip()

_RESERVED_EXPORTS = frozenset({"PATH", "HOME", "LD_LIBRARY_PATH", "TMPDIR", "PWD"})

def _validate_exports(exports: dict[str, "str|Path"]|None) -> dict[str, str]:
    if not exports: return {}
    out: dict[str, str] = {}
    for k, v in exports.items():
        k = str(k)
        assert k not in _RESERVED_EXPORTS, (
            f"export [{k}] is reserved by the framework; "
            f"reserved names are {sorted(_RESERVED_EXPORTS)}"
        )
        assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k), (
            f"export name [{k}] is not a valid shell identifier"
        )
        out[k] = str(v)
    return out


def _materialised_test(env: Environment) -> str:
    sif, sandbox = env.GetLocalPath(), env.GetSandboxPath()
    sif_stamp, sandbox_stamp = env.GetLocalStampPath(), env.GetSandboxStampPath()
    ok_sif = f'( [ -e {sif} ] && [ -e {sif_stamp} ] )'
    ok_sandbox = f'( [ -d {sandbox} ] && [ -e {sandbox_stamp} ] )'
    match env.rootfs:
        case Rootfs.SIF:
            return ok_sif
        case Rootfs.SANDBOX:
            return ok_sandbox
        case _:
            return f'( {ok_sif} || {ok_sandbox} )'


@dataclass
class ExecutionContext:
    _inputs: list[dict[Dependency, ContextData]]
    _get_output_paths: Callable[[Dependency, int, int], ContextPath]
    external_shell: Shell
    external_cwd: Path
    external_agent_home: Path
    _environment: Runtime|Environment = Runtime.DOCKER
    params: dict = field(default_factory=dict)
    _batch_index: int = 0
    _detected_gpus: list|None = None
    _slot_keys: dict[Dependency, str] = field(default_factory=dict)
    _ambiguous_slots: set[str] = field(default_factory=set)

    def __post_init__(self):
        if isinstance(self._environment, Runtime):
            self._environment = Environment(image="", runtime=self._environment)

    def _tool_environment(self, image: str, **kw) -> Environment:
        container = replace(self._environment.container, **kw) if kw else self._environment.container
        return replace(self._environment, image=image, container=container)

    def GetMeta(self, key: Dependency):
        d = self._inputs[self._batch_index]
        if key in d:
            return d[key]
        raise KeyError(f"key [{key}:{key.key}] not found in [{set(str(x)+':'+x.key for x in d.keys())}]")

    def Input(self, key: Dependency):
        return self.GetMeta(key).path
    
    def InputGroup(self, key: Dependency):
        return self.GetMeta(key).input_group

    def ProvenanceOf(self, path: ContextPath) -> dict|None:
        d = self._inputs[self._batch_index]
        for cd in d.values():
            if not cd.provenance:
                continue
            for i, p in enumerate(cd.input_group):
                if p == path:
                    return cd.provenance[i]
        return None

    def _slot_channel(self, key: Dependency) -> str:
        chan = self._slot_keys.get(key)
        if chan is None:
            raise KeyError(
                f"no channel recorded for slot [{key}:{key.key}]; provenance "
                "needs the `slk` line the compiler writes into the step meta"
            )
        if chan in self._ambiguous_slots:
            raise AmbiguousSlotChannel(
                f"slot [{key}:{key.key}] shares channel [{chan}] with another "
                "requirement of the same type -- the wire cannot tell them "
                "apart, so provenance for it is unanswerable. Give the two "
                "slots distinguishable types."
            )
        return chan

    def SourcesOf(self, path: ContextPath, key: Dependency) -> list[ContextPath]:
        if key in self._inputs[self._batch_index]:
            target = self.GetMeta(key)
            if any(p == path for p in target.input_group):
                return [path]
        src = self.ProvenanceOf(path)
        if not src:
            return []
        chan = self._slot_channel(key)
        wanted = {str(x) for x in src.get(chan, [])}
        if not wanted:
            return []
        target = self.GetMeta(key)
        if not target.provenance:
            return []
        out = []
        for i, p in enumerate(target.input_group):
            ids = {str(x) for x in target.provenance[i].get(chan, [])}
            if ids & wanted:
                out.append(p)
        return out

    def SourceOf(self, path: ContextPath, key: Dependency) -> ContextPath|None:
        found = self.SourcesOf(path, key)
        if len(found) == 0:
            return None
        if len(found) > 1:
            raise AmbiguousProvenance(
                f"[{path.local.name}] descends from {len(found)} items of slot "
                f"[{key}:{key.key}] ({', '.join(p.local.name for p in found)}). "
                "The producing task collected more than one, so there is no "
                "single answer -- group that step per ancestor, or use "
                "SourcesOf."
            )
        return found[0]

    def Output(self, key: Dependency, i: int=0):
        return self._get_output_paths(key, i, self._batch_index)  

    def AsBatch(self):
        while self._batch_index < len(self._inputs):
            yield self
            self._batch_index += 1
        self._batch_index = 0

    def LocalShell(self, cmd: str):
        cmd = RemoveLeadingIndent(cmd)
        Log.Info(f"invoked local shell, calling subprocess.run() with:")
        for line in cmd.split("\n"):
            Log.Info(f"    {line}")
        subprocess.run(cmd, shell=True, executable='/bin/bash')

    def DeclaredGpus(self) -> tuple[Gpus, Size|None]:
        raw = self.params.get("gpus")
        if not isinstance(raw, dict): return Gpus.NONE, None
        try:
            toggle = Gpus(raw.get("gpus", Gpus.NONE.value))
        except ValueError:
            toggle = Gpus.NONE
        mem = raw.get("gpu_memory_gb")
        return toggle, None if mem is None else Size.GB(mem)

    def DetectGpus(self, refresh: bool=False) -> list[Size]:
        if self._detected_gpus is not None and not refresh:
            return list(self._detected_gpus)
        FLAG = "msm_gpu"
        probe = (
            'command -v nvidia-smi >/dev/null 2>&1 && '
            f'nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits 2>/dev/null'
            f' | sed "s/^/{FLAG} /" || true'
        )
        try:
            res = self.external_shell.Exec(probe, history=True)
        except Exception as e:
            Log.Warn(f"gpu detection failed: {e}")
            self._detected_gpus = []
            return []
        found: list[Size] = []
        for line in res.out:
            line = line.strip()
            if not line.startswith(FLAG): continue
            val = line[len(FLAG):].strip()
            try:
                found.append(Size.MB(float(val)))
            except ValueError:
                continue
        self._detected_gpus = found
        return list(found)

    def GetContainerModel(self, image: Dependency, binds: list[tuple[Path|str, Path|str]]|None=None, args: list[str]|None=None):
        path = self._inputs[self._batch_index][image].path
        if IsText(path.local):
            with open(path.local) as f:
                content = f.read()
            image_path = ResolveEnvImage(content, self._environment.runtime, path.local)
        else:
            image_path = str(path.external)
    
        _probe = self._tool_environment(str(image_path))

        _binds: list[tuple[Path, Path]] = []
        for batch_item in (self._inputs if _probe.needs_relay else []):
            for _, v in list(batch_item.items()):
                for p in v.input_group:
                    if p.container.is_relative_to(AgentPaths.HOME_ROOT): continue
                    src = p.external.parent
                    if not p.container.is_absolute():
                        dest = src
                    else:
                        dest = p.container.parent
                    if not src.is_absolute() or not dest.is_absolute(): continue

                    found = False
                    for i, (a, b) in enumerate(_binds):
                        ac = Path(os.path.commonpath([a, src]))
                        bc = Path(os.path.commonpath([b, dest]))
                        THRES = 3
                        if len(ac.parts)>=THRES:
                            found = True
                            break
                    if found:
                        _binds[i] = ac, bc
                    else:
                        _binds.append((src, dest))
        if binds is None: binds = []
        if _probe.needs_relay:
            container_ws = AgentPaths.WORK_ROOT
            binds += [
                ('${TMPDIR-"/tmp"}', '${TMPDIR-"/tmp"}'),
                (self.external_agent_home, AgentPaths.HOME_ROOT),
                (self.external_cwd, container_ws),
            ]
            binds += sorted([(s, d) for s, d in _binds])
        else:
            assert not binds, (
                f"binds are meaningless without a container boundary "
                f"(runtime [{_probe.runtime.name}]): {binds}"
            )
            container_ws = self.external_cwd

        extra_args = list(args) if args else []
        declared, _ = self.DeclaredGpus()
        if declared is not Gpus.NONE and self.DetectGpus():
            gpu_args = _probe.MakeGpuArgs()
            if gpu_args and gpu_args[0] not in extra_args:
                extra_args = gpu_args + extra_args

        env = self._tool_environment(
            str(image_path),
            workdir = container_ws,
            binds = binds,
            cache = self.external_agent_home/AgentPaths.CONTAINER_CACHE,
        )
        env.extra_args = extra_args
        return env

    def ExecWithEnv(
        self,
        env: Dependency,
        cmd: str,
        shell: str="bash",
        binds: list[tuple[Path|str, Path|str]]|None=None,
        args: list[str]|None=None,
        exports: dict[str, str|Path]|None=None,
        history: bool=True,
    ):
        """Run `cmd` in the tool environment `env` names, whatever the agent's runtime is.

        The body says what to run and where; the runtime decides how. `GetContainerModel`
        reads `container:` or `conda:` from the resource by runtime, and drops the mounts
        where there is no mount namespace to put them in.
        """
        return self._ExecInEnv(
            image=env, cmd=cmd, shell=shell, binds=binds, args=args,
            exports=exports, history=history,
        )

    def _ExecInEnv(self, image: Dependency, cmd: str, shell="bash", binds: list[tuple[Path|str, Path|str]]|None=None, args: list[str]|None=None, exports: dict[str, str|Path]|None=None, history: bool=True):
        env = self.GetContainerModel(image, binds, args)
        assert env.container.workdir is not None
        use_cache = False
        cached_path = env.GetLocalPath()
        if cached_path is not None:
            FLAG = "cached image exists"
            res = self.external_shell.Exec(
                f'{_materialised_test(env)} && echo "{FLAG}"',
                history=True,
            )
            if FLAG in res.out:
                use_cache = True

        if (not use_cache and cached_path is not None
                and env.runtime == Runtime.APPTAINER):
            sandbox_path = env.GetSandboxPath()
            FLAG = "transform-image-ready"
            materialise = env.MakeMaterialiseCommand().replace("'", "'\\''")
            res = self.external_shell.Exec(
                f'mkdir -p "{cached_path.parent}"; '
                f'flock "{sandbox_path}.lock" -c \'{materialise}\'; '
                f'{_materialised_test(env)} && echo "{FLAG}"',
                history=True,
            )
            if FLAG in res.out:
                use_cache = True

        cmd = RemoveLeadingIndent(cmd)
        Log.Info(f"executing container [{env.image}] using [{env.runtime.name}]")
        h, k = KeyGenerator.FromStr(cmd)
        _bounce_script = Path(f"./_metasmith/.bounce.{k}")
        _bounce_script.parent.mkdir(parents=True, exist_ok=True)
        # The marker is written to its absolute path in the container, and the
        # trap ends on the command's own status. A transform is free to change
        # directory -- several in the standard library do -- and a relative
        # marker follows it: into a directory the task uid cannot write, the
        # write fails, the trap's failure becomes the script's status, and a
        # step that succeeded reports failure.
        marker_name = f"exitcode.{GenerateId()}"
        exit_codef = Path(marker_name)
        container_marker = env.container.workdir/marker_name
        with open(_bounce_script, "w") as f:
            script = [
                f"cd {env.container.workdir}",
                "on_exit() {",
                "    __msm_code=$?",
                f"    echo $__msm_code > {container_marker} 2>/dev/null || true",
                "    exit $__msm_code",
                "}",
                "trap on_exit EXIT",
                "set -e",
                *(f"export {k}={shlex.quote(str(v))}" for k, v in _validate_exports(exports).items()),
                cmd,
            ]
            f.write("\n".join(script))
        Log.Info(f"command with bounce at [{_bounce_script}]:")
        for line in cmd.split("\n"):
            Log.Info(f"    {line}")
        Log.Info(f"binds:")
        for s, d in env.container.binds:
            Log.Info(f"    {s} -> {d}")
        _container_start = f"{env.MakeRunCommand(local=use_cache)} {shell}"
        Log.Info(f"-> container start: [{_container_start}]")
        BREAK_LENGTH = 60
        msg = "-> container ->"
        Log.Info(msg+"-"*(BREAK_LENGTH-len(msg)))
        result = self.external_shell.Exec(
            f"{_container_start} {env.container.workdir/_bounce_script}",
            timeout=None, history=history
        )
        # The container's own status is the source of truth: it survives a
        # transform that leaves the shell somewhere unwritable, and it is the
        # only thing there is when the trap never ran at all. The marker
        # refines it, and a marker that cannot be read is reported as such --
        # not silently turned into a plain exit 1, which is what made a
        # succeeded step and a failed one indistinguishable.
        marker_status = None
        try:
            with open(exit_codef) as f:
                marker_status = int(f.readline().strip())
        except Exception:
            marker_status = None
        exit_code = result.exit_code if result.exit_code is not None else marker_status
        if exit_code is None:
            exit_code = 1
        msg = f"<- container exit [{exit_code}] <-"
        Log.Info(msg+"-"*(BREAK_LENGTH-len(msg)))
        if marker_status is None:
            Log.Warn(
                f"the exit marker [{exit_codef}] could not be read, so the"
                f" container's own status [{exit_code}] is all there is: the"
                f" script did not reach its exit trap"
            )
        elif marker_status != exit_code:
            Log.Warn(
                f"the exit marker [{exit_codef}] says [{marker_status}] and the"
                f" container says [{exit_code}]; taking the container's"
            )
        if exit_codef.exists(): exit_codef.unlink()
        if exit_code != 0:
            Log.Error(
                f"the script in the container exited with code [{exit_code}]"
            )
            time.sleep(5)
            sys.exit(exit_code)
        return result
