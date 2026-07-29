"""The execution contract: what a transform's protocol is handed, and how it runs.

`ExecutionContext` is the protocol's whole view of the world -- its typed
inputs and outputs as `ContextPath` triples, and `ExecWithEnv()` to run
something. `EnvDispatch` is the chain that returns: a transform declares one
arm per world (`ifContainerDo` / `ifVirtualEnvDo`) and each arm dispatches the
instant it is declared, if it matches. A protocol therefore never learns which
runtime it is on, and code that branches on the runtime is a bug -- the `env`
package owns every per-runtime difference.

The record of which arms were *declared* is kept even when none matched, which
is what makes "no arm matched" reportable and what the stage-time portability
manifest reads.

This module deliberately knows nothing about libraries or transform instances;
it sits alongside them, not above them.
"""

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
from ...env import Environment, Runtime, Shell
from ...hashing import KeyGenerator
from ...logging import Log
from ...serialization import IsText
from ..solver import Dependency, Endpoint
from .resources import Gpus, Size

# ContextPath has moved to metasmith.models.paths; re-export to preserve
# the existing `from metasmith.models.libraries import ContextPath` form.
from ..paths import ContextPath, PathMap  # noqa: F401


@dataclass
class ContextData:
    input_group: list[ContextPath]
    endpoint: Endpoint
    type_name: str
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
    """Resolve a generic env-declaration file's content to the image / env-name
    the active runtime should use.

    The generic format is a YAML mapping with an optional ``container:`` (a
    ``docker://…`` URI, used by the container runtimes) and/or an optional
    ``conda:`` (a conda/mamba env name, used by ``Runtime.MAMBA``). Selection is
    by the single global runtime; a missing key for the selected runtime is a
    hard error naming the file.

    Legacy resources (``*.oci`` whose whole content is a bare URI) parse as a
    YAML scalar, not a mapping, and are treated verbatim as the container image
    so existing container runs keep working unchanged.
    """
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
    # legacy bare-URI (*.oci) or unparseable content -> use verbatim as the image
    return content.strip()

CONTAINER_ARM = "ifContainerDo"
VIRTUAL_ENV_ARM = "ifVirtualEnvDo"

# Names whose meaning belongs to the framework, not the transform. Overwriting
# any of these from a protocol reshapes the environment metasmith just built
# (PATH/LD_LIBRARY_PATH), relocates the workdir the bounce script cd'd into
# (PWD), or redirects scratch the runtime already agreed on (TMPDIR, HOME).
# Tool-native names like GTDBTK_DATA_PATH are exactly what exports are for.
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


class EnvDispatch:
    """The chain returned by :meth:`ExecutionContext.ExecWithEnv`.

    Holds no command of its own — each arm is dispatched the instant it is
    declared, if it matches. What it does hold is the record of which arms the
    transform declared, which is what makes the "no arm matched" failure
    reportable and what the stage-time portability manifest reads.
    """

    def __init__(self, context: "ExecutionContext"):
        self._context = context
        self.declared: list[str] = []
        self._matched: str|None = None

    @property
    def matched(self) -> str|None:
        return self._matched

    def _dispatch(self, arm: str, applies: bool, **kw):
        self.declared.append(arm)
        if not applies:
            Log.Info(f"skipping [{arm}] (does not apply to this runtime)")
            return self
        assert self._matched is None, (
            f"[{arm}] and [{self._matched}] both apply to this runtime; "
            "an ExecWithEnv chain must have exactly one matching arm"
        )
        self._matched = arm
        self._context._ExecInEnv(**kw)
        return self

    def ifContainerDo(
        self,
        env: Dependency,
        cmd: str,
        shell: str="bash",
        binds: list[tuple[Path|str, Path|str]]|None=None,
        args: list[str]|None=None,
        exports: dict[str, str|Path]|None=None,
        history: bool=True,
    ) -> "EnvDispatch":
        """How this step runs when the tool lives in a container image.

        `binds` are host->container mount pairs and `args` are extra flags for
        the runtime's own run command; both are meaningless without a mount
        namespace, which is why they live here and not on the venv arm.
        """
        return self._dispatch(
            CONTAINER_ARM, self._context._crosses_boundary,
            image=env, cmd=cmd, shell=shell, binds=binds, args=args,
            exports=exports, history=history,
        )

    def ifVirtualEnvDo(
        self,
        env: Dependency,
        cmd: str,
        shell: str="bash",
        exports: dict[str, str|Path]|None=None,
        history: bool=True,
    ) -> "EnvDispatch":
        """How this step runs when the tool is a package set on PATH.

        There is no mount namespace here, so there is no `binds` — accepting one
        could only mean ignoring it, which is the bug this API replaced. Hand the
        tool its paths directly (every ContextPath view is the host path under
        this runtime) or through `exports`.
        """
        return self._dispatch(
            VIRTUAL_ENV_ARM, not self._context._crosses_boundary,
            image=env, cmd=cmd, shell=shell, exports=exports, history=history,
        )


# work as if batch of 1 item
# until explicitly batch iterated
@dataclass
class ExecutionContext:
    _inputs: list[dict[Dependency, ContextData]]
    _get_output_paths: Callable[[Dependency, int, int], ContextPath]
    external_shell: Shell # relay shell for container runtimes, local shell otherwise
    external_cwd: Path
    external_agent_home: Path
    # The environment a *tool* runs in on this host. Private: a protocol has no
    # business branching on the runtime, and everything that used to require it
    # (GPU flags, bind dialect, whether there is a boundary at all) is answered
    # by the env package. Never `native` -- native describes whether metasmith
    # itself is containerized, which says nothing about the tool's own image.
    _environment: Runtime|Environment = Runtime.DOCKER
    params: dict = field(default_factory=dict)
    _batch_index: int = 0
    _detected_gpus: list|None = None
    _env_dispatches: list["EnvDispatch"] = field(default_factory=list)

    def __post_init__(self):
        # Accept a bare Runtime for the many construction sites that only have
        # one; normalise to an Environment so routing has a single shape.
        if isinstance(self._environment, Runtime):
            self._environment = Environment(image="", runtime=self._environment)

    @property
    def _crosses_boundary(self) -> bool:
        # Whether a tool launched from here lands on the other side of a
        # container boundary. The single question every arm dispatch turns on.
        assert isinstance(self._environment, Environment)
        return self._environment.needs_relay

    def _tool_environment(self, image: str, **kw) -> Environment:
        # The container half is rebuilt per call (each tool has its own image,
        # workdir and binds); runtime/native carry over from the template.
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
        # What this step *asked* for at stage time. Staged by the generator into
        # the step meta file; absent (-> Gpus.NONE) for any step that declared
        # no GPU and for workspaces staged before GPU support existed.
        raw = self.params.get("gpus")
        if not isinstance(raw, dict): return Gpus.NONE, None
        try:
            toggle = Gpus(raw.get("gpus", Gpus.NONE.value))
        except ValueError:
            toggle = Gpus.NONE
        mem = raw.get("gpu_memory_gb")
        return toggle, None if mem is None else Size.GB(mem)

    def DetectGpus(self, refresh: bool=False) -> list[Size]:
        """Per-device VRAM of the GPUs this task actually got, on the exec host.

        Probes through `external_shell`, which is the relay for container
        runtimes and the local shell for mamba/native -- so the answer is about
        the machine the tool will run on, under every runtime. Reports what was
        *allocated*, not what was asked for: under a partial SLURM allocation or
        a MIG slice (where CUDA_VISIBLE_DEVICES is a MIG-<uuid> rather than an
        index) those differ, and the allocated figure is the one a tool sizing
        its own offload needs. A host with no nvidia-smi is a valid empty
        answer, not an error.

        Memoized: the answer cannot change within a task, and GetContainerModel
        consults it on every ExecWithEnv call. Pass refresh=True to probe
        again.
        """
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
            # Generic env declaration: select container: / conda: by the global
            # runtime (legacy bare-URI *.oci files resolve verbatim).
            image_path = ResolveEnvImage(content, self._environment.runtime, path.local)
        else:
            image_path = str(path.external)
    
        # Probe whether this runtime crosses a container boundary. When it
        # does not (mamba/native), paths are identity: the tool runs on the
        # host filesystem in the real cwd, so there is no /ws remap and no
        # binds to compute. The PathMap views collapse to equal.
        _probe = self._tool_environment(str(image_path))

        _binds: list[tuple[Path, Path]] = []
        # Accumulating implicit input binds is dead work with no boundary --
        # and a half-computed bind list is exactly what invites a future
        # reader to "just use it" and reintroduce the silent-drop bug.
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
                        THRES = 3 # '/', '1', '2' >> /1/2
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
            # No mount namespace to bind into. Callers must not reach here with
            # binds -- ExecWithEnv routes them through ifContainerDo, which only
            # runs under a container runtime.
            assert not binds, (
                f"binds are meaningless without a container boundary "
                f"(runtime [{_probe.runtime.name}]): {binds}"
            )
            container_ws = self.external_cwd

        extra_args = list(args) if args else []
        # A step that declared a GPU gets its runtime's GPU flags for free --
        # the transform author never writes `--nv` / `--gpus all`, and never
        # branches on the runtime to pick the dialect. Transforms that still
        # pass them by hand keep working: framework flags whose leading token is
        # already present in `args=` are dropped rather than duplicated.
        #
        # Gated on a device actually being present, not merely declared: a
        # Gpus.OPTIONAL step is expected to land on CPU-only hosts, and there
        # `docker run --gpus all` fails outright ("could not select device
        # driver"), turning a graceful fallback into a dead task.
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

    def ExecWithEnv(self) -> "EnvDispatch":
        """Declare how this step invokes its tool in each world.

        A container is a filesystem layout with an entrypoint; a conda env is a
        package set on PATH. They are not interchangeable -- a third of the tool
        library has no conda form, and where both exist the invocation often
        differs. So the transform declares each world it supports and metasmith
        runs the one that matches the agent::

            context.ExecWithEnv() \\
                .ifContainerDo(env=dep, cmd=..., binds=[...], args=[...]) \\
                .ifVirtualEnvDo(env=dep, cmd=..., exports={...})

        Arms are declarations evaluated in place, not a sequence: the matching
        one runs the moment it is called and the other is recorded and skipped,
        so writing side effects between arms makes their order observable. There
        is no terminal call; a chain whose every arm was skipped is caught by the
        framework after the protocol returns (see `UnmatchedEnvDispatches`),
        because a silently-empty step is worse than a loud one.

        Either arm may be omitted. A container-only tool simply has no
        `ifVirtualEnvDo`, which is what makes "can this run without containers?"
        a question the tooling can answer statically.
        """
        d = EnvDispatch(self)
        self._env_dispatches.append(d)
        return d

    def UnmatchedEnvDispatches(self) -> list["EnvDispatch"]:
        """The `ExecWithEnv()` chains this execution reached that ran nothing.

        Read by `bootstrap.ExecuteStep` after the protocol returns: a chain that
        declares only a container arm on a mamba agent would otherwise no-op its
        way to a step that reports success and produces nothing.
        """
        return [d for d in self._env_dispatches if not d._matched]

    def _ExecInEnv(self, image: Dependency, cmd: str, shell="bash", binds: list[tuple[Path|str, Path|str]]|None=None, args: list[str]|None=None, exports: dict[str, str|Path]|None=None, history: bool=True):
        env = self.GetContainerModel(image, binds, args)
        assert env.container.workdir is not None # for typing
        use_cache = False
        cached_path = env.GetLocalPath()
        if cached_path is not None:
            FLAG = "cached image exists"
            sandbox_path = env.GetSandboxPath()
            res = self.external_shell.Exec(
                f'( [ -e {cached_path} ] || [ -d {sandbox_path} ] ) && echo "{FLAG}"',
                history=True,
            )
            if FLAG in res.out:
                use_cache = True

        if (not use_cache and cached_path is not None
                and env.runtime == Runtime.APPTAINER):
            # Not yet materialised. The default fallback is `apptainer exec
            # docker://...`, which lazily converts the image to a SIF via
            # mksquashfs -- and mksquashfs aborts on large images on some hosts
            # (micb0: "malloc(): corrupted top size", e.g. external_checkm2,
            # gtdbtk). On use-sandbox hosts (apptainer>=1.4 without setuid
            # starter-suid) build the rootfs as a sandbox directory instead: it
            # extracts the OCI layers directly, never invokes mksquashfs, and
            # is cached for reuse. use-sif hosts are left alone -- a sandbox
            # there hits the Bug E.4 fuse-overlayfs SIGBUS under SLURM. An
            # flock guards parallel transforms sharing one image.
            sandbox_path = env.GetSandboxPath()
            probe = env.MakeSandboxDecisionProbe()
            build_sandbox = env.MakeBuildSandboxCommand(from_image=True)
            FLAG = "transform-sandbox-ready"
            res = self.external_shell.Exec(
                f'V=$({probe}); '
                f'if [ "$V" = "use-sandbox" ]; then '
                f'mkdir -p "{sandbox_path.parent}"; '
                f'flock "{sandbox_path}.lock" -c \'[ -d "{sandbox_path}" ] || {build_sandbox}\'; '
                f'[ -d "{sandbox_path}" ] && echo "{FLAG}"; fi',
                history=True,
            )
            if FLAG in res.out:
                use_cache = True

        cmd = RemoveLeadingIndent(cmd)
        Log.Info(f"executing container [{env.image}] using [{env.runtime.name}]")
        h, k = KeyGenerator.FromStr(cmd)
        _bounce_script = Path(f"./_metasmith/.bounce.{k}")
        # Whoever set up this cwd may or may not have made the internals dir: the
        # relay bootstrap does (it deploys the relay there), the relay-free
        # bootstrap and direct-run do not. Owning it here means the arm works the
        # same under every runtime instead of each caller remembering.
        _bounce_script.parent.mkdir(parents=True, exist_ok=True)
        exit_codef = Path(f"exitcode.{GenerateId()}")
        with open(_bounce_script, "w") as f:
            script = [
                f"cd {env.container.workdir}",
                "on_exit() {",
                f"    echo $? > {exit_codef}",
                "}",
                "trap on_exit EXIT",
                "set -e",
                # Exports ride in the bounce script rather than a per-runtime
                # flag (`-e` / `--env` / nothing), so one mechanism serves every
                # runtime and the tool sees its own vocabulary either way.
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
        try:
            with open(exit_codef) as f:
                exit_code = f.readline().strip()
                exit_code = int(exit_code)
        except:
            exit_code = 1
        msg = f"<- container exit [{exit_code}] <-"
        Log.Info(msg+"-"*(BREAK_LENGTH-len(msg)))
        if exit_codef.exists(): exit_codef.unlink()
        if exit_code != 0:
            Log.Error("a non-zero exit code ocurred while running script in container")
            time.sleep(5)
            sys.exit(exit_code)
        # sresult = self.external_shell.Exec(_container_start, timeout=None, history=history)
        # eresult = self.external_shell.Exec("[ -n $APPTAINER_CONTAINER ] || [ -e /.dockerenv ] && exit", timeout=None, history=history)
        return result
