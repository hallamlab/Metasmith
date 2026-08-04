from pathlib import Path
import time
import shutil
import traceback
import re
import json
import os

from .logging import Log
from .constants import AgentPaths
from .agents import Agent
from .models.libraries import ContextPath, ContextData, ExecutionContext, ExecutionResult
from .models.libraries import DataInstance, DataTypeLibrary, TransformInstance, TransformInstanceLibrary
from .models.paths import PathMap
from .models.solver import Dependency, Endpoint
from .hashing import KeyGenerator
from .models.workflow import WorkflowTask, METADATA_FILE, BIND_FILE
from .env import Environment, Rootfs
from .models.lineage import ArityMismatchError, LinPayload, MissingInstanceError
from .coms.via_file_watcher import RemoteShell

_RELAY_MIN_SIZE = 100_000
_ELF_MAGIC = b"\x7fELF"
_MACHO_MAGICS = (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf")

def _assert_real_relay(path: Path, architecture: str, system: str):
    """Client-side counterpart to dev.sh's `_assert_real_relays`.

    That check only ever runs against the maintainer's own build at publish
    time; this runs at deploy time against whatever image the consumer's
    docker/apptainer actually served -- including a stale locally-cached
    image the publish-time check never saw. Checked before the copy so a
    failure leaves the destination absent rather than a corrupt file that
    would look "already deployed" on every later call.
    """
    if not path.exists():
        raise AssertionError(
            f"relay binary not found in image at [{path}] for platform [{architecture}-{system}]"
        )
    size = path.stat().st_size
    with open(path, "rb") as f:
        magic = f.read(4)
    if system == "darwin":
        kind, ok_magic = "Mach-O", magic in _MACHO_MAGICS
    else:
        kind, ok_magic = "ELF", magic == _ELF_MAGIC
    if not (ok_magic and size > _RELAY_MIN_SIZE):
        raise AssertionError(
            f"relay binary at [{path}] looks like a stub or is corrupted "
            f"(expected {kind} magic, size>{_RELAY_MIN_SIZE}B; got "
            f"magic={magic.hex()} size={size}B). This means the metasmith "
            f"image itself is bad -- most likely a stale locally-cached "
            f"image tag. Try a fresh `docker pull <image>` / re-deploy, or "
            f"rebuild via dev.sh if this is a dev image."
        )

def DeployFromContainer(workspace: Path, architecture: str, system: str):
    deploy_root = workspace
    Log.Info(f"deploying to [{deploy_root}]")
    architecture = architecture.lower()
    system = system.lower()
    Log.Info(f"platform [{architecture}/{system}]")
    SUPPORTED_ARCHITECTURES = {"x86_64", "arm64"}
    SUPPORTED_SYSTEMS = {"linux", "darwin"}
    assert architecture in SUPPORTED_ARCHITECTURES, f"[{architecture}] not supported, valid architectures are [{SUPPORTED_ARCHITECTURES}]"
    assert system in SUPPORTED_SYSTEMS, f"[{system}] not supported, valid operating systems are [{SUPPORTED_SYSTEMS}]"

    if not deploy_root.exists():
        deploy_root.mkdir(parents=True, exist_ok=True)
    folders = [
        "relay",
    ]
    for p in folders:
        (deploy_root/p).mkdir(parents=True, exist_ok=True)

    relay_server = Path(f"/app/msm_relay.{architecture}-{system}")
    relay_server_dest = deploy_root/"relay/msm_relay"
    Log.Info(f"deploying relay server to [{relay_server_dest}]")
    if not relay_server_dest.exists():
        _assert_real_relay(relay_server, architecture, system)
        relay_server_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(relay_server, relay_server_dest)

    Log.Info("deployment complete")

def _parse_path(
    p: Path,
    *,
    agent_home: str,
    external_cwd: Path,
    task_key: str,
    container_override: Path | None = None,
) -> ContextPath:
    """Resolve a FILES-entry path into a (local, external, container) view.

    Thin shim over :meth:`metasmith.models.paths.PathMap.Parse` for
    backward-compatible callers. The path overhaul (commit 2 of the
    overhaul pair) centralises the four translation cases into
    ``PathMap.Parse``:

      1. ``/ws/<tail>`` absolute — Docker stringification of an upstream
         process output (inbox #139 fix shape).
      2. ``../ws/<tail>`` relative — apptainer-local stringification of
         the same logical file; routed identically to (1).
      3. Symlink whose target sits under ``HOME_ROOT`` — rerouted to
         ``extern_home/<tail>``.
      4. Symlink whose target sits outside ``HOME_ROOT`` — identity bind.

    Mirrors the inverse rewrite at bin/sbatch:54-80.

    ``external_cwd`` is accepted for signature compatibility but is no
    longer load-bearing — the relative ``../ws/`` case is anchored
    against the path map's ``extern_work``, not against an external cwd
    that callers may pass inconsistently.
    """
    _ = external_cwd  # legacy parameter; PathMap derives anchoring itself
    path_map = PathMap(extern_home=Path(agent_home), task_key=task_key)
    return path_map.Parse(p, container_override=container_override)


def _as_home_rooted(path_map: PathMap, p: Path) -> Path | None:
    """The HOME_ROOT spelling of `p`, if `p` is the host spelling of it.

    Returns None whenever the question does not arise: a path already under
    HOME_ROOT, a path outside the agent home entirely (a legitimate
    identity-bound foreign input), the relay-free arm where the two roots are
    the same directory, or the host-local (`metasmith run`) arm where there
    is no container and the agent home may just be the user's cwd -- pointing
    them at a `/msm_home` spelling there would be advice to introduce the
    very bug this message exists to name.
    """
    if not p.is_absolute() or path_map.host_local:
        return None
    if p.is_relative_to(AgentPaths.HOME_ROOT):
        return None
    if path_map.extern_home == AgentPaths.HOME_ROOT:
        return None
    if not p.is_relative_to(path_map.extern_home):
        return None
    return path_map.ExternalToLocal(p)


def ExecuteStep(
    step,
    agent,
    shell,
    external_cwd: Path,
    task_key: str,
    lineages: list,
    input_by_dep: dict,
    dep2output: list,
    params: dict,
    host_local: bool = False,
    slot_channels: dict[str, str]|None = None,
) -> ExecutionResult:
    """Run a single workflow step's protocol against pre-bound inputs.

    Both the Nextflow path (StageAndRunTransform) and the direct-run path
    (models.direct_run.RunTransform) go through here once they have a step,
    a shell, and the bindings the protocol needs.
    """
    from .models.workflow import WorkflowStep
    assert isinstance(step, WorkflowStep)

    agent_home = str(agent.home.GetPath())
    # Carry the step's host cwd in the PathMap so `ContextPath.ForOutput`
    # can resolve a bare output filename to the correct per-step host
    # location (deeper than `extern_work` by the nxf_work/<hash>/ tail).
    path_map = PathMap(
        extern_home=Path(agent_home),
        task_key=task_key,
        extern_cwd=external_cwd,
        host_local=host_local,
    )
    def _shorten_home(s: str):
        # Log-line shortener: replace the host-side agent_home in a
        # composed log string with the `{agent_home}` placeholder. This
        # is a log-only convenience — production path translation goes
        # through `path_map.Render` or `PathMap.Parse`, not str.replace.
        return s.replace(agent_home, "{agent_home}")

    step_name = f"{step.transform.name}:{step.transform.GetKey()}"
    alldep2output = {d: e for x in dep2output for d, e in x.items()}

    def _status(p: ContextPath):
        return "✓" if p.local.exists() else "X"
    def _parse(p: Path, container_override=None):
        return path_map.Parse(p, container_override=container_override)
    def _get_formatted_size(p: Path):
        if not p.exists():
            return "/"
        try:
            size_bytes = p.stat().st_size
            for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
                if size_bytes < 1024.0:
                    return f"{size_bytes:0.2f} {unit}"
                size_bytes /= 1024.0
            return f"{size_bytes:0.2f} PB" # Fallback for Petabytes
        except:
            return "/"
    inputs: list[dict[Dependency, ContextData]] = []
    Log.Info("uses:")
    missing_input=False
    # Every address on the FILES manifest is read here, inside the bootstrap
    # container, whose mounts are the work dir, the agent home at HOME_ROOT,
    # and whatever `.command.binds` declares. A missing input spelled as a
    # HOST path under the agent home is worth a second line of output: the
    # same file has a HOME_ROOT spelling, and if that one resolves then the
    # file was never missing -- an address reached the manifest in the wrong
    # coordinate system. Collected only for paths that already failed, so it
    # costs nothing when inputs are fine. The message states the alternative
    # rather than blaming a producer, because the SLURM arm reaches the host
    # spelling legitimately: `bin/sbatch` rewrites HOME_ROOT outward before
    # submitting. On the relay-free arm the two roots are one directory and
    # the branch is unreachable by construction.
    miscoordinated: list[tuple[Path, Path]] = []
    ordered_input_deps = list(step.transform.model.requires)
    for batch, batch_lineage in enumerate(lineages):
        if len(lineages)>1:
            Log.Info(f"  > batch [{batch+1}]:")
        g: dict[Dependency, ContextData] = {}
        # C5 — derive a dep-keyed file map up front instead of relying on
        # the implicit positional zip between FILES and ordered_input_deps.
        # The wire still emits FILES positionally (Nextflow input: directive
        # ordering matches model.requires), but routing now reads dep.key
        # explicitly so misalignment shows up at the construction site.
        file_groups: list = batch_lineage.get(LinPayload.FILES_KEY, [])
        files_by_dep_key: dict[str, list] = {
            dep.key: list(fnames)
            for dep, fnames in zip(ordered_input_deps, file_groups)
        }
        # PROV rides the same positional shape as FILES and is built from one
        # value in one closure with it, so the same zip routes it. Absent for a
        # runtime that does not emit it (virtual, direct_run, the harness) and
        # for any step with no inputs -- absent means "not captured", never
        # "no ancestors".
        prov_groups: list = batch_lineage.get(LinPayload.PROV_KEY, [])
        prov_by_dep_key: dict[str, list] = {
            dep.key: list(maps)
            for dep, maps in zip(ordered_input_deps, prov_groups)
        }
        for dep in ordered_input_deps:
            insts = input_by_dep.get(dep, [])
            if len(insts)==0:
                continue
            e = insts[0].dtype
            inst_names = {x.dtype_name for x in insts}
            Log.Info(f"    [{e.key} {'/'.join(inst_names)}] at:")
            file_names = files_by_dep_key.get(dep.key, [])
            input_group = [_parse(Path(p)) for p in file_names]
            for p in input_group:
                if not p.local.exists():
                    missing_input = True
                    alt = _as_home_rooted(path_map, p.local)
                    if alt is not None:
                        miscoordinated.append((p.local, alt))
                Log.Info(_shorten_home(f"        {_status(p)} [{_get_formatted_size(p.local)}] [{p.local}]"))
            prov = prov_by_dep_key.get(dep.key, [])
            if prov and len(prov) != len(input_group):
                # Structural alignment gone wrong is worse than no alignment:
                # a mispaired provenance answers confidently and wrongly, which
                # is the failure this whole mechanism exists to remove. Drop it
                # and say so, rather than let a reordering upstream turn into a
                # silently mislabelled result.
                Log.Warn(
                    f"provenance arity [{len(prov)}] != input arity "
                    f"[{len(input_group)}] for dep [{dep.key}]; dropping "
                    "provenance for this slot rather than mispairing it"
                )
                prov = []
            g[dep] = ContextData(
                input_group=input_group,
                endpoint=e,
                type_name=insts[0].dtype_name,
                provenance=prov,
            )
        inputs.append(g)
    if missing_input:
        for host_view, home_view in miscoordinated:
            m = (
                f"input [{host_view}] is a HOST path under the agent home, "
                f"and [{home_view}] is the same file as this container sees "
                "it. If that one exists, the file was never missing -- an "
                "address reached the FILES manifest in host coordinates and "
                "this container has no mount for it."
            )
            Log.Error(m)
            Log.Info(m)
        m = "detected missing inputs, stopping"
        Log.Error(m)
        Log.Info(m)
        return ExecutionResult(False)

    _hashes = {}
    def _get_output_paths(key: Dependency, i: int, batch: int):
        found = False
        for branch, d2e in enumerate(dep2output):
            if key in d2e:
                dtype = d2e[key]
                found = True
                break
        assert found, f"[{key}] not found in [{dep2output}]"
        if batch not in _hashes:
            # PROV only, NOT LinPayload.lineage_index(): FILES *is* folded into
            # this hash today, and dropping it would rename every output file,
            # which re-mints every file_instance_id and severs the link between
            # existing shards and new runs. PROV's values are maps, so sorted()
            # below raises on them -- this exclusion is what keeps the wire
            # addition from failing every task in output naming.
            lin = {
                k: v for k, v in lineages[batch].items()
                if k != LinPayload.PROV_KEY
            }
            slin = {k:sorted(lin[k]) for k in sorted(lin.keys())}
            _, _hash = KeyGenerator.FromStr(json.dumps(slin), l=16)
            _hashes[batch] = _hash
        _hash = _hashes[batch]
        name = f"{batch+1}-{i+1}-{branch+1}.{_hash}-{dtype.key}{dtype.GetPreferredFileExtension()}"
        return ContextPath.ForOutput(name, path_map)

    if len(agent.setup_commands)>0:
        Log.Info("setup commands for external shell:")
        for line in agent.setup_commands:
            Log.Info(f"    {line}")

    # Two slots of one type are `==` AND share a `.key` -- both derive from the
    # property set -- so every dict in this area (including `din` and
    # `files_by_dep_key`) has already collapsed them. Count over the requires
    # LIST, which is the only place the duplication is still visible; counting
    # the collapsed dict would report one and the refusal below would never
    # fire.
    _slot_channels = slot_channels or {}
    slot_keys: dict[Dependency, str] = {}
    _chans: list[str] = []
    for dep in step.transform.model.requires:
        chan = _slot_channels.get(dep.key)
        if chan is None:
            continue
        slot_keys[dep] = chan
        _chans.append(chan)
    _seen: dict[str, int] = {}
    for chan in _chans:
        _seen[chan] = _seen.get(chan, 0) + 1
    ambiguous_slots = {c for c, n in _seen.items() if n > 1}

    # Lifted out of `params` before the context is built: `params` is the
    # protocol-visible dict, and a protocol has no more business branching on
    # the rootfs than on the runtime.
    _rootfs = params.get("rootfs") or getattr(agent, "rootfs", Rootfs.AUTO)
    params = {k: v for k, v in params.items() if k != "rootfs"}

    context = ExecutionContext(
        _inputs=inputs,
        _get_output_paths=_get_output_paths,
        external_shell=shell,
        external_cwd=external_cwd,
        external_agent_home=Path(agent_home),
        # The TOOL environment, not the agent's own: never native (whether
        # metasmith itself is containerized says nothing about the tool's
        # image), but it does carry the host's GPU flag configuration.
        # Precedence for rootfs falls out of absence: the staged step meta
        # names a mode only when this task overrode one, otherwise the agent's
        # own tendency stands.
        _environment=Environment(
            image="", runtime=agent.runtime, gpu_args=list(agent.gpu_args),
            rootfs=_rootfs,
        ),
        params=params,
        _slot_keys=slot_keys,
        _ambiguous_slots=ambiguous_slots,
    )
    BREAK_LENGTH = 60
    Log.Info(f">>> executing")
    Log.Info(f">>> protocol "+">"*BREAK_LENGTH)

    def on_exit(result: ExecutionResult, message: str|None=None):
        Log.Info(f"<<< protocol "+"<"*BREAK_LENGTH)
        Log.Info(f"<<< [{step_name}] {message}")
        empty = False
        if sum(len(x) for x in result.manifest)==0:
            Log.Warn(f"no registered outputs")
            result.manifest = [{}]
            empty = True
        seen_deps: set[Dependency] = set()
        for i, manifest in enumerate(result.manifest):
            if empty: break
            if len(manifest)>0:
                Log.Info(f"branch [{i+1}] of [{len(result.manifest)}]")
            for d, p in manifest.items():
                if not p.exists(): continue
                e = alldep2output[d]
                insts = step.dependency_map[d]
                inst_names = {x.dtype_name for x in insts}
                Log.Info(f"    ✓ [{_get_formatted_size(p)}] [{e.key} {'/'.join(inst_names)}] produced at [{_shorten_home(str(p))}]")
                seen_deps.add(d)
        missings = []
        for i, g in enumerate(step.transform.model.produces):
            mg = []
            seen = False
            for d in g:
                if d in seen_deps:
                    seen = True
                    continue
                insts = step.dependency_map.get(d, [])
                inst_names = {x.dtype_name for x in insts}
                iname = '/'.join(inst_names) if len(inst_names)>0 else "no expected instances"
                dmeta = context.Output(d)
                mg.append(f"    X branch [{i+1}] [{dmeta.local}] [{iname}]")
            if seen: missings.append(mg)
        if any(len(g)>0 for g in missings):
            Log.Info(f"missing outputs:")
            for m in [m for g in missings for m in g]:
                Log.Info(m)
    try:
        results = step.transform.protocol(context)
        # An ExecWithEnv chain with no arm for this runtime runs nothing. Left
        # alone that is a step which reports success and produces no output --
        # the exact silent failure the arms exist to make impossible. The
        # transform author is not asked to remember; the framework checks.
        unmatched = context.UnmatchedEnvDispatches()
        if unmatched:
            runtime = agent.runtime.name
            declared = sorted({a for d in unmatched for a in d.declared})
            raise AssertionError(
                f"transform [{step_name}] reached [{len(unmatched)}] ExecWithEnv "
                f"declaration(s) with no arm for runtime [{runtime}]; "
                f"arms declared: {declared or ['<none>']}"
            )
        if not isinstance(results, list):
            results = [results]
        for i, result in enumerate(results):
            if len(results)>1:
                Log.Info(f"batch [{i+1}] of [{len(results)}]")
            on_exit(result, f"reports {'success' if result.success else 'failure'}")
        success = any(r.success for r in results)
        if success: Path(".command.success").touch()
        return ExecutionResult(success)
    except Exception as e:
        on_exit(ExecutionResult(False), "failed with error")
        Log.Error(f"error while executing transform [{step_name}]")
        Log.Error(str(e))
        with open("traceback.temp", "w") as f:
            traceback.print_tb(e.__traceback__, file=f)
        with open("traceback.temp", "r") as f:
            Log.Error(f.read()[:-1])
        return ExecutionResult(False)


def StageAndRunTransform(workspace: Path, step_index: int, host: str, stage_root: Path|None=None):
    Log.Info(f"cwd [{os.getcwd()}]")
    # Control-plane root: node-local stage when the host-side bootstrap staged a
    # copy into per-task scratch (SLURM array fan-out), else the shared HOME_ROOT
    # bind (local executor / staging disabled / staging failed → fail-open).
    cp_root = stage_root if stage_root is not None else AgentPaths.HOME_ROOT
    if stage_root is not None:
        Log.Info(f"reading control-plane from node-local stage [{stage_root}]")
    Log.Info(f"loading agent config")
    agent = Agent.Load(AgentPaths.to_definition(root=cp_root))
    agent_home = str(agent.home.GetPath())
    Log.Info(f"agent home [{agent_home}]")

    agent_env = Environment(image=agent.container, runtime=agent.runtime, native=agent.native)
    server_path = AgentPaths.to_local_relay_coms(root=AgentPaths.INTERNALS, host=host)
    if agent_env.needs_relay:
        # Container runtimes launch each tool across the boundary, so they
        # depend on the relay daemon the bootstrap started. mamba/native run
        # the tool in-process — there is no relay to wait on.
        MAX_WAIT = 3
        for i in range(MAX_WAIT):
            if server_path.exists(): break
            Log.Warn(f"waiting {i+1} of {MAX_WAIT} for relay to start")
            time.sleep(1)
        assert server_path.exists(), f"server not started [{server_path}]"

    Log.Info(f"connecting shell (relay={agent_env.needs_relay})")
    with agent_env.ConnectShell(server_path, agent.setup_commands) as shell:
        _paused = False
        class PausedStdOut:
            def __enter__(self):
                nonlocal _paused
                _paused = True
            def __exit__(self, *args):
                nonlocal _paused
                _paused = False
        
        def _make_listener(logger):
            def _listener(x: str):
                if _paused: return
                logger(x)
            return _listener
        shell.RegisterOnOut(_make_listener(Log.Info))
        shell.RegisterOnErr(_make_listener(Log.Error))

        with PausedStdOut():
            res = shell.Exec("pwd -P && sleep 1", history=True)
        assert len(res.out)==1, res
        external_cwd = Path(res.out[0])
        Log.Info(f"external cwd [{external_cwd}]")
        task_key = workspace.name
        task_path = AgentPaths.to_task(task_key, root=cp_root)
        Log.Info(f"loading task from [{task_path}]")
        task = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data(root=cp_root)])

        step = task.plan.steps[step_index-1]    # also 1 indexed for log legibility
        step_name = f"{step.transform.name}:{step.transform.GetKey()}"
        Log.Info(f"step [{step_index}:{step_name}]")

        params = {}
        raw_meta = {}
        try:
            with open(METADATA_FILE) as f:
                for l in f:
                    if l.endswith("\n"): l = l[:-1]
                    if len(l.strip()) == 0:
                        continue
                    if " " in l:
                        k, v = l.split(" ", maxsplit=1)
                    else:
                        k, v = l, ""
                    raw_meta[k] = v
                _vals = raw_meta.get("res", "").strip().split("/")
                for i, k in enumerate(["cpus", "memory", "attempt"]): # match nextflow task.{}
                    if i>=len(_vals): break
                    v = _vals[i]
                    if v.lower() == "null": continue
                    try:
                        vals = re.findall(r"\d+", v)
                        if len(vals)==0: continue
                        v = int(vals[0])
                    except ValueError:
                        continue
                    params[k] = v
                # The GPU declaration is static per step (it comes from the
                # transform's Resources, not a nextflow interpolation), so it
                # rides in via the staged step meta file. Absent for every
                # non-GPU step and for workspaces staged before GPU support.
                if "gpu" in raw_meta:
                    try:
                        params["gpus"] = json.loads(raw_meta["gpu"])
                    except json.JSONDecodeError as e:
                        Log.Warn(f"could not parse gpu metadata [{raw_meta['gpu']}]: {e}")
                # Same channel, same reason: the per-task rootfs override is
                # static per workspace, so it rides in with the staged step
                # meta. Absent for every workspace staged without one, which is
                # what leaves the agent's own tendency in charge.
                if raw_meta.get("rootfs"):
                    try:
                        params["rootfs"] = Rootfs.Parse(raw_meta["rootfs"])
                    except ValueError as e:
                        Log.Warn(f"ignoring unusable rootfs metadata: {e}")
        except Exception as e:
            Log.Error(f"failed to read [{METADATA_FILE}]: {e}")
        # Parse the lin payload via LinPayload.from_json. The v3 envelope
        # `{"v":3,"entries":[<index_map>, ...]}` carries one map per batch
        # member (`Orchestrator._collateBatch` builds one index each), so the
        # batch loop below gets one row per member and `context.AsBatch()`
        # yields that many. Wire v2 carried a single map, which is why a
        # batch_size>1 transform used to run once over member 0.
        lin_raw = raw_meta.get("lin")
        if lin_raw:
            try:
                lin_payload = LinPayload.from_json(lin_raw)
                lineages = lin_payload.entries
            except ValueError as e:
                Log.Error(f"failed to parse v{LinPayload.VERSION} lin payload: {e}")
                return ExecutionResult(False)
        else:
            lineages = [{}]
        group_by_inst = step.group_by_instances
        if len(group_by_inst)==0:
            Log.Error(f"group_by dependency has no bound instances for step [{step_name}]")
            return ExecutionResult(False)
        _dtypes = {x.dtype for x in group_by_inst}
        if len(_dtypes)>1:
            Log.Warn(f"unexpected plural group by [{group_by_inst}]")
        group_by_inst = group_by_inst[0]
        # output_indexes = ["#".join(f"{x}" for x in lin[group_by_inst.dtype.key]) for lin in lineages]
        inst_lookup: dict[str, DataInstance] = {}
        for insts in step.dependency_map.values():
            for inst in insts:
                for key in {inst.instance_id, inst._key, inst.legacy_key}:
                    inst_lookup[key] = inst

        fmt = int(raw_meta.get("fmt", "1"))
        dep_in_raw = {}
        dep_out_raw = []
        # C5 — greenfield sar/par preflight. Both are pre-written to
        # workflow.step_N.meta at workflow.py:1525-1526 and cat'd into
        # METADATA_FILE — no schema change needed at the emit site.
        # `sar` is `dict[dep_key, int]` (expected arity per dep); `par`
        # is the int sample-arity. We only assert on inputs — produced
        # slots may legitimately be empty (optional-branch produces, see
        # the `continue` at the dep2output construction below).
        sar: dict[str, int] = {}
        if "sar" in raw_meta:
            try:
                sar = json.loads(raw_meta["sar"])
            except json.JSONDecodeError:
                Log.Warn("failed to parse sar (structural arity); skipping preflight")
        # `slk` maps a slot to the on-channel name its stream carries. Written
        # by the compiler, which is the only side that knows -- see the note at
        # its emit site. Absent for a workspace staged before this existed, in
        # which case provenance simply is not answerable and SourceOf says so.
        slk: dict[str, str] = {}
        if "slk" in raw_meta:
            try:
                slk = json.loads(raw_meta["slk"])
            except json.JSONDecodeError:
                Log.Warn("failed to parse slk (slot channels); provenance unavailable")
        if fmt >= 2 and "din" in raw_meta and "dot" in raw_meta:
            try:
                dep_in_raw = json.loads(raw_meta["din"])
                dep_out_raw = json.loads(raw_meta["dot"])
            except json.JSONDecodeError:
                Log.Warn("failed to parse metadata v2 dependency payload, falling back to legacy inp/out")
                fmt = 1

        input_by_dep: dict[Dependency, list[DataInstance]] = {}
        if fmt >= 2:
            for dep in step.transform.model.requires:
                ids = dep_in_raw.get(dep.key, [])
                resolved = [inst_lookup[k] for k in ids if k in inst_lookup]
                if not resolved and ids:
                    # G4 — required input dep had non-empty id list but
                    # none resolved against inst_lookup. The silent
                    # fallback to step.dependency_map.get(dep, []) used
                    # to mask drift between the compile-time plan and
                    # the runtime channel; now it raises so the failure
                    # surfaces at the routing site.
                    raise MissingInstanceError(
                        instance_id=ids[0] if ids else None,
                        dep_key=dep.key,
                    )
                if dep.key in sar and len(resolved) != sar[dep.key]:
                    raise ArityMismatchError(
                        expected=sar[dep.key],
                        actual=len(resolved),
                        dep_key=dep.key,
                    )
                input_by_dep[dep] = resolved
        else:
            inp_keys = [x for x in raw_meta.get("inp", "").split(",") if len(x)>0]
            for dep, k in zip(step.transform.model.requires, inp_keys):
                resolved = [x for x in step.dependency_map.get(dep, []) if x.dtype.key == k]
                if not resolved:
                    # G4 (fmt<2 / legacy path) — same hard-raise once the
                    # compile-time plan has declared a key but no instance
                    # matches it.
                    raise MissingInstanceError(
                        instance_id=None,
                        dep_key=dep.key,
                    )
                input_by_dep[dep] = resolved
            for dep in step.transform.model.requires:
                if dep in input_by_dep:
                    continue
                input_by_dep[dep] = step.dependency_map.get(dep, [])

        dep2output: list[dict[Dependency, Endpoint]] = []
        if fmt >= 2:
            for i, dep_group in enumerate(step.transform.model.produces):
                dgroup = {}
                raw_group = dep_out_raw[i] if i < len(dep_out_raw) else {}
                for dep in dep_group:
                    ids = raw_group.get(dep.key, [])
                    resolved = [inst_lookup[k] for k in ids if k in inst_lookup]
                    if len(resolved) == 0:
                        resolved = step.dependency_map.get(dep, [])
                    if len(resolved)==0:
                        continue
                    dgroup[dep] = resolved[0].dtype
                dep2output.append(dgroup)
        else:
            out_groups = raw_meta.get("out", "").split(";") if "out" in raw_meta else []
            for graw, dep_group in zip(out_groups, step.transform.model.produces):
                dgroup = {}
                for k, dep in zip(graw.split(","), dep_group):
                    insts = [x for x in step.dependency_map.get(dep, []) if x.dtype.key == k]
                    if len(insts)==0:
                        insts = step.dependency_map.get(dep, [])
                    if len(insts)==0:
                        continue
                    dgroup[dep] = insts[0].dtype
                dep2output.append(dgroup)
        return ExecuteStep(
            step=step,
            agent=agent,
            shell=shell,
            external_cwd=external_cwd,
            task_key=task_key,
            lineages=lineages,
            input_by_dep=input_by_dep,
            dep2output=dep2output,
            params=params,
            # A mamba/native agent crosses no container boundary even under
            # nextflow, so the three path views must collapse exactly as they
            # do on the direct-run path.
            host_local=not agent_env.needs_relay,
            slot_channels=slk,
        )
