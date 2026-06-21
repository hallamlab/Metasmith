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
from .env import Environment

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
    ordered_input_deps = list(step.transform.model.requires)
    for batch, batch_lineage in enumerate(lineages):
        if len(lineages)>1:
            Log.Info(f"  > batch [{batch+1}]:")
        g: dict[Dependency, ContextData] = {}
        file_groups = batch_lineage['FILES']
        for dep, file_names in zip(ordered_input_deps, file_groups):
            insts = input_by_dep.get(dep, [])
            if len(insts)==0:
                continue
            e = insts[0].dtype
            inst_names = {x.dtype_name for x in insts}
            Log.Info(f"    [{e.key} {'/'.join(inst_names)}] at:")
            input_group = [_parse(Path(p)) for p in file_names]
            for p in input_group:
                missing_input = missing_input or not p.local.exists()
                Log.Info(_shorten_home(f"        {_status(p)} [{_get_formatted_size(p.local)}] [{p.local}]"))
            g[dep] = ContextData(
                input_group=input_group,
                endpoint=e,
                type_name=insts[0].dtype_name,
            )
        inputs.append(g)
    if missing_input:
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
            lin = lineages[batch]
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

    context = ExecutionContext(
        _inputs=inputs,
        _get_output_paths=_get_output_paths,
        external_shell=shell,
        external_cwd=external_cwd,
        external_agent_home=Path(agent_home),
        container_runtime=agent.runtime,
        params=params,
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


def StageAndRunTransform(workspace: Path, step_index: int, host: str):
    Log.Info(f"cwd [{os.getcwd()}]")
    server_path = AgentPaths.to_local_relay_coms(root=AgentPaths.INTERNALS, host=host)
    MAX_WAIT = 3
    for i in range(MAX_WAIT):
        if server_path.exists(): break
        Log.Warn(f"waiting {i+1} of {MAX_WAIT} for relay to start")
        time.sleep(1)
    assert server_path.exists(), f"server not started [{server_path}]"

    Log.Info(f"loading agent config")
    agent = Agent.Load(AgentPaths.to_definition())
    agent_home = str(agent.home.GetPath())
    Log.Info(f"agent home [{agent_home}]")

    Log.Info(f"connecting to relay [{server_path}]")
    agent_env = Environment(image=agent.container, runtime=agent.runtime)
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
        task_path = AgentPaths.to_task(task_key)
        Log.Info(f"loading task from [{task_path}]")
        task = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])

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
        except Exception as e:
            Log.Error(f"failed to read [{METADATA_FILE}]: {e}")
        lineages = raw_meta.get("lin", "[]")
        lineages = json.loads(lineages)
        if not isinstance(lineages, list): lineages = [lineages]
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
                if len(resolved) == 0:
                    resolved = step.dependency_map.get(dep, [])
                input_by_dep[dep] = resolved
        else:
            inp_keys = [x for x in raw_meta.get("inp", "").split(",") if len(x)>0]
            for dep, k in zip(step.transform.model.requires, inp_keys):
                resolved = [x for x in step.dependency_map.get(dep, []) if x.dtype.key == k]
                if len(resolved) == 0:
                    resolved = step.dependency_map.get(dep, [])
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
        )
