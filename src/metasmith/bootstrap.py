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
from .models.solver import Dependency, Endpoint
from .hashing import KeyGenerator
from .models.workflow import WorkflowTask, METADATA_FILE, BIND_FILE
from .coms.via_file_watcher import RemoteShell

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
    def _shorten_home(p: str):
        return p.replace(agent_home, "{agent_home}")
    
    Log.Info(f"connecting to relay [{server_path}]")
    with RemoteShell(server_path, timeout=60, setup_commands=agent.setup_commands) as shell:
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
            res = shell.Exec("pwd -P", history=True)
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
                    k = l[:len("###")]
                    raw_meta[k] = l[len("### "):]
                _vals = raw_meta["res"].strip().split("/")
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
        lineages = raw_meta["lin"]
        # (?=...) is look ahead
        # \g<0> is the matching group
        # lineage = re.sub(r"\w+(?=:)", r'"\g<0>"', lineage)
        lineages = json.loads(lineages)
        if not isinstance(lineages, list): lineages = [lineages]
        group_by_inst = step.dependency_map[step.transform.group_by]
        _dtypes = {x.dtype for x in group_by_inst}
        if len(_dtypes)>1:
            Log.Warn(f"unexpected plural group by [{group_by_inst}]")
        group_by_inst = group_by_inst[0]
        output_indexes = ["#".join(f"{x}" for x in lin[group_by_inst.dtype.key]) for lin in lineages]

        input_map: dict[Endpoint, list[DataInstance]] = {}
        for k in raw_meta["inp"].split(","):
            insts = [x for x in step.uses if x.dtype.key == k]
            input_map[insts[0].dtype] = insts
        input2dep: dict[Endpoint, Dependency] = {}
        for e, d in zip(input_map, step.transform.model.requires):
            input2dep[e] = d
        output_map: list[dict[Endpoint, list[DataInstance]]] = []
        dep2output: list[dict[Dependency, Endpoint]] = []
        for graw, inst_group, dep_group in zip(raw_meta["out"].split(";"), step.produces, step.transform.model.produces):
            group = {}
            dgroup = {}
            for k, dep in zip(graw.split(","), dep_group):
                insts = [x for x in inst_group if x.dtype.key == k]
                e = insts[0].dtype
                group[e] = insts
                dgroup[dep] = e
            output_map.append(group)
            dep2output.append(dgroup)
        alldep2output = {d:e for x in dep2output for d,e in x.items()}
        alloutput_map: dict[Endpoint, list[DataInstance]] = {}
        for x in output_map:
            for k, lst in x.items():
                alloutput_map[k] = alloutput_map.get(k, [])+lst

        input2files: dict[Endpoint, list[Path]] = {}
        for i, e in enumerate(input_map):
            k = f"i{i+1:02}"
            if k not in raw_meta: continue
            # The lookbehind `(?<!...)` asserts that the pattern inside
            # does not precede the current position.
            file_group: list[str] = re.split(r"(?<!\\)\s", raw_meta[k])
            input2files[e] = [Path(re.sub(r"\\\s", " ", f)) for f in file_group]
            # Log.Debug(f"{k} {inst.dtype_name} {input2files[inst]}")

        def _status(p: ContextPath):
            return "✓" if p.local.exists() else "X"
        def _parse_path(p: Path, container_override=None):
            if p.is_symlink():
                external = Path(str(p.readlink()).replace(str(AgentPaths.HOME_ROOT), agent_home))
                if external.is_relative_to(agent_home):
                    tail = external.relative_to(agent_home)
                    local = AgentPaths.HOME_ROOT/tail
                else:
                    local = p
            else:
                local = p
                external = external_cwd/p

            if container_override:
                container = container_override
            else:
                container = local
            return ContextPath(local=local, external=external, container=container)
        inputs: list[dict[Dependency, ContextData]] = []
        Log.Info("uses:")
        missing_input=False
        for batch, batch_lineage in enumerate(lineages):
            if len(lineages)>1:
                Log.Info(f"  > batch [{batch+1}]:")
            g: dict[Dependency, ContextData] = {}
            for e in input_map:
                insts = input_map[e]
                inst_names = {x.dtype_name for x in insts}
                Log.Info(f"    [{e.key} {'/'.join(inst_names)}] at:")
                remaining_files = input2files[e]
                group_size = len(batch_lineage[e.key])
                # Log.Debug(f"{inst.dtype_name} {group_size} {remaining_files}")
                input_group = [_parse_path(p) for p in remaining_files[:group_size]]
                # Log.Debug(f"{inst.dtype_name} {group_size} {[p.container for p in input_group]}")
                input2files[e] = remaining_files[group_size:]
                for p in input_group:
                    missing_input = missing_input or not p.local.exists()
                    Log.Info(_shorten_home(f"        {_status(p)} [{p.local}]"))
                g[input2dep[e]] = ContextData(
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

        kg = KeyGenerator()
        # output_signature = step.transform.output_signature
        def _get_output_paths(key: Dependency, i: int, batch: int):
            d2e = dep2output[batch]
            dtype = d2e[key]
            pattern = dtype.key
            dest = Path(f"{output_indexes[batch]}-{i+1}.{kg.GenerateUID(3)}.{pattern}{dtype.GetPreferredFileExtension()}")
            return _parse_path(dest, container_override=Path("/ws")/dest)

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
        Log.Info(f">>> executing protocol")
        BREAK_LENGTH = 60
        Log.Info(">"*BREAK_LENGTH)
        
        def on_exit(result: ExecutionResult, message: str|None=None):
            Log.Info("<"*BREAK_LENGTH)
            Log.Info(f"<<< [{step_name}] {message}")
            empty = False
            if sum(len(x) for x in result.manifest)==0:
                Log.Warn(f"no registered outputs")
                result.manifest = [{}]
                empty = True
            seen_deps: set[Dependency] = set()
            dep2branch = {}
            for i, manifest in enumerate(result.manifest):
                if empty: break
                Log.Info(f"branch [{i+1}] of [{len(result.manifest)}]")
                for d, p in manifest.items():
                    dep2branch[d] = i
                    e = alldep2output[d]
                    insts = alloutput_map[e]
                    inst_names = {x.dtype_name for x in insts}
                    Log.Info(f"    ✓ [{e.key} {'/'.join(inst_names)}] produced at [{_shorten_home(str(p))}]")
                    seen_deps.add(d)
            missings = []
            for g in step.transform.model.produces:
                for d in g:
                    if d in seen_deps: continue
                    e = alldep2output[d]
                    insts = alloutput_map[e]
                    inst_names = {x.dtype_name for x in insts}
                    missings.append(f"    X [{e.key} {'/'.join(inst_names)}]")
            if len(missings)>0:
                Log.Info(f"missing outputs:")
                for m in missings:
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
