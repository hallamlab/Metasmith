from pathlib import Path
import time
import shutil
import traceback
import re
from glob import glob
import json
import os

from .logging import Log
from .agents import Agent, AgentPaths
from .models.libraries import ContextPath, ContextData, ExecutionContext, ExecutionResult
from .models.libraries import DataInstance, DataTypeLibrary, TransformInstance, TransformInstanceLibrary
from .models.solver import Dependency, Endpoint
from .models.workflow import WorkflowTask, METADATA_FILE
from .coms.via_file_watcher import RemoteShell

def DeployFromContainer(workspace: Path):
    deploy_root = workspace
    Log.Info(f"deploying to [{deploy_root}]")
    if not deploy_root.exists():
        deploy_root.mkdir(parents=True, exist_ok=True)
    folders = [
        "relay",
    ]
    for p in folders:
        (deploy_root/p).mkdir(parents=True, exist_ok=True)

    relay_server = Path("/opt/msm_relay")
    relay_server_dest = deploy_root/"relay/msm_relay"
    Log.Info(f"deploying relay server to [{relay_server_dest}]")
    if not relay_server_dest.exists():
        relay_server_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(relay_server, relay_server_dest)

    Log.Info("deployment complete")

def StageAndRunTransform(workspace: Path, variant_index: int, step_index: int):
    Log.Info(f"cwd [{os.getcwd()}]")
    server_path = AgentPaths.to_local_relay_coms(root=AgentPaths.INTERNALS)
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
    with RemoteShell(server_path, timeout=60) as shell:
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

        variant = task.plans[variant_index-1]       # is 1 indexed for log legibility
        archetype = variant[0]                    # all plans in variant have identical steps; take first as archetype
        step = archetype.steps[step_index-1]    # also 1 indexed for log legibility
        step_name = f"{step.transform.name}:{step.transform.GetKey()}"
        Log.Info(f"variant [{variant_index}] step [{step_index}:{step_name}]")

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
        output_indexes = ["_".join(f"{x}" for x in lin[group_by_inst.dtype.key]) for lin in lineages]

        input2files: dict[DataInstance, list[Path]] = {}
        for i, inst in enumerate(step.uses):
            k = f"i{i+1:02}"
            if k not in raw_meta: continue
            # The lookbehind `(?<!...)` asserts that the pattern inside
            # does not precede the current position.
            file_group: list[str] = re.split(r"(?<!\\)\s", raw_meta[k])
            input2files[inst] = [Path(re.sub(r"\\\s", " ", f)) for f in file_group]
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
        data2dep = {inst:dep for dep, inst in step.dependency_map.items()}
        missing_input=False
        for batch, batch_lineage in enumerate(lineages):
            if len(lineages)>1:
                Log.Info(f"  > batch [{batch+1}]:")
            g: dict[Dependency, ContextData] = {}
            for inst in step.uses:
                Log.Info(f"    [{inst.dtype_name}/{inst.dtype.key}] at:")
                remaining_files = input2files[inst]
                group_size = len(batch_lineage[inst.dtype.key])
                # Log.Debug(f"{inst.dtype_name} {group_size} {remaining_files}")
                input_group = [_parse_path(p) for p in remaining_files[:group_size]]
                # Log.Debug(f"{inst.dtype_name} {group_size} {[p.container for p in input_group]}")
                input2files[inst] = remaining_files[group_size:]
                for p in input_group:
                    missing_input = missing_input or not p.local.exists()
                    Log.Info(_shorten_home(f"        {_status(p)} [{p.local}]"))
                g[data2dep[inst]] = ContextData(
                    input_group=input_group,
                    endpoint=inst.dtype,
                    type_name=inst.dtype_name,
                )
            inputs.append(g)
        if missing_input:
            Log.Error("detected missing inputs, stopping")
            return ExecutionResult(False)

        output_signature = step.transform.output_signature
        def _get_output_paths(key: Dependency, i: int, batch: int):
            pattern = output_signature[key]
            dest = Path(f"{output_indexes[batch]}-{i+1}.{pattern}")
            return _parse_path(dest, container_override=Path("/ws")/dest)

        context = ExecutionContext(
            _inputs=inputs,
            _get_output_paths=_get_output_paths,
            external_shell=shell,
            external_cwd=external_cwd,
            container_runtime=agent.runtime,
            params=params,
        )
        Log.Info(f">>> executing protocol")
        BREAK_LENGTH = 60
        Log.Info(">"*BREAK_LENGTH)
        
        def on_exit(result: ExecutionResult, message: str|None=None):
            Log.Info("<"*BREAK_LENGTH)
            Log.Info(f"<<< [{step_name}] {message}")
            if len(result.manifest)==0:
                Log.Warn(f"no registered outputs")
            output_signature = step.transform.output_signature
            renamed = {}
            for i, entry in enumerate(result.manifest):
                Log.Info(f"output [{i+1}] of [{len(result.manifest)}]")
                to_rename = []
                ok = True
                for dep, path in output_signature.items():
                    inst = step.dependency_map[dep]
                    dep_desc = f"{inst.dtype_name}:{inst.dtype.key}"
                    if dep not in entry:
                        Log.Warn(f"    X [{dep_desc}]")
                        ok = False
                        continue
                    p = entry[dep]
                    if not p.exists():
                        Log.Warn(f"    X [{dep_desc}] from [{_shorten_home(str(p))}]")
                        ok = False
                        continue
                    new_name = Path(f"{output_indexes[0]}-{i+1}.{path}") # todo
                    to_rename.append((p, new_name))
                    Log.Info(f"    ✓ [{dep_desc}] at [{new_name}] from [{_shorten_home(str(p))}]")
                if not ok: continue
                for p, new in to_rename:
                    renamed[p] = new
                    p.rename(new)
        try:
            result = step.transform.protocol(context)
            if isinstance(result, list): result = result[0] # todo
            on_exit(result, f"reports {'success' if result.success else 'failure'}")
            if result.success: Path(".command.success").touch()
            return ExecutionResult(result.success)
        except Exception as e:
            on_exit(ExecutionResult(False), "failed with error")
            Log.Error(f"error while executing transform [{step_name}]")
            Log.Error(str(e))
            with open("traceback.temp", "w") as f:
                traceback.print_tb(e.__traceback__, file=f)
            with open("traceback.temp", "r") as f:
                Log.Error(f.read()[:-1])
            return ExecutionResult(False)
