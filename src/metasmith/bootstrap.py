from pathlib import Path
import time
import shutil
import traceback
import re

from .logging import Log
from .agents import Agent, AgentPaths
from .models.libraries import ContextPath, ContextData, ExecutionContext, ExecutionResult
from .models.libraries import DataInstance, DataTypeLibrary, TransformInstance, TransformInstanceLibrary
from .models.solver import Dependency, Endpoint
from .models.workflow import WorkflowTask
from .coms.terminals import LiveShell
from .coms.via_ws import RemoteShell
from .coms.containers import Container
from .serialization import StdTime

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

def StageAndRunTransform(workspace: Path, step_index: int):
    server_path = AgentPaths.to_local_relay_coms(root=AgentPaths.INTERNALS)
    MAX_WAIT = 3
    for i in range(MAX_WAIT):
        if server_path.exists(): break
        Log.Warn(f"waiting {i+1} of {MAX_WAIT} for relay to start")
        time.sleep(1)
    assert server_path.exists(), f"server not started [{server_path}]"

    Log.Info("connecting to relay")
    Log.Info(f"loading agent config")
    agent = Agent.Load(AgentPaths.to_definition())
    agent_home = str(agent.home.GetPath())
    Log.Info(f"agent home [{agent_home}]")
    def _shorten_home(p: str):
        return p.replace(agent_home, "{agent_home}")
    
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

        _i = step_index-1
        step = None
        for p in task.plans:
            if _i >= len(p.steps):
                _i -= len(p.steps)
                continue
            step = p.steps[_i]
            break
        assert step is not None, step_index
        step_name = f"{step.transform.name}:{step.transform.GetKey()}"
        Log.Info(f"step [{step_index}:{step_name}]")

        def _status(p: ContextPath):
            return "✓" if p.local.exists() else "X"
        container_binds = {}
        def _parse_meta(inst: DataInstance, container_override=None):
            p = inst.path
            if p.is_symlink():
                external = Path(str(p.readlink()).replace(str(AgentPaths.HOME_ROOT), agent_home))
                tail = external.relative_to(agent_home)
                local = AgentPaths.HOME_ROOT/tail
            else:
                local = p
                external = external_cwd/p
            if container_override:
                container = container_override
            else:
                k = external.parent
                if k not in container_binds:
                    container_binds[k] = Path(f"/msm_data/{k.name}")
                container = container_binds[k]/p

            return ContextData(
                path=ContextPath(local=local, external=external, container=container),
                endpoint=inst.dtype,
                type_name=inst.dtype_name,
            )
        inputs:dict[Dependency, ContextData] = {}
        Log.Info("uses:")
        data2dep = {i:d for d, i in step.dependency_map.items()}
        missing_input=False
        for inst in step.uses:
            meta = _parse_meta(inst)
            p = meta.path
            missing_input = missing_input or not p.local.exists()
            Log.Info(_shorten_home(f"    {_status(p)} [{inst.dtype_name}/{inst.dtype.key}] at [{p.external}]"))
            inputs[data2dep[inst]] = meta
        if missing_input:
            Log.Error("detected missing inputs, stopping")
            return ExecutionResult(False)

        # Log.Info("produces:")
        outputs: dict[Dependency, ContextData] = {}
        for inst in step.produces:
            meta = _parse_meta(inst, container_override=Path("/ws")/inst.path)
            p = meta.path
            outputs[data2dep[inst]] = meta
            # Log.Info(_shorten_home(f"    {space} [{inst.dtype_name}/{inst.dtype.key}] at [{p.external}]"))

        params = {}
        try:
            with open(".command.metadata") as f:
                _cpus, _mem = f.readline().strip().split("/")
                for k, v in [ # match nextflow task.{}
                    ("cpus", _cpus),
                    ("memory", _mem),
                ]:
                    if v.lower() == "null": continue
                    try:
                        vals = re.findall(r"\d+", v)
                        if len(vals)==0: continue
                        v = int(vals[0])
                    except ValueError:
                        continue
                    params[k] = v
        except Exception as e:
            Log.Error(f"failed to read .command.metadata: {e}")

        context = ExecutionContext(
            _inputs=inputs,
            _outputs=outputs,
            external_shell=shell,
            external_cwd=external_cwd,
            container_runtime=agent.runtime,
            params=params,
        )
        Log.Info(f">>> executing protocol")
        BREAK_LENGTH = 60
        Log.Info(">"*BREAK_LENGTH)
        
        def on_exit(sucess:bool, message: str|None=None):
            Log.Info("<"*BREAK_LENGTH)
            Log.Info(f"<<< [{step_name}] {message}")
            Log.Info(f"expected outputs:")
            for inst in step.produces:
                meta = _parse_meta(inst, container_override=Path("/ws")/inst.path)
                p = meta.path
                Log.Info(_shorten_home(f"    {_status(p)} [{inst.dtype_name}/{inst.dtype.key}] at [{p.external}]"))
            if not sucess:
                for k, v in outputs.items():
                    p = v.path.local
                    if not p.exists(): continue
                    p.rename(p.with_suffix(f"{p.suffix}.failed")) # ensure that nextflow sees failure, since expected outputs gone
        try:
            result = step.transform.protocol(context)
            on_exit(result.success, f"reports {'success' if result.success else 'failure'}")
            if result.success: Path("./.command.success").touch()
            return ExecutionResult(result.success)
        except Exception as e:
            on_exit(False, "failed with error")
            Log.Error(f"error while executing transform [{step_name}]")
            Log.Error(str(e))
            with open("traceback.temp", "w") as f:
                traceback.print_tb(e.__traceback__, file=f)
            with open("traceback.temp", "r") as f:
                Log.Error(f.read()[:-1])
            return ExecutionResult(False)
