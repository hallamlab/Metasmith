import os
from pathlib import Path
import time
import shutil
import yaml
import traceback

from metasmith.hashing import KeyGenerator

from .logging import Log
from .agents import Agent, AgentPaths
from .models.libraries import ContextPath, ExecutionContext, ExecutionResult
from .models.libraries import DataTypeLibrary, TransformInstance, TransformInstanceLibrary
from .models.workflow import WorkflowTask
from .coms.ipc import LiveShell, RemoteShell
from .coms.containers import Container
from .serialization import StdTime

# CONTAINER = Container("docker://quay.io/hallamlab/metasmith:latest")
# CONTAINER = Container("docker-daemon://quay.io/hallamlab/metasmith:0.2.dev-47c27e4")

def DeployFromContainer(workspace: Path):
    deploy_root = workspace
    Log.Info(f"deploying to [{deploy_root}]")
    if not deploy_root.exists():
        deploy_root.mkdir(parents=True, exist_ok=True)
    folders = [
        "relay/connections",
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
    server_path = AgentPaths.to_relay_coms(root=AgentPaths.INTERNALS)
    MAX_WAIT = 3
    for i in range(MAX_WAIT):
        if server_path.exists(): break
        Log.Warn(f"waiting {i+1} of {MAX_WAIT} for relay to start")
        time.sleep(1)
    assert server_path.exists(), f"server not started [{server_path}]"

    Log.Info("connecting to relay")
    with RemoteShell(server_path) as shell:
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
        
        Log.Info(f"loading agent config")
        agent = Agent.Load(AgentPaths.to_definition())
        agent_home = str(agent.home.GetPath())
        Log.Info(f"agent home [{agent_home}]")
        def _shorten_home(p: str):
            return p.replace(agent_home, "{agent_home}")

        with PausedStdOut():
            res = shell.Exec("pwd -P", history=True)
        external_cwd = Path(res.out[0])
        Log.Info(f"external cwd [{external_cwd}]")
        task_key = workspace.name
        task_path = AgentPaths.to_task(task_key)
        Log.Info(f"loading task from [{task_path}]")
        task = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
        step = task.plan.steps[step_index-1]
        step_name = f"{step.transform.name}:{step.transform.GetKey()}"
        Log.Info(f"step [{step_index}:{step_name}]")

        def _status(p: ContextPath):
            return "✓" if p.local.exists() else "X"
        container_binds = {}
        def _parse_path(p: Path):
            if p.is_symlink():
                external = Path(str(p.readlink()).replace(str(AgentPaths.HOME_ROOT), agent_home))
                tail = external.relative_to(agent_home)
                local = AgentPaths.HOME_ROOT/tail
            else:
                local = p
                external = external_cwd/p
            k = external.parent
            if k not in container_binds:
                container_binds[k] = Path(f"/msm_data/{k.name}")
            container = container_binds[k]/p
            return ContextPath(local=local, external=external, container=container)
        inputs = {}
        Log.Info("uses:")
        for inst in step.uses:
            p = _parse_path(inst.path)
            Log.Info(_shorten_home(f"    {_status(p)} [{inst.dtype_name}] at [{p.external}]"))
            inputs[inst.dtype] = p
        Log.Info("produces:")
        outputs = {}
        for inst in step.produces:
            p = _parse_path(inst.path)
            outputs[inst.dtype] = p
            Log.Info(_shorten_home(f"    [{inst.dtype_name}] at [{p.external}]"))

        context = ExecutionContext(
            _inputs=inputs,
            _outputs=outputs,
            external_shell=shell,
            external_cwd=external_cwd,
            container_runtime=task.container_runtime,
        )
        Log.Info(f">>> executing protocol")
        BREAK_LENGTH = 60
        Log.Info(">"*BREAK_LENGTH)
        try:
            result = step.transform.protocol(context)
        except Exception as e:
            Log.Info("<"*BREAK_LENGTH)
            Log.Info(f"<<< [{step_name}] failed with error")
            Log.Error(f"error while executing transform [{step_name}]")
            Log.Error(str(e))
            with open("traceback.temp", "w") as f:
                traceback.print_tb(e.__traceback__, file=f)
            with open("traceback.temp", "r") as f:
                Log.Error(f.read()[:-1])
            return ExecutionResult(False)
        
        Log.Info("<"*BREAK_LENGTH)
        Log.Info(f"<<< [{step_name}] reports {'success' if result.success else 'failure'}")
        Log.Info(f"expected outputs:")
        for inst in step.produces:
            Log.Info(_shorten_home(f"    {_status(p)} [{inst.dtype_name}] at [{p.external}]"))
