import os
from pathlib import Path
from pkgutil import extend_path
import time
import shutil
import yaml
import traceback

from ..logging import Log
from ..models.libraries import DataTypeLibrary, ExecutionContext, ExecutionResult, TransformInstance, TransformInstanceLibrary
from ..models.workflow import WorkflowTask
from ..coms.ipc import LiveShell, RemoteShell
from ..coms.containers import Container
from ..serialization import StdTime

# CONTAINER = Container("docker://quay.io/hallamlab/metasmith:latest")
# CONTAINER = Container("docker-daemon://quay.io/hallamlab/metasmith:0.2.dev-47c27e4")

def DeployFromContainer(workspace: Path):
    relay_server = Path("/opt/msm_relay")
    # deploy_root = workspace/".msm"
    deploy_root = workspace
    for p in [ # these are coupled to StageAndRunTransform() below
        "relay/connections",
        "lib",
    ]:
        (deploy_root/p).mkdir(parents=True, exist_ok=True)

    Log.Info("deploying relay server")
    relay_server_dest = deploy_root/"relay/msm_relay"
    if not relay_server_dest.exists():
        relay_server_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(relay_server, relay_server_dest)

    Log.Info("deploying nextflow executable")
    nxf_exec = Path("/opt/nextflow")
    nxf_exec_dest = deploy_root/"lib/nextflow"
    if not nxf_exec_dest.exists():
        nxf_exec_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(nxf_exec, nxf_exec_dest)

    Log.Info("deployment complete")

def StageAndRunTransform(workspace: Path, step_index: int):
    os.chdir(workspace)

    server_path = workspace/".msm/relay/connections/main.in"
    MAX_WAIT = 10
    for i in range(MAX_WAIT):
        if server_path.exists(): break
        Log.Warn(f"waiting {i+1} of {MAX_WAIT} for relay to start")
        time.sleep(1)
    assert server_path.exists(), f"server not started [{server_path}]"

    Log.Info("connecting to relay")
    with \
        RemoteShell(server_path) as shell, \
        open(workspace/"relay_shell_history.log", "w") as cmd_log:
        _paused = False
        def _make_listener(logger):
            def _listener(x: str):
                if _paused: return
                logger(x)
            return _listener
        class PausedStdOut:
            def __enter__(self):
                nonlocal _paused
                _paused = True
            def __exit__(self, *args):
                nonlocal _paused
                _paused = False
        
        shell.RegisterOnOut(_make_listener(Log.Info))
        shell.RegisterOnErr(_make_listener(Log.Error))
        
        Log.Info(f"cwd [{Path('.').resolve()}]")
        with PausedStdOut():
            res = shell.Exec("pwd -P", history=True)
        external_cwd = Path(res.out[0])
        Log.Info(f"external cwd [{external_cwd}]")

        local_meta_path = Path("./_metasmith")
        extern_meta_src = local_meta_path.readlink()
        if local_meta_path.is_symlink():
            Log.Info(f"staging task metadata")
            shell.Exec(f"rm {local_meta_path} && cp -r {extern_meta_src} {local_meta_path}")

        Log.Info(f"loading task metadata")
        try_get_task = lambda: WorkflowTask.Load(local_meta_path/"task")
        RETRY = 6
        for i in range(RETRY):
            to_wait = 2**i # total of 63 seconds
            try:
                task = try_get_task()
                break
            except:
                Log.Info(f"failed to load task metadata, retry [{i+1} of {RETRY}] in [{to_wait}] seconds")
                time.sleep(to_wait)
        task = try_get_task()

        step = task.plan.steps[step_index-1]
        step_name = f"{step.transform.name}:{step.transform.GetKey()}"
        Log.Info(f"step {step_index:02} [{step_name}]")
        Log.Info("uses:")
        for inst in step.uses:
            Log.Info(f"    {inst.dtype_name} at {inst.ResolvePath()}")
        Log.Info("produces:")
        for inst in step.produces:
            Log.Info(f"    {inst.dtype_name} at {inst.ResolvePath()}")

        context = ExecutionContext(
            inputs={inst.dtype: inst.ResolvePath() for inst in step.uses},
            outputs={inst.dtype: inst.ResolvePath() for inst in step.produces},
            shell=shell,
        )
        Log.Info(">"*30)
        Log.Info(f">>> executing protocol")
        try:
            result = step.transform.protocol(context)
        except Exception as e:
            Log.Info(f"<<< [{step_name}] failed with error")
            Log.Error(f"error while executing transform [{step_name}]")
            Log.Error(str(e))
            with open("traceback.temp", "w") as f:
                traceback.print_tb(e.__traceback__, file=f)
            with open("traceback.temp", "r") as f:
                Log.Error(f.read()[:-1])
            return ExecutionResult(False)
        Log.Info(f"<<< [{step_name}] reports {'success' if result.success else 'failure'}")
        Log.Info("<"*30)
