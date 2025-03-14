import os
from pathlib import Path
import time
import shutil
import yaml
import traceback

from .logging import Log
from .agents import Agent, AgentPaths
from .models.libraries import ExecutionContext, ExecutionResult
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
        
        with PausedStdOut():
            res = shell.Exec("pwd -P", history=True)
        external_cwd = Path(res.out[0])
        Log.Info(f"external cwd [{external_cwd}]")
        Log.Info(f"loading agent config")
        agent = Agent.Load(AgentPaths.to_definition())
        task_key = workspace.name
        task_path = AgentPaths.to_task(task_key)
        Log.Info(f"loading task from [{task_path}]")
        task = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])

        step = task.plan.steps[step_index-1]
        step_name = f"{step.transform.name}:{step.transform.GetKey()}"
        Log.Info(f"step {step_index:02} [{step_name}]")
        Log.Info("uses:")

        # need to rectify paths
        # - for previous step outputs
                # 2025-03-14_00-40-05  |     ✓ [metagenomics::oci_image_diamond] at [container.diamond.oci.uri -> {home}/data/zHXmpWcrgYaH/container.diamond.oci.uri]
                # 2025-03-14_00-40-05  |     X [metagenomics::orfs_faa] at [orfs.faa -> /msm_home/runs/dwfuH8Cz/nxf_work/19/2272310d5eba7481fe8724313a77b6/orfs.faa]
                # 2025-03-14_00-40-05  |     ✓ [metagenomics::protein_reference_diamond] at [reference.uniprot_sprot.dmnd -> {home}/data/zHXmpWcrgYaH/reference.uniprot_sprot.dmnd]
        # - for containers

        def _external_exists(p: Path):
            FLAG = "exists123"
            with PausedStdOut():
                res = shell.Exec(f"[ -e {p} ] && echo {FLAG}", history=True)
                return FLAG in res.out
        def _status(p: Path, external=True):
            exists = _external_exists(p) if external else p.exists()
            return "✓" if exists else "X"
        inputs = {}
        for inst in step.uses:
            p = inst.path
            if p.is_symlink():
                p_info = f"{p} -> {p.readlink()}".replace(str(agent.home.GetPath()), "{home}")
            else:
                p_info = f"{p}"
            Log.Info(f"    {_status(p)} [{inst.dtype_name}] at [{p_info}]")
            inputs[inst.dtype] = p
        Log.Info("produces:")
        outputs = {}
        for inst in step.produces:
            outputs[inst.dtype] = inst.path
            Log.Info(f"    [{inst.dtype_name}] at [{inst.path}]")

        context = ExecutionContext(
            inputs=inputs,
            outputs=outputs,
            external_shell=shell,
        )
        Log.Info(f">>> executing protocol")
        Log.Info(">"*30)
        try:
            result = step.transform.protocol(context)
        except Exception as e:
            Log.Info("<"*30)
            Log.Info(f"<<< [{step_name}] failed with error")
            Log.Error(f"error while executing transform [{step_name}]")
            Log.Error(str(e))
            with open("traceback.temp", "w") as f:
                traceback.print_tb(e.__traceback__, file=f)
            with open("traceback.temp", "r") as f:
                Log.Error(f.read()[:-1])
            return ExecutionResult(False)
        
        Log.Info("<"*30)
        Log.Info(f"<<< [{step_name}] reports {'success' if result.success else 'failure'}")
        Log.Info(f"expected outputs:")
        for inst in step.produces:
            Log.Info(f"    {_status(inst.path, external=False)} [{inst.dtype_name}] at [{inst.path}]")

