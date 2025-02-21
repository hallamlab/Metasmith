import os
from pathlib import Path
from pkgutil import extend_path
import time
import shutil
import yaml
import traceback

from ..logging import Log
from ..models.libraries import DataTypeLibrary, ExecutionContext, ExecutionResult, TransformInstance, TransformInstanceLibrary
from ..coms.ipc import LiveShell, RemoteShell
from ..coms.containers import Container
from ..serialization import StdTime

# CONTAINER = Container("docker://quay.io/hallamlab/metasmith:latest")
# CONTAINER = Container("docker-daemon://quay.io/hallamlab/metasmith:0.2.dev-47c27e4")

def DeployFromContainer(workspace: Path):
    relay_server = Path("/opt/msm_relay")
    deploy_root = workspace/".msm"
    relay_server_dest = deploy_root/"relay/msm_relay"
    if not relay_server_dest.exists():
        relay_server_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(relay_server, relay_server_dest)
    for p in [ # these are coupled to StageAndRunTransform() below
        "relay/connections",
        "lib",
        "logs",
    ]:
        (deploy_root/p).mkdir(parents=True, exist_ok=True)
    Log.Info("deployment complete")

def StageAndRunTransform(workspace: Path, context_path: Path):
    os.chdir(workspace)

    server_path = workspace/".msm/relay/connections/main.in"
    MAX_WAIT = 10
    for i in range(MAX_WAIT):
        if server_path.exists(): break
        Log.Warn(f"waiting {i+1} of {MAX_WAIT} for relay to start")
        time.sleep(1)
    assert server_path.exists(), f"server not started [{server_path}]"

    logs_dir = workspace/".msm/logs"
    Log.Info("connecting to relay")
    with \
        RemoteShell(server_path) as shell, \
        open(logs_dir/"shell.cmd", "w") as cmd_log:
        def _make_listener(logger):
            def _listener(x: str):
                logger(x)
            return _listener
        
        shell.RegisterOnOut(_make_listener(Log.Info))
        shell.RegisterOnErr(_make_listener(Log.Error))
        def external_shell(cmd: str, timeout: int|float|None=None):
            cmd_log.write(f"{StdTime.Timestamp()} {'='*20}\n")
            cmd_log.write(f"{cmd}\n")
            return shell.Exec(cmd, timeout)
        
        Log.Info(f"cwd [{Path('.').resolve()}]")
        res = shell.Exec("pwd -P", history=True)
        external_cwd = Path(res.out[0])
        Log.Info(f"external cwd [{external_cwd}]")
        Log.Info(f"loading context [{context_path}]")
        with open(context_path) as f:
            context = ExecutionContext.Unpack(yaml.safe_load(f))
        Log.Info(f"transform key [{context.transform_key}]")
        Log.Info(f"external work [{context.work_dir}]")
        Log.Info("uses:")
        for inst in context.input:
            Log.Info(f"    {inst.source.address}: {inst.type}")
        Log.Info("produces:")
        for inst in context.output:
            Log.Info(f"    {inst.source.address}: {inst.type}")
        extern_lib_path = context.work_dir/'metasmith/transforms.lib'
        local_lib_path = Path(f".msm/{extern_lib_path.name}")
        Log.Info(f"staging transform library [{extern_lib_path}] to [{local_lib_path}]")
        external_shell(f"cp -r {extern_lib_path} {local_lib_path}")
        Log.Info("loading transform library")
        trlib: TransformInstanceLibrary = TransformInstanceLibrary.Load(local_lib_path)

        for x in context.input:
            nxf_path = Path(x.source.address)
            orig_path = nxf_path
            Log.Info(f"staging [{x.type}] from [{orig_path}]")
            # # todo: move from /agent_workspace/data
            # new_path = Path(f"./.msm/inputs/{nxf_path.name}")
            # external_shell(f"cp {orig_path} {new_path}")
            # x.source.address = str(new_path)

        Log.Info("getting transform")
        transform = trlib.GetByKey(context.transform_key)
        transform_name = Path(transform._source.address).stem

        context._shell = external_shell
        Log.Info(f">>> executing [{transform_name}] ")
        try:
            result = transform.protocol(context)
        except Exception as e:
            Log.Info(f"<<< [{transform_name}] failed with error")
            Log.Error(f"error while executing transform [{transform_name}]")
            Log.Error(str(e))
            with open("traceback.temp", "w") as f:
                traceback.print_tb(e.__traceback__, file=f)
            with open("traceback.temp", "r") as f:
                Log.Error(f.read()[:-1])
            return ExecutionResult(False)
        Log.Info(f"<<< [{transform_name}] reports {'success' if result.success else 'failure'}")
