import os
from pathlib import Path
import time
import trace
import yaml
import traceback

from ..logging import Log
from ..models.libraries import DataTypeLibrary, ExecutionContext, ExecutionResult, TransformInstance
from ..coms.ipc import LiveShell, RemoteShell
from ..coms.containers import Container
from ..serialization import StdTime

# CONTAINER = Container("docker://quay.io/hallamlab/metasmith:latest")
CONTAINER = Container("docker-daemon://quay.io/hallamlab/metasmith:0.2.dev-47c27e4")

def DeployFromContainer(workspace: Path):
    with LiveShell() as shell:
        shell.Exec(
            f"""\
            cd {workspace}
            mkdir .msm && cd .msm
            mkdir lib logs work inputs outputs
            mkdir -p relay/connections
            cp /app/relay ./relay/server
            """,
        )
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
        
        Log.Info("loading context")
        external_shell(f"rm -f {context_path} && cp {context_path.resolve()} ./")
        with open(context_path) as f:
            raw_context = yaml.safe_load(f)

        for lib_path in raw_context["type_libraries"]:
            lib_path = Path(lib_path)
            Log.Info(f"loading type library [{lib_path.resolve()}]")
            local_path = Path(f"./.msm/lib/{lib_path.name}")
            external_shell(f"cp {lib_path.resolve()} {local_path}")
            DataTypeLibrary.Load(local_path.resolve())

        context = ExecutionContext.Unpack(raw_context)
        Log.Info("staging transform definition")
        external_shell(f"cp {context.transform_definition.resolve()} ./.msm/lib/")

        input_map = {}
        with open("inputs.manifest") as f:
            for l in f:
                k, v = l.strip().split(",")
                input_map[k] = v
        for key, x in context.inputs.items():
            nxf_path = Path(input_map[x.type.name])
            orig_path = nxf_path.resolve()
            Log.Info(f"staging [{x.type.name}:{key}] from [{orig_path}]")
            new_path = Path(f"./.msm/inputs/{nxf_path.name}")
            external_shell(f"cp {orig_path} {new_path}")
            x.source = new_path

        Log.Info("loading transform")
        transform = TransformInstance.Load(workspace/f"./.msm/lib/{context.transform_definition.name}")
        
        context._shell = external_shell
        Log.Info(f">>> executing [{transform.source.stem}] ")
        try:
            result = transform.protocol(context)
        except Exception as e:
            Log.Info(f"<<< [{transform.source.stem}] failed with error")
            Log.Error(f"error while executing transform [{transform.source.stem}]")
            Log.Error(str(e))
            with open("traceback.temp", "w") as f:
                traceback.print_tb(e.__traceback__, file=f)
            with open("traceback.temp", "r") as f:
                Log.Error(f.read()[:-1])
            return ExecutionResult(False)
        Log.Info(f"<<< [{transform.source.stem}] reports {'success' if result.success else 'failure'}")
