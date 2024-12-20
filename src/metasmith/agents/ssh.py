from pathlib import Path
from dataclasses import dataclass
import yaml

from ..coms import LiveShell
from ..logging import Log

# dev
_config = "/home/tony/workspace/tools/Metasmith/main/agent_setup.ssh/sockeye.yml"

CONTAINER = "docker://quay.io/hallamlab/metasmith:latest"
CONTAINER_FILE = "metasmith.sif"

@dataclass
class DeploymentConfig:
    ssh_command: str
    timeout: int
    lib: str
    workspace: str
    pre: str = ""
    post: str = ""
    container: str = CONTAINER

    @classmethod
    def Parse(cls, file_path):
        try:
            with open(file_path, "r") as f:
                config = cls(**yaml.safe_load(f))
            return config
        except TypeError as e:
            emsg = str(e)
            emsg = emsg.replace("DeploymentConfig.__init__()", "").strip()
            to_replace = {
                "missing 1 required positional argument:": "missing field",
                "got an unexpected keyword argument": "unknown field",
                "'": "[",
                "'": "]",
            }
            for k, v in to_replace.items():
                emsg = emsg.replace(k, v, 1)
            Log.Error(f"error with config [{file_path}]")
            Log.Error(emsg)

def Deploy():
    config: DeploymentConfig = DeploymentConfig.Parse(_config)
    if config is None: return
    Log.Info(config)

    with LiveShell(silent=False) as shell:
        shell.Exec(config.ssh_command, timeout=config.timeout)
        shell.Exec(
            f"""\
            {config.pre}
            [ -d {config.lib} ] || mkdir -p {config.lib}
            cd {config.lib}
            [ -f {CONTAINER_FILE} ] || apptainer pull {CONTAINER_FILE} {config.container}
            """,
            timeout=None,
        )
        shell.Exec(
            f"""\
            cd {config.lib}
            apptainer run {CONTAINER_FILE} metasmith api container_unpack
            """,
            timeout=None,
        )

        shell.Exec("exit")

def UnpackContainer():
    pass

def StartContainerService():
    pass
