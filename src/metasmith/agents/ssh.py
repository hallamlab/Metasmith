from pathlib import Path
from dataclasses import dataclass, field
import yaml

from ..coms.ipc import LiveShell
from ..logging import Log

CONTAINER = "docker://quay.io/hallamlab/metasmith:latest"
CONTAINER_FILE = "metasmith.sif"

@dataclass
class DeploymentConfig:
    ssh_command: str
    timeout: int
    workspace: str
    pre: str = ""
    post: str = ""
    container: str = CONTAINER
    container_args: list[str] = field(default_factory=list)

    @classmethod
    def Parse(cls, file_path):
        try:
            with open(file_path, "r") as f:
                config = cls(**yaml.safe_load(f))
            return config
        except FileNotFoundError as e:
            Log.Error(f"config file [{file_path}] not found")
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

def Deploy(config_path: Path):
    config = DeploymentConfig.Parse(config_path)
    if config is None: 
        Log.Error("config failed to parse")
        return
    workspace = Path(config.workspace).absolute()

    with LiveShell() as shell:
        SILENT = False
        internal = workspace/"internal"
        containers = workspace/"containers"
        shell.Exec(config.ssh_command, timeout=config.timeout, silent=SILENT)
        shell.Exec(config.pre, timeout=None, silent=SILENT)
        shell.Exec(
            f"""\
            {config.pre}
            [ -d {internal} ] || mkdir -p {internal}
            [ -d {containers} ] || mkdir -p {containers}
            cd {containers}
            [ -f {CONTAINER_FILE} ] || apptainer pull {CONTAINER_FILE} {config.container}
            """,
            timeout=None, silent=SILENT
        )
        shell.Exec(
            f"""\
            cd {workspace}
            apptainer run {" ".join(config.container_args)} {containers/CONTAINER_FILE} metasmith api unpack_container
            nohup {internal}/relay --io {internal}/connections &
            """,
            timeout=None, silent=SILENT
        )
        shell.Exec(config.post, timeout=None, silent=SILENT)
        shell.Exec("exit")

def UnpackContainer():
    SILENT = False
    Log.Warn("pretend to unpack relay")
    # with LiveShell() as shell:
    #     shell.Exec(
    #         f"""\
    #         pwd
    #         ls
    #         cd ..
    #         ls
    #         """,
    #         timeout=None, silent=SILENT
    #     )

def RunWorkflow():
    Log.Warn("pretend to run job")
    # with LiveShell() as shell:
    #     shell.Exec(
    #         f"""\
    #         pwd
    #         ls
    #         cd ..
    #         ls
    #         """,
    #         timeout=None, silent=SILENT
    #    )

