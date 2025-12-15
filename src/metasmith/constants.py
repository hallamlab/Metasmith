import os
from pathlib import Path
import socket

MODULE_PATH = Path(os.path.realpath(__file__)).parent
NAME = MODULE_PATH.name.lower()
USER = "hallamlab" # github id
GIT_URL = f"https://github.com/{USER}/{NAME}"
SHORT_SUMMARY = "Automated generation of workflows for Nextflow executed using agents"

_cli_call = "metasmith.coms.cli:main"
ENTRY_POINTS = [
    f"metasmith={_cli_call}",
    f"msm={_cli_call}",
]

with open(MODULE_PATH/"version.txt") as f:
    VERSION = f.read().strip()

class AgentPaths:
    WORK_ROOT = Path("/ws")
    HOME_ROOT = Path("/msm_home")
    CONTAINER_CACHE = Path("container_images")
    INTERNALS = Path("_metasmith")
    STAGED = Path("runs")
    TASK = Path("task")
    MAIN_LOG_FILE = "main.log"
    LAUNCHER_FILE = "start.sh"
    NXF_WORKFLOW = "workflow.nf"
    NXF_CONFIG = "workflow.config.nf"
    NXF_PARAMS = "workflow.params.yml"

    @classmethod
    def to_staged(cls, root: Path|None=None):
        if root is None: root = cls.HOME_ROOT
        return root/cls.STAGED

    @classmethod
    def to_task(cls, key: str, root: Path|None=None):
        if root is None: root = cls.HOME_ROOT
        return root/(cls.STAGED/key)/cls.INTERNALS/cls.TASK

    @classmethod
    def to_bootstrap(cls, root: Path|None=None):
        if root is None: root = cls.HOME_ROOT
        return root/"lib/msm_bootstrap"

    @classmethod
    def to_definition(cls, root: Path|None=None):
        if root is None: root = cls.HOME_ROOT
        return root/"lib/agent.yml"

    @classmethod
    def to_relay(cls, root: Path|None=None):
        if root is None: root = cls.HOME_ROOT
        return root/"relay/msm_relay"

    @classmethod
    def to_local_relay_coms(cls, root: Path|None=None, host: str|None=None):
        if not host:
            host = socket.gethostname()
        return cls.to_relay(root).parent/f"{host}"

    @classmethod
    def to_data(cls, root: Path|None=None):
        if root is None: root = cls.HOME_ROOT
        return root/"data"
