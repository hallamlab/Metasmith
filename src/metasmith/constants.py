import os
from pathlib import Path
import socket

MODULE_PATH = Path(os.path.realpath(__file__)).parent
NAME = MODULE_PATH.name.lower()
USER = "hallamlab"
GIT_URL = f"https://github.com/{USER}/{NAME}"
SHORT_SUMMARY = "Automated generation of workflows for Nextflow executed using agents"

DOCS_URL = f"https://{NAME}.readthedocs.io/en/latest/index.html"
CONDA_URL = f"https://anaconda.org/{USER}/{NAME}"
CONTAINER_URL = f"https://quay.io/repository/{USER}/{NAME}"

STDLIB_NAME = "MetasmithLibraries"

_cli_call = "metasmith.coms.cli:main"
ENTRY_POINTS = [
    f"metasmith={_cli_call}",
    f"msm={_cli_call}",
]

with open(MODULE_PATH/"version.txt") as f:
    VERSION = f.read().strip()

_bh = MODULE_PATH/"build_hash.txt"
BUILD_HASH = _bh.read_text().strip() if _bh.exists() else ""

FULL_VERSION = f"{VERSION}+{BUILD_HASH}" if BUILD_HASH else VERSION

CONTAINER_TAG = FULL_VERSION.replace('+', '-')

class AgentPaths:
    CONTAINER_WORK_ROOT = Path("/ws")
    CONTAINER_HOME_ROOT = Path("/msm_home")

    WORK_ROOT = Path(os.environ.get("METASMITH_WORK_ROOT") or CONTAINER_WORK_ROOT)
    HOME_ROOT = Path(os.environ.get("METASMITH_HOME_ROOT") or CONTAINER_HOME_ROOT)
    CONTAINER_CACHE = Path("container_images")
    CONDA_RECIPES = Path("env_recipes")
    INTERNALS = Path("_metasmith")
    STAGED = Path("runs")
    TASK = Path("task")
    MAIN_LOG_FILE = "main.log"
    LAUNCHER_FILE = "start.sh"
    NXF_WORKFLOW = "workflow.nf"
    NXF_CONFIG = "workflow.config.nf"
    NXF_RES = "workflow.resources.nf"
    NXF_PARAMS = "workflow.params.yml"
    GPU_MANIFEST = "workflow.gpu.json"
    # Where each cache-hit step's shard products have to be copied to, because
    # nextflow will not publish a path outside its own work directory.
    CACHE_PUBLISH_MANIFEST = "workflow.cache_publish.json"
    ENV_MANIFEST = "workflow.env.json"
    # Schema 1 recorded which of `container:` / `conda:` a resource carried; schema 2
    # records what each resolves to. Nothing branches on it -- it is here so a reader
    # of an old manifest can tell which shape they have.
    ENV_MANIFEST_SCHEMA = 2
    NXF_TRACE_FILE = "nxf_trace.tsv"

    # A run's identity and its process group, written by start.sh beside PID.lock.
    # PID.lock holds nextflow's pgid and is the cancel handle; RUN.pgid holds the
    # whole run's pgid and RUN.token the environment token every descendant
    # inherits, which together are the reap handle.
    PID_LOCK_FILE = "PID.lock"
    RUN_PGID_FILE = "RUN.pgid"
    RUN_TOKEN_FILE = "RUN.token"
    RUN_TOKEN_ENV = "METASMITH_RUN"
    # Docker label carrying RUN_TOKEN_ENV: `--rm` leaves no other handle on a
    # container whose client was killed.
    RUN_LABEL = "msm.run"

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
