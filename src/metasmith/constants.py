import os
from pathlib import Path
import socket

MODULE_PATH = Path(os.path.realpath(__file__)).parent
NAME = MODULE_PATH.name.lower()
USER = "hallamlab" # github id
GIT_URL = f"https://github.com/{USER}/{NAME}"
SHORT_SUMMARY = "Automated generation of workflows for Nextflow executed using agents"

# Where a release ends up, and where it is documented. The GUI links these from
# its header, so they live here rather than being retyped in the frontend.
DOCS_URL = f"https://{NAME}.readthedocs.io/en/latest/index.html"
CONDA_URL = f"https://anaconda.org/{USER}/{NAME}"
CONTAINER_URL = f"https://quay.io/repository/{USER}/{NAME}"

# The standard library of data types, transforms, and resources. Both `msm lab`
# and `msm gui` materialize this into the working directory under STDLIB_NAME;
# there is no configuration for it, so this is the single place it is pinned.
# STDLIB_NAME is the local directory name only and outlives the source it
# comes from: the standalone MetasmithLibraries repo is retired post-migration,
# so STDLIB_URL now points at the monorepo, and STDLIB_SPARSE_PATH is the
# subtree within it `clone_stdlib`'s live fallback checks out.
STDLIB_NAME = "MetasmithLibraries"
STDLIB_URL = GIT_URL
STDLIB_SPARSE_PATH = "src/metasmith_libraries"

_cli_call = "metasmith.coms.cli:main"
ENTRY_POINTS = [
    f"metasmith={_cli_call}",
    f"msm={_cli_call}",
]

# Public version (PEP 440 release segment). Bumped by hand when shipping.
with open(MODULE_PATH/"version.txt") as f:
    VERSION = f.read().strip()

# Build-time content hash of the source tree. Written by _build_hash.py
# during dev.sh / testing.docker_builder builds; absent in fresh dev
# checkouts (then degrades to bare VERSION).
_bh = MODULE_PATH/"build_hash.txt"
BUILD_HASH = _bh.read_text().strip() if _bh.exists() else ""

# Canonical version string — PEP 440 local form. Used for the wheel
# filename, __version__, and anywhere the exact build state matters.
FULL_VERSION = f"{VERSION}+{BUILD_HASH}" if BUILD_HASH else VERSION

# Container tag — FULL_VERSION rendered for Docker (rejects '+'). This is
# the single +→- translation site; all Docker-side consumers derive from
# CONTAINER_TAG, so dev.sh -ud and Agent.container stay in lockstep.
CONTAINER_TAG = FULL_VERSION.replace('+', '-')

class AgentPaths:
    # The task container's fixed internal layout, established by the bind
    # tuples Agent.Deploy writes. Shell text that will run *inside* a
    # container must interpolate these literals; they are not overridable.
    CONTAINER_WORK_ROOT = Path("/ws")
    CONTAINER_HOME_ROOT = Path("/msm_home")

    # The roots this process resolves agent paths against. Under a container
    # runtime they equal the literals above, because the agent home is
    # dual-bound at both. Under mamba/native nothing is mounted anywhere, so
    # the deployed `msm` / `msm_bootstrap` scripts export the real host paths
    # and every consumer follows without branching on the runtime.
    WORK_ROOT = Path(os.environ.get("METASMITH_WORK_ROOT") or CONTAINER_WORK_ROOT)
    HOME_ROOT = Path(os.environ.get("METASMITH_HOME_ROOT") or CONTAINER_HOME_ROOT)
    CONTAINER_CACHE = Path("container_images")
    INTERNALS = Path("_metasmith")
    STAGED = Path("runs")
    TASK = Path("task")
    MAIN_LOG_FILE = "main.log"
    LAUNCHER_FILE = "start.sh"
    NXF_WORKFLOW = "workflow.nf"
    NXF_CONFIG = "workflow.config.nf"
    NXF_RES = "workflow.resources.nf"
    NXF_PARAMS = "workflow.params.yml"
    # Per-step GPU requirement manifest, written at stage time and read by
    # RunWorkflow's preflight. Stage time knows what each transform asked for;
    # only run time knows what a device is on the target, so the two halves
    # meet through this file rather than in the emitted nextflow.
    GPU_MANIFEST = "workflow.gpu.json"
    # Per-step tool-environment portability, written at stage time and read by
    # RunWorkflow's preflight. Stage time knows which arms each transform
    # declared and which fields its env resource carries; only run time knows
    # what runtime the agent is. Same split, and same file-shaped seam, as the
    # GPU manifest above.
    ENV_MANIFEST = "workflow.env.json"
    # Nextflow's own `-with-trace` table, one row per task attempt. It is the
    # only per-step record that survives `rm -rf work/`, and the only one that
    # reports an exit code, so every consumer asking "which steps died" reads
    # this rather than scraping the log.
    NXF_TRACE_FILE = "nxf_trace.tsv"

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
