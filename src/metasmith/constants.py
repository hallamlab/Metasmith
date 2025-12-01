import os
from pathlib import Path

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
