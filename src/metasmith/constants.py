from pathlib import Path

NAME = Path(__file__).parent.name.lower()
USER = "hallamlab" # github id
GIT_URL = f"https://github.com/{USER}/{NAME}"
SHORT_SUMMARY = "Automated generation of workflows for Nextflow executed using agents"

_cli_call = "metasmith.coms.cli:main"
ENTRY_POINTS = [
    f"metasmith={_cli_call}",
    f"msm={_cli_call}",
]

with open(Path(__file__).parent/"version.txt") as f:
    VERSION = f.read().strip()
