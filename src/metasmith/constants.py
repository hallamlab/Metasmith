from pathlib import Path

NAME = Path(__file__).parent.name
GIT_URL = "https://github.com/Tony-xy-Liu/Metasmith"
SHORT_SUMMARY = "Automated generation of workflows for Nextflow executed using agents"

ENTRY_POINTS = [
    "metasmith=metasmith.coms.cli:main",
    "ms=metasmith.coms.cli:main",
]

with open(Path(__file__).parent/"version.txt") as f:
    VERSION = f.read().strip()
