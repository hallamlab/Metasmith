from pathlib import Path

_MODULE = Path(__file__).resolve().parent
with open(_MODULE / "version.txt") as _f:
    __version__ = _f.read().strip()

NAME = "fabfos"
USER = "hallamlab"
SHORT_SUMMARY = "A pipeline for the analysis of pooled fosmid data, run on metasmith"
ENTRY_POINTS = [f"{e}={NAME}.cli:main" for e in (NAME, "ffs")]
