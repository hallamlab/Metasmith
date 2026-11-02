# `ecspr.bake` and `ecspr.model` must never import each other at module scope.
# They are installed into images with disjoint dependency stacks -- bake carries
# rdkit or torch, the measurement env carries scipy and cobra -- so an import
# across the seam does not degrade, it fails at load.
from pathlib import Path

_MODULE = Path(__file__).resolve().parent

NAME = "ecspr"
USER = "hallamlab"
SHORT_SUMMARY = ("Atom-resolved conductance measurement over metabolic GPR tables")
ENTRY_POINTS = [f"{NAME}={NAME}.cli:main"]


with open(_MODULE / "version.txt") as _f:
    VERSION = _f.read().strip()

_bh = _MODULE / "build_hash.txt"
BUILD_HASH = _bh.read_text().strip() if _bh.exists() else ""

# An unstamped tree says so rather than answering with the bare release segment:
# a package that claims `0.1.0` when nothing pinned which `0.1.0` it was built
# from makes a result untraceable, and the two are indistinguishable in a log.
# `dev/ecspr.sh -be` and `docker/ecspr/dev.sh --build` stamp before they read
# this, so the marker only appears on a route that skipped it -- or on the tree
# vendored as `buildlib::ecspr`, which is imported rather than installed.
FULL_VERSION = f"{VERSION}+{BUILD_HASH}" if BUILD_HASH else f"{VERSION}+unstamped"
__version__ = FULL_VERSION

CONTAINER_TAG = FULL_VERSION.replace("+", "-")
CONTAINER_IMAGE = f"quay.io/{USER}/{NAME}"
