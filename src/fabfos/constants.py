import os
from pathlib import Path

MODULE_PATH = Path(os.path.realpath(__file__)).parent
NAME = MODULE_PATH.name.lower()
USER = "hallamlab"
GIT_URL = f"https://github.com/{USER}/{NAME}"
SHORT_SUMMARY = "A pipeline for the analysis of pooled fosmid data, run on metasmith"

ENTRY_POINTS = [f"{e}={NAME}.cli:main" for e in (NAME, "ffs")]

with open(MODULE_PATH/"version.txt") as f:
    VERSION = f.read().strip()

REPO_ROOT = MODULE_PATH.parents[1]

#: The conda env the AGENT runs in under `--runtime mamba` (the one with
#: metasmith installed). Only the driver's `--agent-env` default.
AGENT_ENV = os.environ.get("FABFOS_AGENT_ENV")


class RefPaths:
    # Where a run finds its references, and how the three env names repoint it.
    #
    # `FABFOS_REFS_ROOT` moves the `processed/` root every reference default is
    # measured from, which is how a run reaches a bake other than this
    # checkout's -- prod against one, local development against another,
    # without editing a default or passing five paths. `FABFOS_REFS_XGDB` moves
    # the pinned library that indexes it; the two are separate because the
    # library is generated beside the data rather than inside it, since the
    # manifest holds absolute paths and pinning *at* `processed/` would stage
    # 24 GB on every run. `FABFOS_LIBRARY` moves the metasmith library of
    # transforms, which is a different thing again.
    #
    # Only refs-root is frozen at import. The other two answer per call: the
    # library is probed across three tiers, and the xgdb follows whichever
    # refs-root the caller is actually working with.
    DEFAULT_REFS_ROOT = REPO_ROOT/"data"/"fabfos"/"processed"
    REFS_ROOT = Path(os.environ.get("FABFOS_REFS_ROOT") or DEFAULT_REFS_ROOT).expanduser().resolve()

    LIBRARY_MARKERS = ("data_types", "resources", "transforms")
    BUNDLED_LIBRARY = MODULE_PATH/"_library"
    DEV_LIBRARY = MODULE_PATH.parent/"metasmith_libraries"

    @classmethod
    def is_library(cls, root: Path) -> bool:
        return all((root/sub).is_dir() for sub in cls.LIBRARY_MARKERS)

    @classmethod
    def library_root(cls) -> Path:
        override = os.environ.get("FABFOS_LIBRARY")
        if override:
            root = Path(override).expanduser().resolve()
            if not cls.is_library(root):
                raise FileNotFoundError(
                    f"FABFOS_LIBRARY=[{root}] is not a metasmith library "
                    f"(missing {'/ '.join(cls.LIBRARY_MARKERS)}/)"
                )
            return root
        for candidate in (cls.BUNDLED_LIBRARY, cls.DEV_LIBRARY):
            if cls.is_library(candidate):
                return candidate
        raise FileNotFoundError(
            "could not locate the FabFos metasmith library. Set FABFOS_LIBRARY, "
            "install the package with a bundled library, or run from a source "
            "checkout with the sibling src/metasmith_libraries module present."
        )

    @classmethod
    def xgdb(cls, refs_root: Path) -> Path:
        override = os.environ.get("FABFOS_REFS_XGDB")
        if override:
            return Path(override).expanduser().resolve()
        return Path(refs_root).resolve().parent/"refs.xgdb"
