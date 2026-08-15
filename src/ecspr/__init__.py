"""ECSPr -- an atom-resolved conductance instrument for metabolic networks.

WHAT THIS PACKAGE IS
--------------------
A GPR table in; a measurement out. Nothing here edits a network, resolves a
metabolite name, or knows what a perturbation is: a condition is a MASK over the
rows of a GPR table, and the difference between two conditions is a subtraction
the caller does over the results. That is the whole contract, and it is what lets
the deployed pipeline and a benchmark script run the identical command.

TWO LAYERS, IN :mod:`ecspr.model`
--------------------------------
The **engine** measures one network:

  :mod:`~ecspr.model.directed`   the rectified-diode Newton solve and its CHOLMOD
                                 binding
  :mod:`~ecspr.model.graph`      the atom network, its terminals, the solve, and
                                 the universal-leakage ground
  :mod:`~ecspr.model.build`      atom pairs x per-reaction weights x direction
                                 ratios -> a network; plus the GEM and GPR builders

The **experiment layer** runs the engine over a set of conditions:

  :mod:`~ecspr.model.evidence`   belief-conservation weights: GPR rows -> ``{mnxr: E}``
  :mod:`~ecspr.model.gpr`        the mask: which rows a condition selects, and what
                                 conductance they aggregate to
  :mod:`~ecspr.model.conditions` the conditions table -- terminals plus a mask, per row
  :mod:`~ecspr.model.probes`     both probes, over one condition or over a table
  :mod:`~ecspr.model.nulls`      draw a null pool, emitted AS a conditions table
  :mod:`~ecspr.model.scoring`    delta, z, percentile rank against that pool

:mod:`ecspr.cli` is the command line over the experiment layer -- four verbs,
``two-point`` / ``ground`` / ``draw`` / ``score`` -- and is the only interface the
metasmith transform uses, so the transform and a benchmark run cannot drift apart
into two call sequences.
"""
from pathlib import Path

_MODULE = Path(__file__).resolve().parent

NAME = "ecspr"
USER = "hallamlab"  # github id, and the quay.io / anaconda org
SHORT_SUMMARY = ("Atom-resolved conductance measurement over metabolic GPR tables")
ENTRY_POINTS = [f"{NAME}={NAME}.cli:main"]

# Versioning, on metasmith's model and deliberately independent of it and of
# FabFos: ECSPr ships as its own package and its own image, so `version.txt`
# here is bumped when ECSPr ships, not when FabFos does.

# Public version (PEP 440 release segment). Bumped by hand when shipping.
with open(_MODULE / "version.txt") as _f:
    VERSION = _f.read().strip()

# Build-time content hash of the source tree. Written by `_build_hash.py
# --write` immediately before a build; gitignored, and absent in a fresh
# checkout (which then degrades to the bare VERSION).
_bh = _MODULE / "build_hash.txt"
BUILD_HASH = _bh.read_text().strip() if _bh.exists() else ""

# Canonical version string -- PEP 440 local form. What `ecspr --version`
# reports, so a result can be traced to the exact source state that produced it.
FULL_VERSION = f"{VERSION}+{BUILD_HASH}" if BUILD_HASH else VERSION
__version__ = FULL_VERSION

# Container tag -- FULL_VERSION rendered for Docker, which rejects '+'. This is
# the single +->- translation site; docker/ecspr/dev.sh derives its tag here so
# the image name and the version the image reports cannot disagree.
CONTAINER_TAG = FULL_VERSION.replace("+", "-")
CONTAINER_IMAGE = f"quay.io/{USER}/{NAME}"
