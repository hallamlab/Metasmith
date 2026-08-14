"""ECSPr -- an atom-resolved conductance instrument for metabolic networks.

WHAT THIS PACKAGE IS
--------------------
A GPR table in; a measurement out. Nothing here edits a network, resolves a
metabolite name, or knows what a perturbation is: a condition is a MASK over the
rows of a GPR table, and the difference between two conditions is a subtraction
the caller does over the results. That is the whole contract, and it is what lets
the deployed pipeline and a benchmark script run the identical command.

TWO LAYERS
----------
The **engine** measures one network:

  :mod:`ecspr.directed`  the rectified-diode Newton solve and its CHOLMOD binding
  :mod:`ecspr.graph`     the atom network, its terminals, the solve, and the
                         universal-leakage ground
  :mod:`ecspr.build`     atom pairs x per-reaction weights x direction ratios ->
                         a network; plus the GEM and GPR builders

The **experiment layer** runs the engine over a set of conditions:

  :mod:`ecspr.evidence`   belief-conservation weights: GPR rows -> ``{mnxr: E}``
  :mod:`ecspr.gpr`        the mask: which rows a condition selects, and what
                          conductance they aggregate to
  :mod:`ecspr.conditions` the conditions table -- terminals plus a mask, per row
  :mod:`ecspr.probes`     both probes, over one condition or over a table
  :mod:`ecspr.nulls`      draw a null pool, emitted AS a conditions table
  :mod:`ecspr.scoring`    delta, z, percentile rank against that pool

:mod:`ecspr.cli` is the command line over the experiment layer -- four verbs,
``two-point`` / ``ground`` / ``draw`` / ``score`` -- and is the only interface the
metasmith transform uses, so the transform and a benchmark run cannot drift apart
into two call sequences.
"""
from pathlib import Path

_MODULE = Path(__file__).resolve().parent
with open(_MODULE / "version.txt") as _f:
    __version__ = _f.read().strip()

NAME = "ecspr"
USER = "hallamlab"  # github id
SHORT_SUMMARY = ("Atom-resolved conductance measurement over metabolic GPR tables")
ENTRY_POINTS = [f"{NAME}={NAME}.cli:main"]
