"""Both probes, run over one condition or over a whole conditions table.

ONE OUTPUT SCHEMA, WHATEVER THE PROBE
-------------------------------------
Every measurement is a row of ``(condition_id, probe, orientation, element,
readout, value)``. ``readout`` is a sink metabolite, any metabolite from the
universal ground's draw vector, or ``total``. That is what lets
:mod:`ecspr.scoring` be probe-blind, and it is why a baseline is named at SCORING
time rather than flagged at measurement time: a delta is a subtraction over rows,
never something a probe was told to compute.

Readouts whose name starts with ``_`` are per-condition DIAGNOSTICS carried in the
same table -- coverage, convergence, the AAM gap. Scoring ignores them; a coverage
audit filters for them. They are here rather than in a sidecar file because a
result whose coverage lives in another artifact is a result someone will read
without it.

A METABOLITE ABSENT FROM THE DRAW IS A COVERAGE GAP, NOT A ZERO
---------------------------------------------------------------
``measure_leak``'s draw covers metabolites that are actual NODES of the built
graph. A sink that never became a node emits no row at all, and ``_missing_sinks``
records it: reading a missing key as zero is how an arm with the smaller graph
wins by abstaining.

RESUME IS PER CONDITION
-----------------------
Each condition writes its own shard as soon as it is solved, and a rerun skips
shards already on disk. This workstation kills long local jobs, and a thousand-draw
null is exactly the shape of run that gets killed at draw 900.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from . import conditions as cond_mod
from .build import ORIENTATIONS, graph_from_pairs, load_direction_ratios, load_pairs
from .directed import _HAVE_CHOLMOD
from .graph import Terminal, measure_leak
from .graph import solve as graph_solve
from .gpr import condition_weights, load_gpr

PROBES = ("two-point", "ground")
RESULT_COLUMNS = ("condition_id", "probe", "orientation", "element", "readout", "value")


class Basis:
    """The reference basis every condition in a run is measured against: the atom
    pairs, the direction ratios, the element and the orientation. Loaded once --
    the carbon slice alone is ~2M rows -- and shared across every condition, which
    is also the only way the observed and null arms can be the same measurement."""

    def __init__(self, atom_pairs, direction=None, *, element="C",
                 orientation="as_written"):
        if orientation not in ORIENTATIONS:
            raise ValueError(f"orientation must be one of {ORIENTATIONS}")
        self.element = element
        self.orientation = orientation
        self.pairs = load_pairs(atom_pairs, element=element)
        self.ratios = load_direction_ratios(direction) if direction else {}
        self.atom_pairs_path = str(atom_pairs)
        self.direction_path = str(direction) if direction else None

    def graph(self, weights):
        return graph_from_pairs(self.pairs, self.element, weights, self.ratios,
                                orientation=self.orientation)


def _rows(cid, probe, orientation, element, pairs):
    return [dict(condition_id=cid, probe=probe, orientation=orientation,
                 element=element, readout=k, value=(float(v) if v is not None else None))
            for k, v in pairs]


def measure(basis: Basis, gpr: pd.DataFrame, c: cond_mod.Condition, *, probe: str,
            weighting="belief", leak=1e-6, port=1.0, readouts="sinks") -> list:
    """One condition -> result rows. The whole of what a probe does."""
    if probe not in PROBES:
        raise ValueError(f"probe must be one of {PROBES}, got {probe!r}")
    element = c.element or basis.element
    weights, cov = condition_weights(gpr, weighting=weighting, **c.mask_kwargs())
    g = basis.graph(weights)

    diag = [(f"_{k}", v) for k, v in cov.items() if v is not None]
    diag += [("_n_nodes", g.meta["n_nodes"]), ("_n_edges", g.meta["n_edges"]),
             ("_n_reactions_used", g.meta["n_reactions_used"]),
             ("_n_aam_gap", g.meta["n_aam_gap"]),
             ("_cholmod_available", int(_HAVE_CHOLMOD))]

    src = Terminal.metabolite(g, c.source_hub, label="source")
    diag.append(("_missing_source", len(src.missing)))
    if g.m == 0 or not src.nodes:
        # A definite abstention, not a zero: no edge survived the mask, or the
        # injection point never became a node. Emitting a 0.0 total here would put
        # a number in the denominator that means "unmeasurable".
        diag.append(("_abstained", 1))
        return _rows(c.condition_id, probe, basis.orientation, element, diag)
    diag.append(("_abstained", 0))

    if probe == "two-point":
        snk = Terminal.merge(g, c.sinks, label="ground")
        diag.append(("_missing_sinks", len(snk.missing)))
        if not snk.nodes:
            diag.append(("_abstained", 1))
            return _rows(c.condition_id, probe, basis.orientation, element, diag)
        sol = graph_solve(g, src, snk)
        out = [("total", sol.total)]
        out += [(m, sol.delivered(m)) for m in c.sinks if m not in snk.missing]
        diag += [("_converged", int(sol.converged)),
                 ("_conservation_error", sol.conservation_error())]
    else:
        r = measure_leak(g, src, c.sinks, leak=leak, port=port)
        draw = r["draw"]
        want = c.readouts or (c.sinks if readouts == "sinks" else ("all",))
        keys = sorted(draw) if tuple(want) == ("all",) else [m for m in want if m in draw]
        out = [("total", r["total"])] + [(m, draw[m]) for m in keys]
        diag += [("_converged", int(r["converged"])),
                 ("_missing_readouts", len([m for m in want if m not in draw])),
                 ("_n_metabolites", r["n_metabolites"]),
                 ("_prec_share", r["prec_share"]), ("_leak_frac", r["leak_frac"]),
                 ("_leak", leak)]

    return _rows(c.condition_id, probe, basis.orientation, element, out + diag)


def _shard_name(c: cond_mod.Condition, probe, orientation) -> str:
    h = hashlib.blake2b(f"{c.condition_id}\x00{probe}\x00{orientation}".encode(),
                        digest_size=8).hexdigest()
    return f"{h}.json"


def run(basis: Basis, gpr_paths, conditions, *, probe, weighting="belief", leak=1e-6,
        port=1.0, readouts="sinks", shard_dir=None, log=print) -> pd.DataFrame:
    """Measure every condition, sharding to disk and resuming from what is there."""
    gpr = load_gpr(gpr_paths)
    shard_dir = Path(shard_dir) if shard_dir else None
    if shard_dir:
        shard_dir.mkdir(parents=True, exist_ok=True)

    frames, n_done, n_new = [], 0, 0
    for i, c in enumerate(conditions):
        shard = shard_dir / _shard_name(c, probe, basis.orientation) if shard_dir else None
        if shard is not None and shard.exists():
            frames.append(pd.DataFrame(json.loads(shard.read_text())))
            n_done += 1
            continue
        rows = measure(basis, gpr, c, probe=probe, weighting=weighting, leak=leak,
                       port=port, readouts=readouts)
        if shard is not None:
            shard.write_text(json.dumps(rows))
        frames.append(pd.DataFrame(rows))
        n_new += 1
        if n_new == 1 or n_new % 25 == 0 or i + 1 == len(conditions):
            log(f"[{probe}] {i + 1}/{len(conditions)} conditions "
                f"({n_new} solved, {n_done} resumed)")
    if n_done:
        log(f"[{probe}] resumed {n_done} condition(s) from {shard_dir}")
    if not frames:
        return pd.DataFrame(columns=list(RESULT_COLUMNS))
    return pd.concat(frames, ignore_index=True)[list(RESULT_COLUMNS)]


def write_results(df: pd.DataFrame, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix == ".parquet":
        df.to_parquet(p, index=False)
    else:
        df.to_csv(p, sep="\t", index=False)
    return p


def read_results(path) -> pd.DataFrame:
    p = Path(path)
    # `float_precision="round_trip"`, not pandas' default. The default C converter is
    # fast and lossy in the last few digits, which is invisible until a benchmark
    # compares a written result against the value that produced it and finds them
    # unequal at 1e-16. A results table that does not round-trip is not a record.
    return (pd.read_parquet(p) if p.suffix == ".parquet"
            else pd.read_csv(p, sep="\t", float_precision="round_trip"))
