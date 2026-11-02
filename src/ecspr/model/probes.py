from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from . import tests as test_mod
from .build import ORIENTATIONS, graph_from_pairs, load_direction_ratios, load_pairs
from .directed import _HAVE_CHOLMOD
from .graph import Terminal, measure_leak
from .graph import solve as graph_solve
from .gpr import load_gpr, resolve_weights

SEP = test_mod.SEP

PROBES = ("two-point", "ground")

# KEYED ON THE TEST, not on the network. One condition carries several tests -- the same
# construction under `ground` and under `two-point`, or read against two different
# baselines -- and keying on the condition would collapse them into one row.
RESULT_COLUMNS = ("test_id", "probe", "orientation", "element", "readout", "value")


class Basis:
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
        self._by_element = {element: self}

    def graph(self, weights):
        return graph_from_pairs(self.pairs, self.element, weights, self.ratios,
                                orientation=self.orientation)

    def for_element(self, element: str) -> "Basis":
        # ONE BASIS PER ELEMENT, built on demand and shared across the run.
        #
        # **CAUTION** A test carries its own `element`, and `measure` LABELS its rows with
        # it -- but the graph used to come from whichever element the caller constructed the
        # basis on. A tests table spanning C, N, P and S therefore solved every row on one
        # element's graph and labelled the rows with four, silently. The label and the graph
        # now come from the same object, so that cannot be said any more.
        if element not in self._by_element:
            sib = Basis(self.atom_pairs_path, self.direction_path, element=element,
                        orientation=self.orientation)
            sib._by_element = self._by_element
            self._by_element[element] = sib
        return self._by_element[element]


def _rows(tid, probe, orientation, element, pairs):
    return [dict(test_id=tid, probe=probe, orientation=orientation,
                 element=element, readout=k, value=(float(v) if v is not None else None))
            for k, v in pairs]


def measure(basis: Basis, corpus: dict, condition, t: test_mod.Test, *, probe: str,
            weighting="belief", leak=1e-6, port=1.0, readouts="sinks",
            resolved=None, graph=None) -> list:
    """One test on one condition. `corpus` is `{table name: DataFrame}`.

    THE CONDITION AND THE TEST ARE SEPARATE ARGUMENTS because they come from separate
    tables and one condition serves several tests. Resolving the condition is the
    expensive half -- the metagenome corpus is 19.7M rows -- so `run` resolves each
    condition once and passes the result in as `resolved`.

    `graph` is the same economy one level down: the graph is a pure function of the
    condition's weights and the basis, so the tests sharing both share one. `run` passes
    it; a direct caller may leave it out and pay for its own.
    """
    if probe not in PROBES:
        raise ValueError(f"probe must be one of {PROBES}, got {probe!r}")
    basis = basis.for_element(t.element) if t.element else basis
    element = basis.element
    weights, cov = (resolved if resolved is not None
                    else resolve_weights(condition, corpus, weighting=weighting))
    g = basis.graph(weights) if graph is None else graph

    diag = [(f"_{k}", v) for k, v in cov.items() if v is not None]
    diag += [("_n_nodes", g.meta["n_nodes"]), ("_n_edges", g.meta["n_edges"]),
             ("_n_reactions_used", g.meta["n_reactions_used"]),
             ("_n_aam_gap", g.meta["n_aam_gap"]),
             ("_cholmod_available", int(_HAVE_CHOLMOD))]

    src = Terminal.metabolite(g, t.source_hub, label="source")
    diag.append(("_missing_source", len(src.missing)))
    if g.m == 0 or not src.nodes:
        diag.append(("_abstained", 1))
        return _rows(t.test_id, probe, basis.orientation, element, diag)
    diag.append(("_abstained", 0))

    if probe == "two-point":
        snk = Terminal.merge(g, t.sinks, label="ground")
        diag.append(("_missing_sinks", len(snk.missing)))
        if not snk.nodes:
            diag.append(("_abstained", 1))
            return _rows(t.test_id, probe, basis.orientation, element, diag)
        sol = graph_solve(g, src, snk)
        out = [("total", sol.total)]
        out += [(m, sol.delivered(m)) for m in t.sinks if m not in snk.missing]
        diag += [("_converged", int(sol.converged)),
                 ("_conservation_error", sol.conservation_error())]
    else:
        r = measure_leak(g, src, t.sinks, leak=leak, port=port)
        draw = r["draw"]
        want = t.readouts or (t.sinks if readouts == "sinks" else ("all",))
        keys = sorted(draw) if tuple(want) == ("all",) else [m for m in want if m in draw]
        out = [("total", r["total"])] + [(m, draw[m]) for m in keys]
        diag += [("_converged", int(r["converged"])),
                 ("_missing_readouts", len([m for m in want if m not in draw])),
                 ("_n_metabolites", r["n_metabolites"]),
                 ("_prec_share", r["prec_share"]), ("_leak_frac", r["leak_frac"]),
                 ("_leak", leak)]

    return _rows(t.test_id, probe, basis.orientation, element, out + diag)


def _shard_name(t: test_mod.Test, probe, orientation) -> str:
    # **CAUTION** THIS KEY CHANGED TWICE, so every shard directory written before either
    # change stops resuming and re-solves. That is correct both times, but a long sweep
    # will look like it lost its cache.
    #
    # First with the condition/test split: the old key hashed a condition_id that no longer
    # identifies a measurement. Then with the TERMINALS, which it had never covered -- so a
    # re-run that kept the test ids and changed the sink set resumed the old sinks' answers
    # under the new sinks' name, and nothing anywhere said so. A shard is a MEASUREMENT,
    # and the terminals are half of what a measurement is.
    key = "\x00".join((t.test_id, probe, orientation, t.element,
                       t.source_hub, SEP.join(t.sinks), SEP.join(t.readouts)))
    h = hashlib.blake2b(key.encode(), digest_size=8).hexdigest()
    return f"{h}.json"


def load_corpus(gpr_paths) -> dict:
    """`{table name: DataFrame}` from the staged GPR tables, keyed by FILE NAME.

    The name is what a conditions table writes in its `table` column, so it is the
    basename rather than the path: the same condition has to resolve whether the engine
    staged the table under a run directory or a driver passed it from `data/`.
    """
    if isinstance(gpr_paths, (str, bytes)) or hasattr(gpr_paths, "__fspath__"):
        gpr_paths = [gpr_paths]
    corpus = {}
    for path in gpr_paths:
        name = Path(path).name
        if name in corpus:
            raise ValueError(
                f"two staged GPR tables are both called {name!r}. A conditions table "
                f"names its tables by basename, so one of them would be unreachable")
        corpus[name] = load_gpr(path)
    return corpus


def run(basis: Basis, gpr_paths, conditions, tests, *, probe, weighting="belief",
        leak=1e-6, port=1.0, readouts="sinks", shard_dir=None, log=print) -> pd.DataFrame:
    """Every test, on the condition it names.

    `conditions` is a list of `Condition`; `tests` a list of `Test`. A test whose
    network names no condition is refused rather than skipped -- a sweep quietly missing
    half its arms reads as a null result.
    """
    corpus = load_corpus(gpr_paths)
    by_id = {c.condition_id: c for c in conditions}
    missing = sorted({t.network for t in tests if t.network not in by_id})
    if missing:
        raise ValueError(f"{len(missing)} test(s) name networks with no condition: "
                         f"{missing[:5]}. Known: {sorted(by_id)[:5]}")
    shard_dir = Path(shard_dir) if shard_dir else None
    if shard_dir:
        shard_dir.mkdir(parents=True, exist_ok=True)

    # ONE RESOLUTION PER CONDITION, however many tests read it. Kept here rather than
    # inside `measure` so the cache cannot outlive the corpus it was computed from.
    resolved: dict = {}
    # ONE GRAPH PER (CONDITION, ELEMENT), and it is dropped the moment the network
    # changes. Unbounded it would be the run's memory ceiling -- a null shard is a
    # thousand conditions and a carbon graph is twenty thousand nodes -- so it holds only
    # the network in hand, at most one graph per element. That is the whole win when a
    # tests table carries many axes on one construction: forty carbon axes used to build
    # the same carbon graph forty times. It is worth nothing when the tests are not
    # grouped by network, which is why the hit count is logged rather than assumed.
    graphs: dict = {}
    graphs_for = None
    frames, n_done, n_new, n_built = [], 0, 0, 0
    for i, t in enumerate(tests):
        shard = shard_dir / _shard_name(t, probe, basis.orientation) if shard_dir else None
        if shard is not None and shard.exists():
            frames.append(pd.DataFrame(json.loads(shard.read_text())))
            n_done += 1
            continue
        if t.network not in resolved:
            resolved[t.network] = resolve_weights(by_id[t.network], corpus,
                                                  weighting=weighting)
        b = basis.for_element(t.element) if t.element else basis
        if graphs_for != t.network:
            graphs, graphs_for = {}, t.network
        if b.element not in graphs:
            graphs[b.element] = b.graph(resolved[t.network][0])
            n_built += 1
        rows = measure(b, corpus, by_id[t.network], t, probe=probe,
                       weighting=weighting, leak=leak, port=port, readouts=readouts,
                       resolved=resolved[t.network], graph=graphs[b.element])
        if shard is not None:
            shard.write_text(json.dumps(rows))
        frames.append(pd.DataFrame(rows))
        n_new += 1
        if n_new == 1 or n_new % 25 == 0 or i + 1 == len(tests):
            log(f"[{probe}] {i + 1}/{len(tests)} tests "
                f"({n_new} solved, {n_done} resumed)")
    if n_new:
        log(f"[{probe}] {n_built} graph build(s) for {n_new} solve(s) over "
            f"{len(resolved)} network(s)")
    if n_done:
        log(f"[{probe}] resumed {n_done} test(s) from {shard_dir}")
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
    return (pd.read_parquet(p) if p.suffix == ".parquet"
            else pd.read_csv(p, sep="\t", float_precision="round_trip"))
