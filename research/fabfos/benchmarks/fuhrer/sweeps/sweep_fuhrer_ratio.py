#!/usr/bin/env python3
"""Every Fuhrer deletion, every reachable axis, scored as a RATIO against a shared
denominator. The genome-wide deletion sweep.

    PYTHONPATH=$PWD/src mamba run -n ecspr python \
        research/fabfos/benchmarks/fuhrer/sweeps/sweep_fuhrer_ratio.py --channel gem --workers 3
    ... --channel denovo --workers 3 --min-lanes 2
    ... --fold-check glgC        # the fold-semantics gate; run it before trusting a sweep

WHY A RATIO AND NOT A DELTA. A two-point effective conductance is monotone under Rayleigh:
lowering any reaction's weight can only lower it. So a deletion pushes EVERY single readout
down, and a one-probe sweep measures how much network a gene carries rather than which fate
its loss favours. The ratio of two competing sinks is two-sided by construction, and that is
the whole reason a second solve is carried per mutant:

    ratio(axis) = C(D-glucose -> axis sink) / C(D-glucose -> CO2)

The source is the medium's carbon source and the denominator is oxidation to CO2 -- carbon
leaving the cell rather than being built into anything -- so the ratio asks what fraction of
the glucose pool\'s reachable fate is the axis rather than combustion. Every axis shares it,
which is what makes the axes comparable to each other within one channel.

THE PERTURBATION IS A DELETION AT FOLD 0, WHICH IS NOT WHAT THE OTHER THREE ARMS DO. Fang and
the 2010 eydallin arm overexpress at fold 2. Here a deleted gene\'s reactions go to weight
zero, `graph_from_pairs` keeps only strictly positive weights, and the edges genuinely
disappear. Two consequences follow that an overexpression sweep never meets:

  - THE DENOMINATOR CAN REACH ZERO. Deleting a gene can sever glucose from CO2 as easily as
    from the axis, and then the ratio is inf or nan rather than a number. The three states
    are NAMED in their own column rather than inferred downstream from a float -- `finite`,
    `sink_severed` (numerator alive, denominator gone) and `isolated` (both gone) mean
    different things and an analysis that reads them off `isinf` cannot tell the last two
    apart.
  - EVERY DELTA IS <= 0 BY RAYLEIGH ON EACH LEG, though not on the quotient. Score the
    result on |delta| unless there is a specific reason not to.

FOLD 1.0 MUST RETURN A BIT-EXACT ZERO AND THIS SCRIPT CHECKS IT. `--fold-check` solves one
gene at fold 1.0 and at the requested fold. The fold-1.0 ratio must equal the unperturbed
host ratio to the bit -- if it does not, the multiply is re-deriving the graph differently
and every delta in the sweep is partly solver jitter. Then the requested fold must MOVE the
numerator, which is what proves the fold multiplies the weight dict rather than unioning
into it.

ABSOLUTE CONDUCTANCES ARE NOT COMPARABLE ACROSS CHANNELS -- the de-novo background carries
six times the reactions and everything is larger there -- so only ranks within one channel
mean anything, and the channel is written into every output filename and every row.

`--min-lanes` FILTERS THE CLONE SIDE ONLY. The eydallin arm found that applying it to the
background disconnects the target outright, so the background stays whole and only the
mutant\'s credited reactions are required to agree across lanes.

A MUTANT WITH NO ATOM-MAPPED REACTION GETS A ROW CARRYING AN EXACT ZERO DELTA rather than an
absence, so the population stays the 3,806 deletions the screen measured. Rows append as
they finish, so an interrupted sweep resumes -- and the partial file is opened only when
there is something to append, because a completed sweep\'s partial is a read-only DVC
hardlink and opening it for append dies.
"""
from __future__ import annotations

import os

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
    os.environ.setdefault(_v, "1")

import argparse                                                        # noqa: E402
import csv                                                             # noqa: E402
import multiprocessing as mp                                           # noqa: E402
import sys                                                             # noqa: E402
import time                                                            # noqa: E402
from pathlib import Path                                               # noqa: E402

import pandas as pd                                                    # noqa: E402


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/fuhrer/sinks"))

from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                          # noqa: E402
import bake_pairs                                                      # noqa: E402
import probe_axes                                                      # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
COHORT_GPR = RUNS / "fuhrer_clones/gpr"
OUT_DIR = RUNS / "fuhrer_clones/ecspr"
AXES = ROOT / "data/fabfos/benchmarks/fuhrer/axes.tsv"
Y_DIR = ROOT / "data/fabfos/benchmarks/fuhrer/Y"

FIELDS = ("condition_id", "gene", "axis", "n_rxn", "over_num", "over_den", "over_ratio",
          "delta_ratio_pct", "ratio_state", "rxns")

FINITE, SINK_SEVERED, ISOLATED = "finite", "sink_severed", "isolated"
SINK_NOT_NODE, SOURCE_NOT_NODE = "sink_not_node", "source_not_node"

_S: dict = {}


def _probe(g, src_mnxm: str, snk_mnxm: str) -> tuple[float, str]:
    """Conductance between two metabolites' carbon atoms, and why it is zero if it is.

    A DELETION CAN REMOVE A TERMINAL FROM THE GRAPH ENTIRELY, which an overexpression sweep
    never meets and which the fang arm's version of this function therefore treats as a
    fatal error. At fold 0 a gene's reactions lose their edges, and if those were the only
    mapped carbon edges touching the sink, the sink stops being a node. That is a RESULT --
    the deletion severed the metabolite from the carbon graph -- not a bug, and it happens
    for real: deleting `bioD` removes dethiobiotin's only atom-mapped reaction. Raising here
    kills the pool worker, and because `SystemExit` is a BaseException the pool does not
    catch it, so the gene silently vanishes from the output instead of failing the run.
    """
    src = Terminal.metabolite(g, src_mnxm, label=src_mnxm)
    if src.missing:
        return 0.0, SOURCE_NOT_NODE
    snk = Terminal.metabolite(g, snk_mnxm, label=snk_mnxm)
    if snk.missing:
        return 0.0, SINK_NOT_NODE
    return float(solve(g, src, snk).total), ""


def _ratios(weights: dict) -> dict:
    """One graph build, then the shared denominator once and each numerator once."""
    g = graph_from_pairs(_S["pairs"], _S["element"], weights, _S["ratios"])
    den, den_note = _probe(g, _S["src"], _S["den"])
    out = {}
    for axis, sink in _S["axes"]:
        num, num_note = _probe(g, _S["src"], sink)
        if den > 0:
            # A numerator of zero with a live denominator is a perfectly good ratio of
            # zero; the note only distinguishes `no route` from `no node`.
            out[axis] = (num, den, num / den, num_note or FINITE)
        else:
            out[axis] = (num, den, float("inf") if num > 0 else float("nan"),
                         den_note or (SINK_SEVERED if num > 0 else num_note or ISOLATED))
    return out


def _one(task):
    gene, rxns = task
    base_w, fold = _S["base_w"], _S["fold"]
    got = _ratios({**base_w, **{x: base_w[x] * fold for x in rxns}})
    rows = []
    for axis, (num, den, r, state) in got.items():
        host_r = _S["host_ratio"][axis]
        rows.append(dict(condition_id=f"{_S['prefix']}:{gene}", gene=gene, axis=axis,
                         n_rxn=len(rxns), over_num=num, over_den=den, over_ratio=r,
                         delta_ratio_pct=100.0 * (r - host_r) / host_r,
                         ratio_state=state, rxns=",".join(rxns)))
    return rows


def live_axes(channel: str, host: str, element: str, out_dir: Path,
              roles=("target",), stem: str = "axes") -> list[tuple[str, str]]:
    """The declared target axes that `probe_axes.py` found reachable on THIS channel.

    Reading them from the probe's own output rather than re-solving is what keeps the sweep
    from quietly falling back to a column of zeros: an axis that is dead here is absent from
    the sweep entirely, and its absence is a documented reachability result rather than a
    flat ranking."""
    p = out_dir / probe_axes.probe_name(stem, channel, host, element)
    if not p.exists():
        raise SystemExit(f"[sweep] no axis probe at {p} -- run "
                         f"sinks/probe_axes.py --channel {channel} --axes {stem}.tsv "
                         f"first; sweeping without "
                         f"it risks reporting an unreachable sink as a null")
    df = pd.read_csv(p, sep="\t")
    df = df[df.role.isin(roles) & (df.axis != probe_axes.DENOMINATOR_AXIS)]
    live = df[pd.to_numeric(df.g, errors="coerce").fillna(0) > 0]
    dead = sorted(set(df.axis) - set(live.axis))
    print(f"[sweep] {channel}: sweeping {len(live)} reachable axes {sorted(live.axis)}"
          + (f"; skipping unreachable {dead}" if dead else ""), file=sys.stderr)
    return list(zip(live.axis, live.sink_mnxm))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--channel", choices=("gem", "denovo"), required=True)
    ap.add_argument("--fold", type=float, default=0.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--min-lanes", type=int, default=1,
                    help="de-novo only: credit the CLONE with a reaction only when at least "
                         "this many of the four annotation lanes assert it. Applies to the "
                         "clone side alone -- the eydallin arm found that applying it to the "
                         "background disconnects the target outright")
    ap.add_argument("--host", default="e_coli_bw25113")
    ap.add_argument("--cohort-dir", type=Path, default=COHORT_GPR)
    ap.add_argument("--census", default="mutant_census.tsv")
    ap.add_argument("--label", default="fuhrer")
    ap.add_argument("--limit", type=int, default=None, help="first N solvable mutants")
    ap.add_argument("--fold-check", default=None, metavar="GENE",
                    help="solve one cohort gene at fold 1.0 and --fold and report the move, "
                         "then exit. Two gates in one: fold 1.0 must reproduce the "
                         "unperturbed host ratio to the bit, and --fold must move the "
                         "numerator")
    ap.add_argument("--roles", nargs="+", default=["target"],
                    help="which declared axes to sweep. `control` adds each axis's own "
                         "one-reaction carbon neighbour, which is the sharpest control this "
                         "panel has: if a target and its own immediate precursor rank alike, "
                         "the probe has ranked the module around the metabolite rather than "
                         "the metabolite")
    ap.add_argument("--axes", type=Path, default=AXES,
                    help="the declared panel to sweep. `axes_wide.tsv` is the genome-wide "
                         "counterpart, selected on reachability rather than on the paper's "
                         "own AUC")
    ap.add_argument("--y-dir", type=Path, default=Y_DIR,
                    help="where the measured-phenotype sidecars for --axes live")
    ap.add_argument("--probe-dir", type=Path, default=OUT_DIR,
                    help="where sinks/probe_axes.py wrote its reachability table")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    if a.min_lanes > 1 and a.channel != "denovo":
        raise SystemExit("--min-lanes applies to the de-novo channel; the GEM channel is one "
                         "curated lane and has nothing to agree with")
    tag = (f"{a.label}_ratio_sweep_{a.channel}_{a.host}_fold{a.fold}_{a.element}"
           + ("" if a.axes.stem == "axes" else f"_{a.axes.stem}")
           + (f"_lanes{a.min_lanes}" if a.min_lanes > 1 else "")
           + ("_ctl" if sorted(a.roles) != ["target"] else ""))
    part = a.out_dir / f"{tag}.partial.tsv"
    final = a.out_dir / f"{tag}.tsv"

    t0 = time.time()
    _S["pairs"] = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    _S["ratios"] = load_direction_ratios(bake_pairs.direction_ratios())
    _S["element"], _S["fold"] = a.element, a.fold

    axes_decl = pd.read_csv(a.axes, sep="\t", dtype=str).fillna("")
    den_row = axes_decl[axes_decl.axis == probe_axes.DENOMINATOR_AXIS].iloc[0]
    _S["src"], _S["den"] = den_row.src_mnxm, den_row.sink_mnxm
    _S["axes"] = live_axes(a.channel, a.host, a.element, a.probe_dir, tuple(a.roles),
                           stem=a.axes.stem)
    if not _S["axes"]:
        raise SystemExit(f"[sweep] no target axis reaches on the {a.channel} channel -- "
                         f"there is nothing here to sweep")

    base_w = probe_axes.host_weights(a.host, a.channel)
    _S["base_w"] = base_w

    clone = pd.read_parquet(a.cohort_dir / f"gpr_{a.channel}.parquet")
    clone = clone[clone.in_atom_universe.fillna(False)]
    prefixes = sorted({str(c).split(":", 1)[0] for c in clone.condition_id})
    if len(prefixes) != 1:
        raise SystemExit(f"[sweep] the cohort GPR mixes condition prefixes {prefixes}")
    _S["prefix"] = prefixes[0]
    if a.min_lanes > 1:
        agree = clone.groupby(["condition_id", "mnxr"]).channel.nunique()
        clone = clone[pd.MultiIndex.from_arrays([clone.condition_id, clone.mnxr])
                      .isin(agree[agree >= a.min_lanes].index)]
    absent = sorted(set(clone.mnxr.astype(str)) - set(base_w))
    if absent:
        raise SystemExit(f"[sweep] {len(absent)} clone reaction(s) are not in the "
                         f"{a.channel} background, e.g. {absent[:5]}")

    census = pd.read_csv(a.cohort_dir / a.census, sep="\t").drop_duplicates("gene")
    by_gene = (clone.assign(g=clone.condition_id.str.split(":", n=1).str[1])
               .groupby("g").mnxr.apply(lambda s: sorted(set(s.astype(str)))).to_dict())

    host = _ratios(base_w)
    _S["host_ratio"] = {ax: host[ax][2] for ax in host}
    for ax, (num, den, r, state) in host.items():
        if state != FINITE:
            raise SystemExit(f"[sweep] the UNPERTURBED host already reads {state} on axis "
                             f"{ax}: there is no ratio to perturb")
        print(f"[sweep] {a.channel} host | {ax:18s} num {num:.9f} / den {den:.9f} = "
              f"{r:.9f}", file=sys.stderr)
    print(f"[sweep] {len(base_w):,} background reactions | setup {time.time() - t0:.1f}s",
          file=sys.stderr)

    if a.fold_check:
        gene = a.fold_check
        if gene not in by_gene:
            raise SystemExit(f"[fold-check] {gene} carries no atom-mapped reaction on the "
                             f"{a.channel} channel; pick one that does, e.g. "
                             f"{sorted(by_gene)[:5]}")
        rxns = by_gene[gene]
        one = _ratios({**base_w, **{x: base_w[x] * 1.0 for x in rxns}})
        two = _ratios({**base_w, **{x: base_w[x] * a.fold for x in rxns}})
        print(f"\n[fold-check] {gene}: {len(rxns)} reaction(s) {rxns}")
        # A fold of 1.0 rebuilds the graph from a different dict object holding the same
        # numbers. If the solve is deterministic the ratio comes back bit-identical to the
        # unperturbed host's; if it does not, part of every delta below is solver jitter
        # and no ranking in the sweep is trustworthy.
        jitter = {ax: one[ax][2] - _S["host_ratio"][ax] for ax in one}
        off = {ax: d for ax, d in jitter.items() if d != 0.0}
        if off:
            raise SystemExit(f"[fold-check] FAILED: fold 1.0 does not reproduce the "
                             f"unperturbed host ratio bit-exactly on {len(off)} axes "
                             f"{dict(list(off.items())[:4])} -- every delta in a sweep "
                             f"would carry that much solver jitter")
        print(f"[fold-check] fold 1.0 reproduces the host ratio bit-exactly on all "
              f"{len(one)} axes")
        moved = []
        for ax in one:
            d = 100.0 * (two[ax][2] - one[ax][2]) / one[ax][2]
            print(f"  {ax:18s} fold 1.0 num {one[ax][0]:.9f} ratio {one[ax][2]:.9f} | "
                  f"fold {a.fold} num {two[ax][0]:.9f} ratio {two[ax][2]:.9f} | "
                  f"{d:+.6f}%")
            moved.append(abs(two[ax][0] - one[ax][0]) > 0)
        if not any(moved):
            raise SystemExit(
                "[fold-check] FAILED: the conductance did not move at all between fold 1.0 "
                "and fold {:g}. The fold is not multiplying the clone's reaction weights, "
                "and every sweep number would be an artefact.".format(a.fold))
        print(f"[fold-check] PASSED: the numerator moves on "
              f"{sum(moved)}/{len(moved)} axes, so the fold multiplies rather than unions.")
        return 0

    done = set()
    if part.exists():
        done = set(pd.read_csv(part, sep="\t").gene.astype(str))
        print(f"[sweep] resuming: {len(done):,} clones already measured", file=sys.stderr)

    todo = [(g, by_gene[g]) for g in sorted(by_gene) if g not in done]
    if a.limit:
        todo = todo[:a.limit]
    print(f"[sweep] {len(census):,} clone genes | {len(by_gene):,} atom-mapped | "
          f"{len(todo):,} to solve on {a.workers} worker(s)", file=sys.stderr)

    # Open the partial ONLY when there is something to append. A completed sweep's partial
    # is a read-only DVC hardlink into the shared cache, and opening it for append raises
    # before the finalisation below ever runs -- so a re-run of a finished sweep dies
    # instead of rewriting its own summary.
    if todo:
        new_file = not part.exists()
        with part.open("a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(FIELDS), delimiter="\t")
            if new_file:
                w.writeheader()
            t = time.time()
            with mp.Pool(a.workers) as pool:
                for i, recs in enumerate(pool.imap_unordered(_one, todo, chunksize=4), 1):
                    w.writerows(recs)
                    if i % 100 == 0:
                        fh.flush()
                        el = time.time() - t
                        print(f"  {i:5}/{len(todo)}  {el / i:.2f}s/mutant  "
                              f"eta {(len(todo) - i) * el / i / 60:.1f} min",
                              file=sys.stderr)

    solved = pd.read_csv(part, sep="\t")
    frames = []
    for axis, _sink in _S["axes"]:
        s = solved[solved.axis == axis]
        df = census.merge(s.drop(columns=["condition_id", "axis"]), on="gene", how="left")
        unsolved = df.n_rxn.isna()
        df["axis"] = axis
        df["n_rxn"] = df.n_rxn.fillna(0).astype(int)
        df["host_num"], df["host_den"] = host[axis][0], host[axis][1]
        df["host_ratio"] = host[axis][2]
        df["over_num"] = df.over_num.fillna(host[axis][0])
        df["over_den"] = df.over_den.fillna(host[axis][1])
        df["over_ratio"] = df.over_ratio.fillna(host[axis][2])
        df["delta_ratio_pct"] = df.delta_ratio_pct.fillna(0.0)
        df["ratio_state"] = df.ratio_state.where(~unsolved, FINITE)
        df["rxns"] = df.rxns.fillna("")
        frames.append(df)
    out = pd.concat(frames, ignore_index=True)
    out["channel"] = a.channel
    # THE LABEL IS PER (GENE, AXIS) AND COMES FROM THE SCREEN, NOT FROM THE SWEEP. Each Y
    # sidecar carries every deletion's signed z-score on that axis's ion and whether it
    # clears the paper's 2.765 threshold. Joining on `gene` here rather than in the
    # analyser keeps the sweep file self-describing; the analyser still re-reads the
    # sidecars for the sign, because `is_positive` alone throws the direction away.
    labels = []
    for axis, _sink in _S["axes"]:
        path = a.y_dir / f"measured_{axis}.tsv"
        if not path.exists():
            print(f"[sweep] no Y sidecar for control axis {axis} -- it is a size control, "
                  f"not a readout, so its rows carry no label", file=sys.stderr)
            continue
        y = pd.read_csv(path, sep="\t", comment="#")
        labels.append(y[["gene", "is_positive", "z", "direction"]].assign(axis=axis))
    y_all = (pd.concat(labels, ignore_index=True) if labels
             else pd.DataFrame(columns=["gene", "is_positive", "z", "direction", "axis"]))
    out = out.merge(y_all, on=["gene", "axis"], how="left")
    out["is_positive"] = out.is_positive.fillna(False).astype(bool)
    out = out.sort_values(["axis", "delta_ratio_pct"], ascending=[True, False])
    final.unlink(missing_ok=True)
    out.to_csv(final, sep="\t", index=False)

    print(f"\n[sweep] wrote {final}", file=sys.stderr)
    for axis, _sink in _S["axes"]:
        d = out[out.axis == axis]
        print(f"    {axis:24s} {len(d):,} genes | {int((d.n_rxn > 0).sum()):,} solved | "
              f"positives {int(d.is_positive.sum())} "
              f"({int((d.is_positive & (d.n_rxn > 0)).sum())} atom-mapped) | "
              f"states {d.ratio_state.value_counts().to_dict()}", file=sys.stderr)
    print(f"    total {(time.time() - t0) / 60:.1f} min", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
