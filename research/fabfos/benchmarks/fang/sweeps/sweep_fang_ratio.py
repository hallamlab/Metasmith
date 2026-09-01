#!/usr/bin/env python3
"""Every ASKA clone, every reachable Fang axis, scored as a RATIO against a shared
denominator. The library sweep.

    mamba run -n ecspr python research/fabfos/benchmarks/fang/sweeps/sweep_fang_ratio.py \
        --channel gem --workers 3
    ... --channel denovo --workers 3 --min-lanes 2
    ... --fold-check waaY        # the fold-semantics gate; run it before trusting a sweep

WHY A RATIO AND NOT A DELTA. A two-point effective conductance is monotone under Rayleigh:
raising any reaction's weight can only raise it. So a x2 fold on any clone pushes EVERY
single readout up, and a one-probe sweep measures how much network a clone touches rather
than which fate it favours. The ratio of two competing sinks is two-sided by construction,
and that is the whole reason a second solve is carried per clone:

    ratio(axis) = C(acetyl-CoA -> axis sink) / C(acetyl-CoA -> CO2)

The denominator is oxidation to CO2 -- carbon leaving the cell rather than being built into
anything -- so the ratio asks what fraction of the acetyl-CoA pool's reachable fate is the
axis rather than combustion. Every axis shares it, which is what makes the axes comparable
to each other within one channel.

FOUR AXES, AND WHICH CHANNEL EACH LIVES ON IS NOT A CHOICE. `sinks/probe_axes.py` solves
each declared sink on the unperturbed background first, and this script reads its output and
sweeps only the axes that reach. On this study the two channels are complementary rather
than redundant, which is a fact about the bases and not about the biology:

    axis                curated (iML1515)      de-novo (4-lane)
    LPS inner core      0.2995                 NOT A NODE
    Kdo2-lipid A        0 (sink_is_node=True)  26.14
    phosphatidate       3.168                  22.80
    hexadecanoate       2.788                  23.13

So each LPS axis exists on exactly one channel and neither is droppable. ABSOLUTE
CONDUCTANCES ARE NOT COMPARABLE ACROSS CHANNELS -- the de-novo background carries six times
the reactions and everything is larger there -- so only ranks within one channel mean
anything, and the channel is written into every output filename and every row.

THE FOLD MULTIPLIES, IT DOES NOT UNION, AND GETTING THAT WRONG MANUFACTURES A NULL. Every
ASKA clone is a chromosomal E. coli ORF that the host already carries, so unioning a clone's
reactions into the background changes nothing for any clone in the library and the sweep
returns a flat column that looks exactly like a real negative result. `--fold-check` exists
to prove the opposite before any sweep is believed: it solves one gene at fold 1.0 and at
the requested fold and refuses to continue if the conductance does not move.

A CLONE WITH NO ATOM-MAPPED REACTION GETS A ROW CARRYING AN EXACT ZERO DELTA rather than an
absence, so the population stays the 4,102 clones the screen assayed. Rows append as they
finish, so an interrupted sweep resumes.
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
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/fang/sinks"))

from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                          # noqa: E402
import bake_pairs                                                      # noqa: E402
import probe_axes                                                      # noqa: E402

RUNS = ROOT / "data/fabfos/runs"
COHORT_GPR = RUNS / "fang/gpr"
OUT_DIR = RUNS / "fang/ecspr"
AXES = ROOT / "data/fabfos/benchmarks/fang/axes.tsv"

FIELDS = ("condition_id", "gene", "axis", "n_rxn", "over_num", "over_den", "over_ratio",
          "delta_ratio_pct", "ratio_state", "rxns")

FINITE, SINK_SEVERED, ISOLATED = "finite", "sink_severed", "isolated"

_S: dict = {}


def _probe(g, src_mnxm: str, snk_mnxm: str) -> float:
    src = Terminal.metabolite(g, src_mnxm, label=src_mnxm)
    snk = Terminal.metabolite(g, snk_mnxm, label=snk_mnxm)
    if src.missing or snk.missing:
        raise SystemExit(f"[sweep] terminal missing: src={src.missing} snk={snk.missing}")
    return float(solve(g, src, snk).total)


def _ratios(weights: dict) -> dict:
    """One graph build, then the shared denominator once and each numerator once."""
    g = graph_from_pairs(_S["pairs"], _S["element"], weights, _S["ratios"])
    den = _probe(g, _S["src"], _S["den"])
    out = {}
    for axis, sink in _S["axes"]:
        num = _probe(g, _S["src"], sink)
        if den > 0:
            out[axis] = (num, den, num / den, FINITE)
        else:
            out[axis] = (num, den, float("inf") if num > 0 else float("nan"),
                         SINK_SEVERED if num > 0 else ISOLATED)
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
              roles=("target",)) -> list[tuple[str, str]]:
    """The declared target axes that `probe_axes.py` found reachable on THIS channel.

    Reading them from the probe's own output rather than re-solving is what keeps the sweep
    from quietly falling back to a column of zeros: an axis that is dead here is absent from
    the sweep entirely, and its absence is a documented reachability result rather than a
    flat ranking."""
    p = out_dir / f"axis_probe_{channel}_{host}_{element}.tsv"
    if not p.exists():
        raise SystemExit(f"[sweep] no axis probe at {p} -- run "
                         f"sinks/probe_axes.py --channel {channel} first; sweeping without "
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
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--min-lanes", type=int, default=1,
                    help="de-novo only: credit the CLONE with a reaction only when at least "
                         "this many of the four annotation lanes assert it. Applies to the "
                         "clone side alone -- the eydallin arm found that applying it to the "
                         "background disconnects the target outright")
    ap.add_argument("--host", default="e_coli_k12")
    ap.add_argument("--cohort-dir", type=Path, default=COHORT_GPR)
    ap.add_argument("--census", default="clone_census.tsv")
    ap.add_argument("--label", default="fang")
    ap.add_argument("--limit", type=int, default=None, help="first N solvable clones")
    ap.add_argument("--fold-check", default=None, metavar="GENE",
                    help="solve one cohort gene at fold 1.0 and --fold and report the move, "
                         "then exit. The gate against a set-union fold, which is a silent "
                         "no-op for every clone in this library")
    ap.add_argument("--roles", nargs="+", default=["target"],
                    help="which declared axes to sweep. `control` adds the size and "
                         "chain-length controls, and axis 1's size control is the sharpest "
                         "one available: it is axis 1's own substrate, identical but for "
                         "RfaY's phosphate, so if the two rank alike the axis measures a "
                         "large LPS molecule rather than the phosphorylated one")
    ap.add_argument("--probe-dir", type=Path, default=OUT_DIR,
                    help="where sinks/probe_axes.py wrote its reachability table")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    if a.min_lanes > 1 and a.channel != "denovo":
        raise SystemExit("--min-lanes applies to the de-novo channel; the GEM channel is one "
                         "curated lane and has nothing to agree with")
    tag = (f"{a.label}_ratio_sweep_{a.channel}_{a.host}_fold{a.fold}_{a.element}"
           + (f"_lanes{a.min_lanes}" if a.min_lanes > 1 else "")
           + ("_ctl" if sorted(a.roles) != ["target"] else ""))
    part = a.out_dir / f"{tag}.partial.tsv"
    final = a.out_dir / f"{tag}.tsv"

    t0 = time.time()
    _S["pairs"] = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    _S["ratios"] = load_direction_ratios(bake_pairs.direction_ratios())
    _S["element"], _S["fold"] = a.element, a.fold

    axes_decl = pd.read_csv(AXES, sep="\t", dtype=str).fillna("")
    den_row = axes_decl[axes_decl.axis == probe_axes.DENOMINATOR_AXIS].iloc[0]
    _S["src"], _S["den"] = den_row.src_mnxm, den_row.sink_mnxm
    _S["axes"] = live_axes(a.channel, a.host, a.element, a.probe_dir, tuple(a.roles))
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

    new = not part.exists()
    with part.open("a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(FIELDS), delimiter="\t")
        if new:
            w.writeheader()
        t = time.time()
        with mp.Pool(a.workers) as pool:
            for i, recs in enumerate(pool.imap_unordered(_one, todo, chunksize=4), 1):
                w.writerows(recs)
                if i % 25 == 0:
                    fh.flush()
                    el = time.time() - t
                    print(f"  {i:5}/{len(todo)}  {el / i:.2f}s/clone  "
                          f"eta {(len(todo) - i) * el / i / 60:.1f} min", file=sys.stderr)

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
    out["is_positive"] = out.fang_direction.astype(str) == "up"
    out["assayed"] = out.assayed_individually.astype(bool)
    out = out.sort_values(["axis", "delta_ratio_pct"], ascending=[True, False])
    out.to_csv(final, sep="\t", index=False)

    print(f"\n[sweep] wrote {final}", file=sys.stderr)
    for axis, _sink in _S["axes"]:
        d = out[out.axis == axis]
        print(f"    {axis:18s} {len(d):,} genes | {int((d.n_rxn > 0).sum()):,} solved | "
              f"positives {int(d.is_positive.sum())} "
              f"({int((d.is_positive & (d.n_rxn > 0)).sum())} atom-mapped) | "
              f"states {d.ratio_state.value_counts().to_dict()}", file=sys.stderr)
    print(f"    total {(time.time() - t0) / 60:.1f} min", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
