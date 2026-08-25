#!/usr/bin/env python3
"""Every SCALEs gene, TWO conductance readouts each on LW06, scored as their ratio.

    mamba run -n ecspr python research/fabfos/benchmarks/woodruff/sweeps/sweep_woodruff_ratio.py \
        --channel gem --fold-check         # the two preconditions, solves nothing else
    ... --channel gem --workers 3
    ... --channel denovo --workers 3                    # unfiltered, ~40 min
    ... --channel denovo --min-lanes 2 --workers 3      # the lane filter, clone-side

    ratio = C(pyruvate -> ethanol) / C(pyruvate -> oxaloacetate)

WHY A RATIO AND NOT A DELTA. A two-point effective conductance is monotone in every edge
conductance (Rayleigh), so a x2 fold on any gene's reactions can only push a single
readout UP. A signed phenotype -- this gene helps production, that one hurts it -- is
inexpressible in one such readout. Dividing two competing fates of the SAME source
metabolite is what makes the score two-sided: pyruvate is the branch point the engineered
ethanologen was built around, ethanol is where LW06 is supposed to send it, and
oxaloacetate is the anaplerotic fate that competes for it. A gene that raises both legs
equally scores zero; only a gene that shifts the SPLIT registers.

THE TWO CONDUCTANCES ARE INDEPENDENT SOLVES on the same graph, not two readings of one.
Each builds its own terminal pair; only the graph and its weights are shared. Doubling
the probes doubles the per-gene cost, and that is the price of the normalisation.

THE DENOMINATOR IS NOT ONE COMMITTED ENZYME, AND THAT IS A REAL LIMIT OF THIS ARM.
iML1515 carries NO pyruvate carboxylase: `pyc` has zero GPR rows in this host. Every
pyruvate -> oxaloacetate route here is therefore indirect -- through PEP (`ppsA`, or
`pykA`/`pykF` run backwards, then `ppc`, the model's single PEP-carboxylase row) or
through malate (`maeA`/`maeB`, then `mdh` or `mqo`). The ratio still solves and both legs
are finite, but the denominator measures a diffuse anaplerotic capacity spread over eight
or so reactions rather than the sensitivity of one committed step. The eydallin
benchmark's glycogen -> pyruvate arm did have a committed enzyme behind it; this one does
not, and a null here is correspondingly less sharp.

THE BACKGROUND IS THE `in_atom_universe` FILTER, WHICH IS THIS BENCHMARK'S CONVENTION AND
NOT THE EYDALLIN SWEEP'S. `sweeps/sweep_scales.py` settled it and `panels/sink_panel.py`
follows it: transport reactions carry atom pairs and therefore real edges, and leaving
them in lets carbon reach a sink through a periplasmic shortcut that performs no
chemistry. 1,409 reactions filtered against 2,186 unfiltered on this host. NOTHING HERE
IS COMPARABLE TO AN EYDALLIN NUMBER, which used an unfiltered weight dict.

THE TWO CHANNELS' CONDUCTANCES ARE NOT COMPARABLE TO EACH OTHER EITHER -- backgrounds of
1,409 curated and 8,560 de-novo reactions on the same host. Only ranks within one channel
are, and the two reach the shared filter convention by different routes: the GEM table
carries `in_atom_universe`, while the de-novo table arrives entirely null and has the flag
recomputed here against the bake's own atom universe (`host_weights` refuses if either
stops being true).

THE DE-NOVO CHANNEL IS PROMISCUOUS AND `--min-lanes` IS THE KNOWN ANSWER, NOT A TUNING
KNOB. In the eydallin benchmark one ProtBERT call assigned glgC's committed step to 27
unrelated ORFs -- secE, cyoA-D, nuoA, rpsJ, tolR among them -- which tied at an identical
delta, filled the top of the ranking and pushed the AUC BELOW chance rather than to it;
`--min-lanes 2` moved it 0.4636 -> 0.4922. The same promiscuity is visible here before any
statistic: unfiltered, this cohort's de-novo table gives `adhP` 142 reactions against the
curated channel's 2, and 7.75 reactions per clone on average. The filter is applied
CLONE-SIDE ONLY. Applying it to the background is what disconnected eydallin's target
outright, because the committed step had single-lane support, and there would have been no
probe left to run.

FOLD MULTIPLIES; IT DOES NOT UNION. Every gene in this library is a gene the host already
carries, so an implementation that added a clone's reactions to the background as a SET
would be the identity map and the whole sweep would return flat -- which reads as a null
result rather than as a bug. `--fold-check` exists so that failure cannot happen quietly:
it solves a named host gene at fold 1.0 and 2.0, prints the set-union control beside
them, and REFUSES to proceed unless multiplication moves the ratio and union does not.

THREE HOST-DELETED REACTIONS ARE REFUSED RATHER THAN RESTORED. LW06 is BW25113 with
ldhA, ackA, frdABCD and adhE knocked out, and the clone GPR table was built against
unedited iML1515, so `ldhA`, `frdA`, `frdB` and `frdD` nominate LDH_D, FRD2 and FRD3 --
reactions this host does not have. Folding a weight the background does not carry is not
dosage, it is topology, and the fold semantics cannot express it; restoring FRD2/FRD3
would be worse still, since its GPR rule is `frdA and frdB and frdC and frdD` and every
one of the four is deleted, so no single-gene clone reconstitutes the complex. Those
reactions are dropped from those genes' fold sets and the affected genes fall through to
the exact-zero population with the reason printed. Four genes of 4,225, stated rather
than silently folded.

A GENE WITH NO ATOM-MAPPED REACTION IS AN EXACT ZERO, not an absence. It is never solved
-- folding nothing is the identity -- but it gets its row, because it is a gene the
screen measured and this method cannot see. Dropping those rows would silently redefine
the population as "genes the model happens to carry".

UNDER A DELETION THE DENOMINATOR CAN REACH ZERO, which doubling never allows. `--fold 0`
can disconnect oxaloacetate from pyruvate outright while ethanol is still reachable. That
is not a solver failure and not an infinity to be clipped; it is recorded as
`sink_severed`. Where BOTH legs go to zero the gene has severed pyruvate from the network
on both sides and the ratio is genuinely undefined -- `isolated`, counted separately,
never folded in with the first.
"""
from __future__ import annotations

import os

for _v in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
           "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_v, "1")

import argparse                                                        # noqa: E402
import csv                                                             # noqa: E402
import json                                                            # noqa: E402
import multiprocessing as mp                                           # noqa: E402
import sys                                                             # noqa: E402
import time                                                            # noqa: E402
from pathlib import Path                                               # noqa: E402

import pandas as pd                                                    # noqa: E402

READ_EXACT = dict(sep="\t", float_precision="round_trip")


def _repo_root(start: Path) -> Path:
    for d in (start, *start.parents):
        if (d / "data" / "fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(Path(__file__).resolve())
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks/eydallin"))

import bake_pairs                                                                  # noqa: E402
sys.path.insert(0, str(ROOT / "src/fabfos/build_references/resources/buildlib"))
import bench_universe as bu                                                        # noqa: E402
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                                      # noqa: E402

GPR = ROOT / "data/fabfos/runs/woodruff_clones/gpr"
OUT_DIR = ROOT / "data/fabfos/runs/woodruff_clones/ecspr"
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
METANETX = ROOT / "data/fabfos/originals/metanetx"

HOST = "e_coli_lw06"
COHORT = "scales_tol"

PYRUVATE_MNXM = "MNXM23"
ETHANOL_MNXM = "MNXM1108092"
OXALOACETATE_MNXM = "MNXM46"

FIELDS = ("condition_id", "gene", "gene_norm", "n_rxn", "over_num", "over_den",
          "over_ratio", "delta_ratio_pct", "ratio_state", "rxns")

FINITE, SINK_SEVERED, ISOLATED = "finite", "sink_severed", "isolated"

# The gene whose fold `--fold-check` proves the sweep on. adhP carries ALCD2x and ALCD19,
# the two alcohol dehydrogenase reactions iML1515 keeps in LW06 after adhE is deleted, so
# it sits directly on the numerator's last step -- the clearest place a working fold has
# to show and a broken one has to fail.
FOLD_CHECK_GENE = "adhp"

_S: dict = {}


def host_weights(host: str, channel: str, universe: set | None = None) -> dict:
    """The background, filtered the way `sweeps/sweep_scales.py` settled it for this
    benchmark. The GEM table carries `in_atom_universe` already; the de-novo table arrives
    entirely null and the flag has to be recomputed against the bake's own atom universe,
    so the two channels reach the same convention by different routes and the guard below
    refuses if that ever stops being true."""
    p = ROOT / f"data/fabfos/runs/{host}/gpr/gpr_{channel}.parquet"
    df = pd.read_parquet(p, columns=["mnxr", "in_atom_universe"])
    if channel == "gem":
        keep = df.in_atom_universe.fillna(False).astype(bool)
        if not keep.any():
            raise SystemExit(f"{p.relative_to(ROOT)} has no in_atom_universe=True rows; the "
                             f"filtered background convention cannot be applied to it")
    else:
        if df.in_atom_universe.notna().any():
            raise SystemExit(
                f"{p.relative_to(ROOT)} has a non-null in_atom_universe; it used to arrive "
                f"entirely null and be recomputed here. Check which flag is authoritative "
                f"before trusting either.")
        if universe is None:
            raise SystemExit("the de-novo background needs the atom universe to recompute")
        keep = df.mnxr.astype(str).isin(universe)
    return {m: 1.0 for m in sorted(df[keep].mnxr.dropna().astype(str).unique())}


def terminal_report(base_w: dict, channel: str) -> tuple:
    """Precondition (a), per terminal and per leg, printed before anything is swept.

    An unreachable terminal is UNINTERPRETABLE, never a zero, and the two are different
    failures: the sink panel in this same benchmark has Kdo2-lipid A returning a hard
    `g = 0` with `sink_is_node=True` while glucose 1-phosphate is not a node of the curated
    graph at all. Only the first is a measurement. So `sink_is_node` and `converged` are
    reported for every terminal on every channel rather than inferred from a number being
    finite."""
    g = graph_from_pairs(_S["pairs"], _S["element"], base_w, _S["ratios"])
    named = (("pyruvate", PYRUVATE_MNXM), ("ethanol", ETHANOL_MNXM),
             ("oxaloacetate", OXALOACETATE_MNXM))
    terms, missing = {}, []
    print(f"[terminals] {channel}: {len(base_w):,} background reactions", file=sys.stderr)
    for name, mnxm in named:
        t = Terminal.metabolite(g, mnxm, label=name)
        terms[name] = t
        node = not t.missing
        if not node:
            missing.append(f"{name} ({mnxm})")
        print(f"  {name:14s} {mnxm:14s} sink_is_node={node}", file=sys.stderr)
    if missing:
        print(f"[terminals] UNINTERPRETABLE on the {channel} channel: "
              f"{', '.join(missing)} is not a node of this graph. That is a coverage fact "
              f"about this basis, NOT a conductance of zero, and no sweep is run from it.",
              file=sys.stderr)
        return terms, None
    legs = {}
    for leg, (a, b) in (("numerator", ("pyruvate", "ethanol")),
                        ("denominator", ("pyruvate", "oxaloacetate"))):
        s = solve(g, terms[a], terms[b])
        legs[leg] = (float(s.total), bool(s.converged))
        print(f"  {leg:12s} C({a} -> {b}) = {s.total:.9f}  converged={s.converged}",
              file=sys.stderr)
    return terms, legs


def _probe(g, src_mnxm: str, snk_mnxm: str) -> float:
    src = Terminal.metabolite(g, src_mnxm, label=src_mnxm)
    snk = Terminal.metabolite(g, snk_mnxm, label=snk_mnxm)
    if src.missing or snk.missing:
        raise SystemExit(f"[sweep] terminal is not a node of this graph: "
                         f"src={src_mnxm} missing={src.missing} / "
                         f"snk={snk_mnxm} missing={snk.missing}. An unreachable terminal "
                         f"is uninterpretable, never a zero")
    return float(solve(g, src, snk).total)


def _ratio(weights: dict) -> tuple:
    g = graph_from_pairs(_S["pairs"], _S["element"], weights, _S["ratios"])
    num = _probe(g, PYRUVATE_MNXM, ETHANOL_MNXM)
    den = _probe(g, PYRUVATE_MNXM, OXALOACETATE_MNXM)
    if den > 0:
        return num, den, num / den, FINITE
    return num, den, (float("inf") if num > 0 else float("nan")), \
        (SINK_SEVERED if num > 0 else ISOLATED)


def _one(task):
    gene, norm, rxns = task
    base_w, fold = _S["base_w"], _S["fold"]
    num, den, r, state = _ratio({**base_w, **{x: base_w[x] * fold for x in rxns}})
    host_r = _S["host_ratio"]
    return dict(condition_id=f"{COHORT}:{norm}", gene=gene, gene_norm=norm,
                n_rxn=len(rxns), over_num=num, over_den=den, over_ratio=r,
                delta_ratio_pct=100.0 * (r - host_r) / host_r,
                ratio_state=state, rxns=",".join(rxns))


def fold_check(base_w: dict, by_gene: dict, host_ratio: float, fold: float) -> None:
    """Precondition (b): overexpression must MULTIPLY, and a set union must be a no-op."""
    rxns = by_gene.get(FOLD_CHECK_GENE)
    if not rxns:
        raise SystemExit(f"[fold-check] {FOLD_CHECK_GENE} carries no atom-mapped reaction "
                         f"in this cohort; pick a gene the host demonstrably has")
    print(f"[fold-check] {FOLD_CHECK_GENE}: {len(rxns)} reaction(s) {rxns}",
          file=sys.stderr)
    seen = {}
    for f in (1.0, fold, fold * 2):
        num, den, r, state = _ratio({**base_w, **{x: base_w[x] * f for x in rxns}})
        seen[f] = r
        print(f"  fold {f:<5}: num {num:.9f}  den {den:.9f}  ratio {r:.9f}  "
              f"delta {100.0 * (r - host_ratio) / host_ratio:+.6f}%  [{state}]",
              file=sys.stderr)
    _, _, r_union, _ = _ratio({**base_w, **{x: 1.0 for x in rxns}})
    d_union = 100.0 * (r_union - host_ratio) / host_ratio
    print(f"  set-union control: ratio {r_union:.9f}  delta {d_union:+.6f}%",
          file=sys.stderr)

    if seen[1.0] != host_ratio:
        raise SystemExit(f"[fold-check] fold 1.0 must be the identity; got {seen[1.0]!r} "
                         f"against host {host_ratio!r}")
    if seen[fold] == host_ratio:
        raise SystemExit(f"[fold-check] fold {fold} did not move the ratio at all. The "
                         f"fold is not multiplying, and every number this sweep would "
                         f"produce is a no-op reported as a null")
    if d_union != 0.0:
        raise SystemExit(f"[fold-check] the set-union control moved the ratio by "
                         f"{d_union}%, so a clone reaction is NOT already in the "
                         f"background and the union/multiply distinction is confounded")
    print(f"[fold-check] PASS: multiplication moves the ratio, union is exactly a no-op",
          file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--channel", choices=("gem", "denovo"), default="gem")
    ap.add_argument("--fold", type=float, default=2.0)
    ap.add_argument("--element", default="C")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--host", default=HOST)
    ap.add_argument("--fold-check", action="store_true",
                    help="run both preconditions and exit without sweeping")
    ap.add_argument("--min-lanes", type=int, default=1,
                    help="de-novo only: credit the CLONE with a reaction only when at least "
                         "this many of the four annotation lanes assert it. THE BACKGROUND "
                         "IS LEFT WHOLE -- in the eydallin benchmark applying the same rule "
                         "to the background disconnected the target outright, because the "
                         "committed step had single-lane support and there would have been "
                         "no probe left to run.")
    ap.add_argument("--limit", type=int, default=None, help="first N solvable genes (smoke)")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    if a.min_lanes > 1 and a.channel != "denovo":
        raise SystemExit("--min-lanes applies to the de-novo channel; the GEM channel is "
                         "one curated lane and has nothing to agree with")
    tag = (f"woodruff_ratio_sweep_{a.channel}_{a.host}_fold{a.fold}_{a.element}"
           + (f"_lanes{a.min_lanes}" if a.min_lanes > 1 else ""))
    part = a.out_dir / f"{tag}.partial.tsv"
    final = a.out_dir / f"{tag}.tsv"

    t0 = time.time()
    _S["pairs"] = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    _S["ratios"] = load_direction_ratios(bake_pairs.direction_ratios())
    _S["element"], _S["fold"] = a.element, a.fold

    universe = None
    if a.channel == "denovo":
        universe, ustats = bu.atom_universe(
            BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet",
            exclude=bu.transport_mnxrs(bu.reac_prop_path(METANETX)))
        print(bu.universe_line(ustats, "woodruff"), file=sys.stderr)
    base_w = host_weights(a.host, a.channel, universe)
    _S["base_w"] = base_w

    census = pd.read_csv(GPR / "gene_census.tsv", sep="\t", dtype=str).fillna("")
    clone = pd.read_parquet(GPR / f"gpr_{a.channel}.parquet")
    clone = clone[clone.in_atom_universe.fillna(False).astype(bool)]
    clone["g"] = clone.condition_id.str.split(":", n=1).str[1]

    deleted = sorted(set(clone.mnxr.astype(str)) - set(base_w))
    hit = sorted(set(clone.loc[clone.mnxr.astype(str).isin(deleted), "g"]))
    if deleted:
        print(f"[sweep] {len(deleted)} clone reaction(s) are absent from the {a.host} "
              f"{a.channel} background -- {a.host}'s own deletions. Refused rather than "
              f"restored (see the docstring): {deleted} on gene(s) {hit}", file=sys.stderr)
        clone = clone[~clone.mnxr.astype(str).isin(deleted)]

    def per_clone(frame) -> tuple:
        g = frame.groupby("g").mnxr.nunique()
        return int(g.size), float(g.mean()) if g.size else 0.0

    n_before, mean_before = per_clone(clone)
    lane_filter = None
    if a.min_lanes > 1:
        agree = clone.groupby(["g", "mnxr"]).channel.nunique()
        clone = clone[pd.MultiIndex.from_arrays([clone.g, clone.mnxr])
                      .isin(agree[agree >= a.min_lanes].index)]
        n_after, mean_after = per_clone(clone)
        lane_filter = dict(min_lanes=a.min_lanes, genes_before=n_before,
                           genes_after=n_after, mean_rxn_before=mean_before,
                           mean_rxn_after=mean_after)
        print(f"[sweep] --min-lanes {a.min_lanes} applied CLONE-SIDE (the background is "
              f"whole): {n_before:,} -> {n_after:,} genes with a reaction, mean reactions "
              f"per clone {mean_before:.2f} -> {mean_after:.2f}", file=sys.stderr)
    else:
        print(f"[sweep] no lane filter: {n_before:,} genes with a reaction, mean "
              f"{mean_before:.2f} reactions per clone", file=sys.stderr)

    by_gene = clone.groupby("g").mnxr.apply(lambda s: sorted(set(s.astype(str)))).to_dict()
    norm_to_gene = dict(zip(census.gene_norm, census.gene))

    terms, legs = terminal_report(base_w, a.channel)
    if legs is None:
        raise SystemExit(f"[sweep] a terminal is not a node of the {a.channel} graph. "
                         f"Reported above as uninterpretable; nothing is swept and no "
                         f"zero is written")
    host_num, host_den, host_ratio, host_state = _ratio(base_w)
    if host_state != FINITE:
        raise SystemExit(f"[sweep] the UNPERTURBED host already reads {host_state}: "
                         f"pyruvate->ethanol {host_num:.6g}, pyruvate->oxaloacetate "
                         f"{host_den:.6g}. There is no ratio to perturb")
    _S["host_ratio"] = host_ratio
    print(f"[sweep] {a.channel} on {a.host}: {len(base_w):,} background reactions "
          f"(in_atom_universe filter, uniform 1.0)\n"
          f"        pyruvate->ethanol {host_num:.9f} | pyruvate->oxaloacetate "
          f"{host_den:.9f} | host ratio {host_ratio:.9f} | setup {time.time() - t0:.1f}s",
          file=sys.stderr)

    fold_check(base_w, by_gene, host_ratio, a.fold)
    if a.fold_check:
        return 0

    todo_all = [(norm_to_gene[n], n, by_gene[n]) for n in sorted(by_gene)
                if n in norm_to_gene]
    if a.limit:
        todo_all = todo_all[:a.limit]

    done = set()
    if part.exists():
        done = set(pd.read_csv(part, **READ_EXACT).gene_norm.astype(str))
        print(f"[sweep] resuming: {len(done):,} genes already measured", file=sys.stderr)
    todo = [t for t in todo_all if t[1] not in done]
    print(f"[sweep] {len(census):,} measured genes | {len(todo_all):,} atom-mapped | "
          f"{len(todo):,} to solve on {a.workers} worker(s)", file=sys.stderr)

    # Open the partial ONLY when there is something to append to it. Once a finished sweep
    # has been `dvc add`ed, the partial comes back as a read-only hardlink out of the DVC
    # cache, so an unconditional `open("a")` makes re-running a COMPLETE sweep -- to refresh
    # its manifest, or to check it still reproduces -- die on PermissionError with every
    # number already on disk.
    new = not part.exists()
    if todo:
        with part.open("a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(FIELDS), delimiter="\t")
            if new:
                w.writeheader()
            t = time.time()
            with mp.Pool(a.workers) as pool:
                for i, rec in enumerate(pool.imap_unordered(_one, todo, chunksize=2), 1):
                    w.writerow(rec)
                    if i % 25 == 0:
                        fh.flush()
                        el = time.time() - t
                        print(f"  {i:5}/{len(todo)}  {el / i:.2f}s/gene  "
                              f"eta {(len(todo) - i) * el / i / 60:.1f} min", file=sys.stderr)
    else:
        print("[sweep] nothing to solve; re-merging the existing partial", file=sys.stderr)

    solved = pd.read_csv(part, **READ_EXACT)
    keep = ["gene", "gene_norm", "bnum_from_name", "bnum_sheet", "phenotype",
            "fitness_15", "fitness_30", "gem_resolved_via", "gem_n_in_universe"]
    df = census[[c for c in keep if c in census.columns]].copy()
    df = df.merge(solved.drop(columns=["condition_id", "gene"]), on="gene_norm", how="left")
    unsolved = df.n_rxn.isna()
    df["n_rxn"] = df.n_rxn.fillna(0).astype(int)
    df["host_num"], df["host_den"], df["host_ratio"] = host_num, host_den, host_ratio
    df["over_num"] = df.over_num.fillna(host_num)
    df["over_den"] = df.over_den.fillna(host_den)
    df["over_ratio"] = df.over_ratio.fillna(host_ratio)
    df["delta_ratio_pct"] = df.delta_ratio_pct.fillna(0.0)
    # A gene the method never reached was never perturbed, so it reads exactly the host
    # -- which is `finite` by the guard above, not a missing state.
    df["ratio_state"] = df.ratio_state.where(~unsolved, FINITE)
    df["rxns"] = df.rxns.fillna("")
    df["channel"] = a.channel
    df["host"] = a.host
    df["fold"] = a.fold
    df = df.sort_values("delta_ratio_pct", ascending=False).reset_index(drop=True)
    df.to_csv(final, sep="\t", index=False)

    n_solved = int((df.n_rxn > 0).sum())
    manifest = dict(
        tag=tag, channel=a.channel, host=a.host, cohort=COHORT, fold=a.fold,
        element=a.element, background_reactions=len(base_w),
        weight_dict_convention="in_atom_universe filter, uniform 1.0",
        numerator=dict(src=PYRUVATE_MNXM, snk=ETHANOL_MNXM, base=host_num,
                       converged=legs["numerator"][1], sink_is_node=True),
        denominator=dict(src=PYRUVATE_MNXM, snk=OXALOACETATE_MNXM, base=host_den,
                         converged=legs["denominator"][1], sink_is_node=True),
        host_ratio=host_ratio, measured_genes=int(len(census)), solved_genes=n_solved,
        exact_zero_genes=int(len(census)) - n_solved,
        host_deleted_reactions_refused=deleted, genes_affected_by_refusal=hit,
        lane_filter=lane_filter,
        ratio_state=df.ratio_state.value_counts().to_dict(),
        minutes=round((time.time() - t0) / 60, 2))
    (a.out_dir / f"{tag}.BUILD.json").write_text(json.dumps(manifest, indent=2))

    print(f"\n[sweep] wrote {final}\n"
          f"    {len(df):,} measured genes | {n_solved:,} solved | "
          f"{len(df) - n_solved:,} exact zeros\n"
          f"    ratio state {df.ratio_state.value_counts().to_dict()}\n"
          f"    total {(time.time() - t0) / 60:.1f} min", file=sys.stderr)
    # THE DE-NOVO CHANNEL'S KNOWN PATHOLOGY, CHECKED RATHER THAN REDISCOVERED. In the
    # eydallin benchmark one promiscuous ProtBERT call assigned glgC's committed step to 27
    # unrelated ORFs; they tied at an identical delta, filled the top of the ranking and
    # pushed the AUC BELOW chance. The signature is a large block of genes sharing one
    # delta, so it is printed here whether or not a lane filter was applied.
    solved_rows = df[df.n_rxn > 0]
    if len(solved_rows):
        blocks = solved_rows.groupby("delta_ratio_pct").gene.agg(list)
        biggest = max(blocks, key=len)
        if len(biggest) > 1:
            shared = set.intersection(*(set(str(r).split(",")) for r in
                                        solved_rows[solved_rows.gene.isin(biggest)].rxns))
            print(f"    largest tie block: {len(biggest)} genes share one delta "
                  f"({sorted(biggest)[:12]}{' ...' if len(biggest) > 12 else ''}); "
                  f"reactions common to all of them: {sorted(shared) or 'none'}",
                  file=sys.stderr)
        else:
            print("    largest tie block: 1 gene -- no shared-reaction tie block",
                  file=sys.stderr)
    print(df.head(15)[["gene", "phenotype", "n_rxn", "delta_ratio_pct"]]
          .to_string(index=False), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
