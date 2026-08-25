#!/usr/bin/env python3
"""Every SCALEs gene, TWO conductance readouts each on LW06, scored as their ratio.

    mamba run -n ecspr python research/fabfos/benchmarks/woodruff/sweeps/sweep_woodruff_ratio.py \
        --fold-check                       # the two preconditions, solves nothing else
    mamba run -n ecspr python research/fabfos/benchmarks/woodruff/sweeps/sweep_woodruff_ratio.py \
        --channel gem --workers 3

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
from ecspr.model.build import graph_from_pairs, load_direction_ratios, load_pairs  # noqa: E402
from ecspr.model.graph import Terminal, solve                                      # noqa: E402

GPR = ROOT / "data/fabfos/runs/woodruff_clones/gpr"
OUT_DIR = ROOT / "data/fabfos/runs/woodruff_clones/ecspr"

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


def host_weights(host: str, channel: str) -> dict:
    p = ROOT / f"data/fabfos/runs/{host}/gpr/gpr_{channel}.parquet"
    df = pd.read_parquet(p, columns=["mnxr", "in_atom_universe"])
    keep = df.in_atom_universe.fillna(False).astype(bool)
    if not keep.any():
        raise SystemExit(f"{p.relative_to(ROOT)} has no in_atom_universe=True rows; the "
                         f"filtered background convention cannot be applied to it")
    return {m: 1.0 for m in sorted(df[keep].mnxr.dropna().astype(str).unique())}


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
    ap.add_argument("--limit", type=int, default=None, help="first N solvable genes (smoke)")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR)
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)

    tag = f"woodruff_ratio_sweep_{a.channel}_{a.host}_fold{a.fold}_{a.element}"
    part = a.out_dir / f"{tag}.partial.tsv"
    final = a.out_dir / f"{tag}.tsv"

    t0 = time.time()
    _S["pairs"] = load_pairs(bake_pairs.atom_pairs(), element=a.element)
    _S["ratios"] = load_direction_ratios(bake_pairs.direction_ratios())
    _S["element"], _S["fold"] = a.element, a.fold

    base_w = host_weights(a.host, a.channel)
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

    by_gene = clone.groupby("g").mnxr.apply(lambda s: sorted(set(s.astype(str)))).to_dict()
    norm_to_gene = dict(zip(census.gene_norm, census.gene))

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

    new = not part.exists()
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
        numerator=dict(src=PYRUVATE_MNXM, snk=ETHANOL_MNXM, base=host_num),
        denominator=dict(src=PYRUVATE_MNXM, snk=OXALOACETATE_MNXM, base=host_den),
        host_ratio=host_ratio, measured_genes=int(len(census)), solved_genes=n_solved,
        exact_zero_genes=int(len(census)) - n_solved,
        host_deleted_reactions_refused=deleted, genes_affected_by_refusal=hit,
        ratio_state=df.ratio_state.value_counts().to_dict(),
        minutes=round((time.time() - t0) / 60, 2))
    (a.out_dir / f"{tag}.BUILD.json").write_text(json.dumps(manifest, indent=2))

    print(f"\n[sweep] wrote {final}\n"
          f"    {len(df):,} measured genes | {n_solved:,} solved | "
          f"{len(df) - n_solved:,} exact zeros\n"
          f"    ratio state {df.ratio_state.value_counts().to_dict()}\n"
          f"    total {(time.time() - t0) / 60:.1f} min", file=sys.stderr)
    print(df.head(15)[["gene", "phenotype", "n_rxn", "delta_ratio_pct"]]
          .to_string(index=False), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
