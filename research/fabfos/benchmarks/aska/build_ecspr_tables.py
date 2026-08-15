#!/usr/bin/env python3
"""The tables ECSPr eats: the clones, the thioesterase, the null pool, the conditions.

No solver code and no harness -- the packaged `ecspr` command line is the only
thing that runs a probe, and everything here is a table it reads. Four artifacts,
each with one reason to exist.

`gpr_clones.parquet` is the study tier's own `gpr_manual.parquet` with ONE FIELD
CHANGED: `unit_id` becomes the clone rather than the study. Under `--weighting
uniform` a reaction's conductance is the number of distinct units nominating it,
so leaving every clone under one unit would make a two-clone strain add the same
conductance as a one-clone strain, while a size-2 null draw adds two units. The
comparison is between a tested clone and a drawn clone, so the two arms have to
count units the same way.

`gpr_tesa.parquet` is the plasmid every strain carries. Strain F expresses a
leaderless cytosolic TesA', and the model has no such gene -- iML1515's own tesA
is the PERIPLASMIC lysophospholipase. But the reaction TesA' performs is in the
model: iML1515 carries the acyl-ACP <-> free fatty acid step at four chain
lengths (as `aas`/`acpP`'s AACPS reactions, whose direction ratio is 1.0, so the
edge is undirected and carries current toward the fatty acid). So the plasmid
enters the only way this package allows anything to enter -- as a unit whose rows
duplicate reactions the host already has -- and nothing is invented. It sits in
the BACKGROUND, so it is present in the baseline, in every control and in every
null draw, and therefore cancels in every delta.

`gpr_null_pool.parquet` is the whole ASKA (-) library on the same schema, one unit
per clone. A clone whose ORF resolves to no reaction STILL GETS A ROW, with a null
mnxr: `ecspr draw` samples the pool's `feature_id` values, so a clone absent from
the table cannot be drawn, and a null made only of clones the model can see is a
null for a different question than the one being asked.

`conditions_observed.tsv` is one row per measured strain plus the baseline. The
background is the host and the plasmid; the mask is this condition's own id; the
drop withholds the host's fadE rows, because every strain in the paper is
MG1655(DE3) dfadE -- and, for the two rfaY-deletion strains, the host's waaY row
as well. The drop is keyed on `evidence_id` rather than `mnxr` on purpose: the
complementation strain deletes the chromosomal copy and carries a plasmid one, and
only a key that separates the host's row from the clone's row can say that. Both
genes are sole-gene reactions in iML1515, so the drop is exact.

Writes everything under `main/benchmarks/aska/cache/`.
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[3]
CACHE = HERE / "cache"
BAKE = ROOT / "data/fabfos/processed/metabolism_bake"
HOST_GEM = ROOT / "data/fabfos/benchmarks/hosts/e_coli_k12/gpr_gem.parquet"
STUDY = ROOT / "data/fabfos/benchmarks/aska_ffa"
ROSTER = ROOT / "data/fabfos/originals/benchmarks/aska/library/aska_clone_minus.tsv"
GENOME = ROOT / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
PAIRS = ROOT / "data/fabfos/benchmark/reference_tier4/atom_pairs_tier4.parquet"

HOST_UNIT = "iML1515"
TESA_UNIT = "tesA_prime"

# Glycerol: the paper's carbon source, 30 g/L in an optimised M9.
SOURCE = "MNXM89612"

# The measured quantity is the sum of C12-C18 saturated and monounsaturated free
# fatty acids. All six are nodes of the carbon graph. C16:0 and C16:1 are reached
# only through the lysophospholipase route -- the atom-pair table carries no
# palmitoyl-ACP thioesterase at all -- which is why they are kept as separate
# readouts rather than folded away.
SINKS = ("MNXM402", "MNXM314", "MNXM108", "MNXM1107900", "MNXM236", "MNXM1364393")
SINK_NAMES = {"MNXM402": "C12:0", "MNXM314": "C14:0", "MNXM108": "C16:0",
              "MNXM1107900": "C16:1", "MNXM236": "C18:0", "MNXM1364393": "C18:1"}

# The acyl-ACP <-> free fatty acid step, one reaction per chain length iML1515
# covers. Chosen because each is ALREADY a host reaction sharing the host's own
# acyl-ACP node -- the other nine candidates in the atom-pair table hang off
# acyl-ACP metabolites nothing in iML1515 touches, so duplicating one of those
# would add an edge with no path to it.
TESA_REACTIONS = ("MNXR95146", "MNXR95138", "MNXR95145", "MNXR95144")

# Every strain is MG1655(DE3) dfadE. Sole-gene reactions, so the drop is exact.
FADE_EVIDENCE = ("ACOAD1f", "ACOAD2f", "ACOAD3f", "ACOAD4f", "ACOAD5f",
                 "ACOAD6f", "ACOAD7f", "ACOAD8f")
WAAY_EVIDENCE = ("HEPK2",)
DELETION_EVIDENCE = {"rfaY": WAAY_EVIDENCE}

SEP = "|"
GPR_COLS = ("build_id", "host", "unit_id", "feature_id", "feature_kind",
            "feature_name", "mnxr", "channel", "evidence_id", "evidence_name",
            "raw_score", "projection_via", "in_atom_universe", "gpr_rule")
COND_COLS = ("condition_id", "element", "source_hub", "sink_hub", "readout_hub",
             "media", "background_column", "background_values", "mask_column",
             "mask_values", "drop_column", "drop_values", "arm", "cohort",
             "is_control", "stratum", "n_units", "draw_id")


def gene_to_bnumber(gbk: Path) -> dict:
    text = gbk.read_text()
    block = re.compile(
        r'/gene="([^"]+)"\s*\n\s*/locus_tag="([^"]+)"'
        r'(?:\s*\n\s*/gene_synonym="([^"]*)")?', re.S)
    primary, alias = {}, {}
    for m in block.finditer(text):
        name, tag, syns = m.group(1), m.group(2), (m.group(3) or "")
        primary.setdefault(name, tag)
        for s in re.split(r";\s*", syns.replace("\n", " ")):
            if s.strip():
                alias.setdefault(s.strip(), tag)
    return {**alias, **primary}


def _blank_row(**kw):
    row = {c: None for c in GPR_COLS}
    row.update(channel="manual_gpr", raw_score=np.float32(1.0),
               projection_via="curated", host="e_coli_k12")
    row.update(kw)
    return row


def build_tesa(host: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for mnxr in TESA_REACTIONS:
        hit = host[host["mnxr"] == mnxr]
        if hit.empty:
            raise SystemExit(f"[tesa] {mnxr} is not a host reaction; the plasmid "
                             f"may only duplicate edges the host already has")
        rows.append(_blank_row(
            build_id="plasmid_pF", unit_id=TESA_UNIT, feature_id="tesA_prime",
            feature_kind="curated_gene", feature_name="tesA'", mnxr=mnxr,
            evidence_id=f"pF:{mnxr}",
            evidence_name="leaderless cytosolic acyl-ACP thioesterase (pF)",
            in_atom_universe=bool(hit["in_atom_universe"].iloc[0])))
    return pd.DataFrame(rows, columns=list(GPR_COLS))


def build_pool(host: pd.DataFrame, lookup: dict) -> tuple:
    roster = pd.read_csv(ROSTER, sep="\t", dtype=str, keep_default_na=False)
    by_gene = {t: grp for t, grp in host.groupby("feature_id")}
    rows, seen, unresolved = [], 0, 0
    for _, r in roster.iterrows():
        clone, gene = r["JW ID"].strip(), r["Gene Name"].strip()
        if not clone or not gene:
            continue
        tag = lookup.get(gene, "")
        got = by_gene.get(tag)
        if not tag:
            unresolved += 1
        common = dict(build_id="aska_minus", unit_id=clone, feature_id=tag or clone,
                      feature_kind="aska_clone", feature_name=gene,
                      evidence_id=clone, evidence_name=f"pCA24N-{gene}")
        if got is None or got.empty:
            # In the pool with a null reaction: drawable, and a genuine zero.
            rows.append(_blank_row(mnxr=None, in_atom_universe=None, **common))
            continue
        seen += 1
        for _, h in got.iterrows():
            rows.append(_blank_row(mnxr=h["mnxr"],
                                   in_atom_universe=bool(h["in_atom_universe"]),
                                   **common))
    pool = pd.DataFrame(rows, columns=list(GPR_COLS))
    pool["in_atom_universe"] = pool["in_atom_universe"].astype("boolean")
    return pool, dict(clones=int(pool["unit_id"].nunique()), with_reactions=seen,
                      unresolved=unresolved,
                      draw_values=int(pool["feature_id"].nunique()))


def condition_rows(study_conds: pd.DataFrame, ext: pd.DataFrame,
                   carriable: set) -> pd.DataFrame:
    """One row per measured strain, plus the unperturbed baseline.

    `is_control` IS RECOMPUTED HERE AND NOT TAKEN FROM THE STUDY TIER. The tier
    calls a condition structural when none of its reactions is in the tier's atom
    universe, and that universe EXCLUDES TRANSPORT -- for a good reason of its own,
    since a transporter's only atom pairs are the ATP hydrolysis every transporter
    shares. But ECSPr builds its graph straight from the atom-pair table, which
    keeps those pairs, so a transporter clone does add edges. Trusting the tier's
    flag put msbA -- thirty-two transport reactions -- in the control set, where it
    moved the readout by 5% and inflated the controls' spread past the null's,
    which reads exactly like a failed gate.

    A control here is therefore a condition none of whose added reactions appears
    in the element's atom-pair table at all. Those return the baseline bit-for-bit,
    which is the property the floor is measured from.
    """
    add = ext[ext["role"] == "add"]
    n_units = add.groupby("obs_id")["gene"].nunique()
    reaches = add[add["mnxr"] != ""].groupby("obs_id")["mnxr"].apply(
        lambda s: bool(set(s) & carriable))
    dels = (ext[ext["role"] == "del"].groupby("obs_id")["gene"]
            .apply(lambda s: sorted(set(s))))

    base_drop = list(FADE_EVIDENCE)
    common = dict(element="C", source_hub=SOURCE, sink_hub=SEP.join(SINKS),
                  readout_hub=SEP.join(SINKS), media="",
                  background_column="unit_id",
                  background_values=SEP.join((HOST_UNIT, TESA_UNIT)),
                  drop_column="evidence_id", draw_id=None)

    rows = [dict(condition_id="aska_ffa:BASELINE", mask_column="", mask_values="",
                 drop_values=SEP.join(base_drop), arm="observed", cohort="aska_ffa",
                 is_control=1, stratum=0, n_units=0, **common)]

    c = study_conds[study_conds["element"] == "C"]
    for _, r in c.iterrows():
        cid = r["condition_id"]
        if cid.endswith(":BASELINE") or cid.endswith(":ONPATH"):
            continue
        drop = list(base_drop)
        for gene in dels.get(cid, []):
            drop += list(DELETION_EVIDENCE.get(gene, ()))
        n = int(n_units.get(cid, 0))
        rows.append(dict(
            condition_id=cid, mask_column="condition_id", mask_values=cid,
            drop_values=SEP.join(drop), arm="observed", cohort="aska_ffa",
            is_control=int(not bool(reaches.get(cid, False))), stratum=n,
            n_units=n, **common))
    return pd.DataFrame(rows, columns=list(COND_COLS))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=CACHE)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    host = pd.read_parquet(HOST_GEM)
    lookup = gene_to_bnumber(GENOME)

    # The terminals have to be NODES, and a source that never became one makes the
    # probe abstain on every condition at once -- checked here, where it is one
    # message, rather than discovered as 92 abstentions.
    pairs = pd.read_parquet(PAIRS, columns=["mnxr", "element", "substrate", "product"])
    pairs = pairs[pairs["element"] == "C"]
    # Every reaction the basis can carry an edge for. This, not the study tier's
    # atom universe, is what decides whether a condition is a no-op -- see
    # `condition_rows`.
    carriable = set(pairs["mnxr"].unique())
    pairs = pairs[pairs["mnxr"].isin(set(host["mnxr"]))]
    nodes = set(pairs["substrate"]) | set(pairs["product"])
    if SOURCE not in nodes:
        raise SystemExit(f"[terminals] source {SOURCE} is not a carbon node of the "
                         f"host network")
    absent = [m for m in SINKS if m not in nodes]
    print(f"[terminals] source {SOURCE} present; sinks "
          f"{len(SINKS) - len(absent)}/{len(SINKS)} are host carbon nodes"
          + (f"; ABSENT {[SINK_NAMES[m] for m in absent]}" if absent else ""))

    tesa = build_tesa(host)
    tesa.to_parquet(args.out / "gpr_tesa.parquet", index=False)
    print(f"[tesa] {len(tesa)} rows over {tesa['mnxr'].nunique()} host reactions")

    clones = pd.read_parquet(STUDY / "gpr_manual.parquet")
    clones = clones[list(GPR_COLS) + ["condition_id"]].copy()
    # The clone is the unit. See the module docstring.
    clones["unit_id"] = ("aska:" + clones["feature_name"].fillna("none").astype(str))
    clones.to_parquet(args.out / "gpr_clones.parquet", index=False)
    print(f"[clones] {len(clones)} rows, {clones['unit_id'].nunique()} units, "
          f"{clones['condition_id'].nunique()} conditions")

    pool, stats = build_pool(host, lookup)
    pool.to_parquet(args.out / "gpr_null_pool.parquet", index=False)
    print(f"[pool] {len(pool):,} rows | {stats['clones']:,} clones | "
          f"{stats['with_reactions']:,} with >=1 reaction | "
          f"{stats['unresolved']} unresolved names | "
          f"{stats['draw_values']:,} distinct feature_id to draw from")

    ext = pd.read_csv(STUDY / "extraction.tsv", sep="\t", keep_default_na=False)
    sc = pd.read_csv(STUDY / "conditions.tsv", sep="\t", keep_default_na=False)
    conds = condition_rows(sc, ext, carriable)
    conds.to_csv(args.out / "conditions_observed.tsv", sep="\t", index=False)
    like = conds[conds["n_units"] > 0]
    like.to_csv(args.out / "conditions_like.tsv", sep="\t", index=False)
    print(f"[conditions] {len(conds)} rows "
          f"({int(conds['is_control'].sum())} controls incl. the baseline); "
          f"strata {sorted(set(like['n_units']))}; "
          f"{len(like)} drawable in --like")

    # The direction reference: the bake's integer-coded table decoded onto MNXR,
    # which is the shape `ecspr.model.build.load_direction_ratios` reads.
    dpath = args.out / "direction_ratios.parquet"
    d = pd.read_parquet(BAKE / "direction.parquet")
    v = pd.read_parquet(BAKE / "vocab.parquet")
    rv = v[v["kind"] == "rxn"][["code", "symbol"]].rename(
        columns={"code": "rxn", "symbol": "mnxr"})
    d = d.merge(rv, on="rxn", how="inner")
    d = d[d["mnxr"] != "EMPTY"][["mnxr", "ratio"]]
    d.to_parquet(dpath, index=False)
    print(f"[direction] {len(d):,} reactions -> {dpath}")


if __name__ == "__main__":
    main()
