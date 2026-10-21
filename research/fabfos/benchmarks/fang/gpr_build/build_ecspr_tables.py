#!/usr/bin/env python3
"""The tables ECSPr eats: the clones, the thioesterase, the null pool, the conditions.

No solver code and no harness -- the packaged `ecspr` command line is the only
thing that runs a probe, and everything here is a table it reads. Four artifacts,
each with one reason to exist.

`gpr_clones.parquet` is the study tier's own `gpr_manual.parquet` with ONE FIELD
CHANGED: `genome_unit` becomes the clone rather than the study. Under `--weighting
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
mnxr: `ecspr draw` samples the pool's `orf` values, so a clone absent from
the table cannot be drawn, and a null made only of clones the model can see is a
null for a different question than the one being asked.

`conditions_observed.tsv` and `tests_observed.tsv` are the two tables: what each
strain IS, and what is measured on it. A construction unions the host table and
the plasmid table whole, unions this strain's clone rows, and SUBTRACTS the host's
fadE rows -- every strain in the paper is MG1655(DE3) dfadE, and the two
rfaY-deletion strains lose waaY as well. The subtract names the HOST TABLE and
keys on `intermediate_id`: the complementation strain deletes the chromosomal copy
and carries a plasmid one, so a subtract reaching the clone table would delete the
very thing the strain tests. Both genes are sole-gene reactions in iML1515, so the
subtract is exact.

`tests_like.tsv` is the drawable subset -- the strains that add at least one unit
-- and is what `ecspr draw --like-tests` matches a null pool against.

Writes everything under `research/fabfos/benchmarks/fang/cache/`.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


ROOT = _repo_root(HERE)
sys.path.insert(0, str(ROOT / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe                                          # noqa: E402
sys.path.insert(0, str(ROOT / "src"))
from ecspr.model import conditions as cond_mod                        # noqa: E402
from ecspr.model import tests as test_mod                             # noqa: E402
LANE = ROOT / "research/fabfos/benchmarks/fang"
CACHE = LANE / "cache"

sys.path.insert(0, str(ROOT / "research/fabfos/benchmarks"))
import bake_identity                                                          # noqa: E402

BAKE = bake_identity.DEPLOYED
HOST_GEM = ROOT / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
STUDY = ROOT / "data/fabfos/benchmarks/fang"
ROSTER = ROOT / "data/fabfos/originals/benchmarks/aska/library/aska_clone_minus.tsv"
GENOME = ROOT / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"

HOST_UNIT = "iML1515"
TESA_UNIT = "tesA_prime"

SOURCE = "MNXM89612"

SINKS = ("MNXM402", "MNXM314", "MNXM108", "MNXM1107900", "MNXM236", "MNXM1364393")
SINK_NAMES = {"MNXM402": "C12:0", "MNXM314": "C14:0", "MNXM108": "C16:0",
              "MNXM1107900": "C16:1", "MNXM236": "C18:0", "MNXM1364393": "C18:1"}

TESA_REACTIONS = ("MNXR95146", "MNXR95138", "MNXR95145", "MNXR95144")

FADE_EVIDENCE = ("ACOAD1f", "ACOAD2f", "ACOAD3f", "ACOAD4f", "ACOAD5f",
                 "ACOAD6f", "ACOAD7f", "ACOAD8f")
WAAY_EVIDENCE = ("HEPK2",)
DELETION_EVIDENCE = {"rfaY": WAAY_EVIDENCE}

SEP = "|"
EXTENSIONS = ("attribution", "feature", "universe")
GPR_COLS = tuple(fe.schema_for(EXTENSIONS))
BASELINE_ID = "aska_ffa:BASELINE"

# THE STAGED NAMES, which are what a conditions table selects on. A step names its table
# by BASENAME, so these must match what `run_panel.py` passes to `--gpr`.
HOST_TABLE = HOST_GEM.name
TESA_TABLE = "gpr_tesa.parquet"
CLONE_TABLE = "gpr_clones.parquet"


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


def carbon_pairs(bake: Path) -> pd.DataFrame:
    v = pd.read_parquet(bake / "vocab.parquet")
    rxn = v[v["kind"] == "rxn"].set_index("code")["symbol"]
    met = v[v["kind"] == "met"].set_index("code")["symbol"]
    ele = v[v["kind"] == "element"].set_index("symbol")["code"]
    if "C" not in ele.index:
        raise SystemExit(f"[terminals] {bake}/vocab.parquet codes no carbon element")
    p = pd.read_parquet(bake / "atom_pairs.parquet",
                        columns=["rxn", "element", "tail_met", "head_met"])
    p = p[p["element"] == int(ele.loc["C"])]
    return pd.DataFrame(dict(mnxr=rxn.reindex(p["rxn"]).to_numpy(),
                             substrate=met.reindex(p["tail_met"]).to_numpy(),
                             product=met.reindex(p["head_met"]).to_numpy()))


def _blank_row(**kw):
    row = {c: None for c in GPR_COLS}
    row.update(channel="manual_gpr", llr=np.float32(fe.ASSERTION_LLR),
               projection_via="curated", evidence_quality="unknown", lane_set="curated",
               host="e_coli_k12")
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
            source="pF", build_id="plasmid_pF", genome_unit=TESA_UNIT, orf="tesA_prime",
            feature_kind="curated_gene", feature_name="tesA'", mnxr=mnxr,
            intermediate_id=f"pF:{mnxr}",
            intermediate_name="leaderless cytosolic acyl-ACP thioesterase (pF)",
            in_atom_universe=bool(hit["in_atom_universe"].iloc[0])))
    df = pd.DataFrame(rows, columns=list(GPR_COLS))
    fe.validate_gpr(df, "curated", None, "pF", EXTENSIONS)
    return df


def build_pool(host: pd.DataFrame, lookup: dict) -> tuple:
    roster = pd.read_csv(ROSTER, sep="\t", dtype=str, keep_default_na=False)
    by_gene = {t: grp for t, grp in host.groupby("orf")}
    rows, seen, unresolved = [], 0, 0
    for _, r in roster.iterrows():
        clone, gene = r["JW ID"].strip(), r["Gene Name"].strip()
        if not clone or not gene:
            continue
        tag = lookup.get(gene, "")
        got = by_gene.get(tag)
        if not tag:
            unresolved += 1
        common = dict(source="aska_minus", build_id="aska_minus", genome_unit=clone,
                      orf=tag or clone, feature_kind="aska_clone", feature_name=gene,
                      intermediate_id=clone, intermediate_name=f"pCA24N-{gene}")
        if got is None or got.empty:
            rows.append(_blank_row(mnxr=None, in_atom_universe=None, **common))
            continue
        seen += 1
        for _, h in got.iterrows():
            rows.append(_blank_row(mnxr=h["mnxr"],
                                   in_atom_universe=bool(h["in_atom_universe"]),
                                   **common))
    pool = pd.DataFrame(rows, columns=list(GPR_COLS))
    pool["in_atom_universe"] = pool["in_atom_universe"].astype("boolean")
    pool = (pool.drop_duplicates(fe.grain_key(EXTENSIONS))
                .sort_values(fe.grain_key(EXTENSIONS), kind="mergesort",
                             na_position="last").reset_index(drop=True))
    fe.validate_gpr(pool, "curated", None, "aska_minus", EXTENSIONS)
    return pool, dict(clones=int(pool["genome_unit"].nunique()), with_reactions=seen,
                      unresolved=unresolved,
                      draw_values=int(pool["orf"].nunique()))


def condition_rows(study_conds: pd.DataFrame, ext: pd.DataFrame,
                   carriable: set) -> tuple:
    """`(conditions, tests)` -- what each strain IS, and what is measured on it.

    THE BACKGROUND IS TWO WHOLE TABLES, the host GEM and the tesA' plasmid, so it is two
    `union` steps rather than a `genome_unit` selector over a concatenation. Both are
    present in the baseline, in every control and in every null draw, and therefore cancel
    in every delta.

    THE DELETION SUBTRACTS FROM THE HOST TABLE ALONE, and that is the sharpening the
    schema now allows. Every strain in the paper is MG1655(DE3) dfadE, and the two
    rfaY-deletion strains lose waaY as well; the complementation strain deletes the
    chromosomal copy and CARRIES A PLASMID ONE, so a subtract that reached the clone table
    would delete the very thing the strain is testing. Keying on `intermediate_id` said
    that before by relying on the two tables spelling the reaction differently. Naming the
    table says it outright.
    """
    add = ext[ext["role"] == "add"]
    n_units = add.groupby("obs_id")["gene"].nunique()
    reaches = add[add["mnxr"] != ""].groupby("obs_id")["mnxr"].apply(
        lambda s: bool(set(s) & carriable))
    dels = (ext[ext["role"] == "del"].groupby("obs_id")["gene"]
            .apply(lambda s: sorted(set(s))))

    background = (cond_mod.Step(table=HOST_TABLE, type="", set=(), operation="union"),
                  cond_mod.Step(table=TESA_TABLE, type="", set=(), operation="union"))
    settings = dict(element="C", source_hub=SOURCE, sink_hub=SEP.join(SINKS),
                    readout_hub=SEP.join(SINKS))

    # `aska_ffa` is the cohort id the published study-tier products under
    # `data/fabfos/benchmarks/fang/` were built with, and these tables have to join
    # against them. It is the study's former registration name, kept because the ids are
    # in data rather than because the lane is still called that.
    def _drop(values):
        return (cond_mod.Step(table=HOST_TABLE, type="intermediate_id",
                              set=tuple(values), operation="subtract"),)

    base_drop = list(FADE_EVIDENCE)
    conds = [cond_mod.Condition(BASELINE_ID, background + _drop(base_drop))]
    tests = [test_mod.Test(test_id=BASELINE_ID, network=BASELINE_ID, arm="ecspr",
                           settings=settings,
                           meta=dict(arm="observed", cohort="aska_ffa", is_control=1,
                                     stratum=0, n_units=0, draw_id=None))]

    c = study_conds[study_conds["element"] == "C"]
    for _, r in c.iterrows():
        cid = r["condition_id"]
        if cid.endswith(":BASELINE") or cid.endswith(":ONPATH"):
            continue
        drop = list(base_drop)
        for gene in dels.get(cid, []):
            drop += list(DELETION_EVIDENCE.get(gene, ()))
        n = int(n_units.get(cid, 0))
        conds.append(cond_mod.Condition(cid, background + (
            cond_mod.Step(table=CLONE_TABLE, type="condition_id", set=(cid,),
                          operation="union"),) + _drop(drop)))
        # A CONTROL IS DERIVED, never declared: a strain whose additions reach no
        # atom-mapped reaction is a real perturbation that cannot move the measurement,
        # which is exactly what makes it the noise floor.
        tests.append(test_mod.Test(
            test_id=cid, network=cid, arm="ecspr", read_against=BASELINE_ID,
            settings=settings,
            meta=dict(arm="observed", cohort="aska_ffa",
                      is_control=int(not bool(reaches.get(cid, False))),
                      stratum=n, n_units=n, draw_id=None)))
    return conds, tests


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=CACHE)
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    host = pd.read_parquet(HOST_GEM)
    lookup = gene_to_bnumber(GENOME)

    pairs = carbon_pairs(BAKE)
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
    clones = clones[fe.schema_for(fe.extensions_of(clones))].copy()
    clones["genome_unit"] = ("aska:" + clones["feature_name"].fillna("none").astype(str))
    clones.to_parquet(args.out / "gpr_clones.parquet", index=False)
    print(f"[clones] {len(clones)} rows, {clones['genome_unit'].nunique()} units, "
          f"{clones['condition_id'].nunique()} conditions")

    pool, stats = build_pool(host, lookup)
    pool.to_parquet(args.out / "gpr_null_pool.parquet", index=False)
    print(f"[pool] {len(pool):,} rows | {stats['clones']:,} clones | "
          f"{stats['with_reactions']:,} with >=1 reaction | "
          f"{stats['unresolved']} unresolved names | "
          f"{stats['draw_values']:,} distinct orf to draw from")

    ext = pd.read_csv(STUDY / "extraction.tsv", sep="\t", keep_default_na=False)
    sc = pd.read_csv(STUDY / "conditions.tsv", sep="\t", keep_default_na=False)
    conds, tests = condition_rows(sc, ext, carriable)
    cond_mod.write(conds, args.out / "conditions_observed.tsv")
    test_mod.write(tests, args.out / "tests_observed.tsv", "ecspr")
    like = [t for t in tests if int(t.meta.get("n_units") or 0) > 0]
    test_mod.write(like, args.out / "tests_like.tsv", "ecspr")
    print(f"[conditions] {len(conds)} constructions, {len(tests)} tests "
          f"({sum(int(t.meta.get('is_control') or 0) for t in tests)} controls incl. the "
          f"baseline); strata {sorted({int(t.meta['n_units']) for t in like})}; "
          f"{len(like)} drawable in --like-tests")

    dpath = bake_identity.build_direction_ratios(
        args.out / "direction_ratios.parquet", BAKE)
    print(f"[direction] {dpath} <- bake {bake_identity.identity(BAKE)}")


if __name__ == "__main__":
    main()
