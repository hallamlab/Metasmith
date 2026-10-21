#!/usr/bin/env python3
"""The Fuhrer cohort's edges on both channels: every Keio deletion the screen measured,
read off BW25113's curated iML1515 table and its de-novo table.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/fuhrer/gpr_build/build_fuhrer_gpr.py
    ... --publish        # writes data/fabfos/runs/fuhrer_clones/gpr/

ONE RESOLVER, ALREADY RUN. Unlike the ASKA builder this script resolves nothing: `parse/`
placed every screened gene on a b-number and a BW25113 protein accession already, and this
reads that table. The two channels key on DIFFERENT identifiers and that is not a detail --
`gpr_gem.parquet` keys `orf` on iML1515's b-numbers, `gpr_denovo.parquet` on BW25113 protein
accessions -- so one join would silently produce an empty channel.

A DELETION IS `action=del`. The fang and 2010-eydallin arms carry `action=add`, because an
overexpression clone is a second copy of a gene the host already has; a Keio mutant is the
gene's absence, and the two disagree about the sign of every perturbation by construction.
`action` is part of the grain key precisely so one condition can carry both.

THE CENSUS IS THE UNION OF THE SCREEN AND THE MODEL, NOT THEIR INTERSECTION, and that is
what makes it a denominator. Three populations overlap here and every rate this arm quotes
has to say which one it is over:

    screened     the 3,806 deletions this screen measured (`wt` is not one of them)
    model_gene   the 1,511 genes iML1515 names -- the only ones the curated probe can rank
    assayed      the Keio collection's roster (Baba 2006), which is neither of the above

A model gene the screen never measured keeps a row with `screened=False`, and a screened
gene the model does not name keeps one with a blank `model_gene`. Dropping either would turn
"n of 3,806" into "n of 1,348" somewhere downstream and flatter the method by two thirds.

**CAUTION** `in_atom_universe` ARRIVES ALL-NULL ON THE DE-NOVO CHANNEL and is recomputed here
with transport excluded. Defaulting the null is wrong in a different direction on each
channel: true admits transporters and reconnects everything, false admits nothing and reads
as "the organism cannot reach it".

Every parquet is unlinked before it is written. DVC checks data out as read-only hardlinks
into a shared cache, so writing through one edits the cache object every other worktree
sees.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/keio"))
import baba_roster                                                     # noqa: E402
sys.path.insert(0, str(REPO / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe                                           # noqa: E402
sys.path.insert(0, str(REPO / "src/fabfos/build_references/resources/buildlib"))
import bench_universe as bu                                            # noqa: E402

HOST_GEM = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet"
HOST_DENOVO = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_denovo.parquet"
RESOLUTION = REPO / "data/fabfos/runs/fuhrer_clones/parse/lof/lof_resolution.tsv"
BAKE = REPO / "data/fabfos/processed/metabolism_bake"
METANETX = REPO / "data/fabfos/originals/metanetx"
OUT = REPO / "data/fabfos/runs/fuhrer_clones/gpr"

HOST = "e_coli_bw25113"
COHORT = "fuhrer"
SOURCE_ORGANISM = "e_coli_bw25113"
EXTENSIONS = ("attribution", "feature", "universe", "cohort")
GEM_LANE_SET = "curated"
DENOVO_LANE_SET = "chosen_4"

CENSUS_COLS = ("gene", "condition_id", "b_number", "model_gene", "denovo_feature",
               "screened", "assayed", "essential",
               "gem_n_reactions", "gem_n_mnxr", "gem_n_in_universe",
               "denovo_n_mnxr", "denovo_n_in_universe",
               "fuhrer_n_differential_ions", "fuhrer_category", "fuhrer_growth_rate")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)}/ rather than beside this "
                         f"script")
    a = ap.parse_args()

    gem = pd.read_parquet(HOST_GEM)
    gem_id = str(gem["unit_id"].iloc[0])
    model = gem[gem["feature_kind"] == "gem_gene"]
    symbol = {str(o): str(n) for o, n in zip(model["orf"], model["feature_name"]) if n}
    print(f"{HOST}/{gem_id}: {len(gem):,} rows, {model.orf.nunique():,} model genes, "
          f"{gem.mnxr.nunique():,} MNXR "
          f"({gem[gem.in_atom_universe].mnxr.nunique():,} atom-mapped)")

    universe, ustats = bu.atom_universe(
        BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet",
        exclude=bu.transport_mnxrs(bu.reac_prop_path(METANETX)))
    print(bu.universe_line(ustats, COHORT))
    dn = pd.read_parquet(HOST_DENOVO)
    dn = dn.assign(in_atom_universe=dn["mnxr"].isin(universe))
    dn_lanes = sorted(dn["channel"].astype(str).unique())
    print(f"de-novo table: {len(dn):,} rows, {dn.orf.nunique():,} ORFs, lanes {dn_lanes}")

    assayed, essential = baba_roster.collection()
    print(f"Keio roster: {len(assayed):,} assayed, {len(essential):,} essential "
          f"(Baba 2006 Supplementary Tables 2 and 6)")

    res = pd.read_csv(RESOLUTION, sep="\t", dtype=str).fillna("")
    gem_idx = {o: g for o, g in model.groupby("orf")}
    dn_idx = {o: g for o, g in dn.groupby("orf")}

    gem_parts, dn_parts, census = [], [], []
    for r in res.itertuples(index=False):
        cond = f"{COHORT}:{r.gene}"
        g_hit = gem_idx.get(r.b_number, model.iloc[0:0])
        d_hit = dn_idx.get(r.bw25113_accession, dn.iloc[0:0])
        if len(g_hit):
            gem_parts.append(g_hit.assign(
                source=gem_id, build_id=f"direct_{COHORT}_{gem_id}", host=HOST,
                unit_id=gem_id, feature_kind="mutant_gene", condition_id=cond,
                cohort=COHORT, action="del", source_organism=SOURCE_ORGANISM))
        if len(d_hit):
            dn_parts.append(d_hit.assign(
                source="bw25113_orfs",
                build_id=f"denovo_{COHORT}_" + "+".join(dn_lanes), host=HOST,
                unit_id="bw25113_orfs", feature_kind="mutant_gene",
                feature_name=r.gene, condition_id=cond, cohort=COHORT, action="del",
                source_organism=SOURCE_ORGANISM))
        census.append(dict(
            gene=r.gene, condition_id=cond, b_number=r.b_number,
            model_gene=r.b_number if len(g_hit) else "",
            denovo_feature=r.bw25113_accession if len(d_hit) else "",
            screened=True, assayed=r.b_number in assayed,
            essential=r.b_number in essential,
            gem_n_reactions=int(g_hit["intermediate_id"].nunique()),
            gem_n_mnxr=int(g_hit["mnxr"].nunique()),
            gem_n_in_universe=int(g_hit[g_hit["in_atom_universe"].fillna(False)]
                                  ["mnxr"].nunique()),
            denovo_n_mnxr=int(d_hit["mnxr"].nunique()),
            denovo_n_in_universe=int(d_hit[d_hit["in_atom_universe"]]["mnxr"].nunique()),
            fuhrer_n_differential_ions=r.n_differential_ions,
            fuhrer_category=r.fuhrer_category, fuhrer_growth_rate=r.growth_rate_mean))

    # Model genes the screen never measured: an unlabelled gene, not a negative one.
    screened_orfs = set(res.b_number) - {""}
    for orf in sorted(set(gem_idx) - screened_orfs):
        hit = gem_idx[orf]
        gene = symbol.get(orf) or orf
        census.append(dict(
            gene=gene, condition_id="", b_number=orf, model_gene=orf, denovo_feature="",
            screened=False, assayed=orf in assayed, essential=orf in essential,
            gem_n_reactions=int(hit["intermediate_id"].nunique()),
            gem_n_mnxr=int(hit["mnxr"].nunique()),
            gem_n_in_universe=int(hit[hit["in_atom_universe"].fillna(False)]
                                  ["mnxr"].nunique()),
            denovo_n_mnxr=0, denovo_n_in_universe=0,
            fuhrer_n_differential_ions="", fuhrer_category="", fuhrer_growth_rate=""))

    cen = pd.DataFrame(census)[list(CENSUS_COLS)]
    dupes = cen.condition_id[(cen.condition_id != "") & cen.condition_id.duplicated()]
    if len(dupes):
        raise SystemExit(f"[fuhrer-gpr] {len(dupes)} colliding condition ids, "
                         f"e.g. {dupes.tolist()[:5]}")

    cols = fe.schema_for(EXTENSIONS)

    def assemble(parts, like):
        df = (pd.concat(parts, ignore_index=True) if parts
              else like.reindex(columns=cols))
        return (df[cols].sort_values(fe.grain_key(EXTENSIONS), kind="mergesort")
                        .reset_index(drop=True))

    gem_df = assemble(gem_parts, gem)
    dn_df = assemble(dn_parts, dn)
    dn_df["raw_score"] = dn_df["raw_score"].astype(np.float32)
    fe.validate_gpr(gem_df, GEM_LANE_SET, None, gem_id, EXTENSIONS)
    fe.validate_gpr(dn_df, DENOVO_LANE_SET, None, "bw25113_orfs", EXTENSIONS)

    scr = cen[cen.screened]
    print(f"\ncurated : {len(gem_df):,} rows | "
          f"{int((scr.model_gene != '').sum()):,}/{len(scr):,} screened deletions are "
          f"iML1515 genes, {int((scr.gem_n_in_universe > 0).sum()):,} of them atom-mapped")
    print(f"de-novo : {len(dn_df):,} rows | "
          f"{int((scr.denovo_feature != '').sum()):,}/{len(scr):,} matched a BW25113 ORF, "
          f"{int((scr.denovo_n_mnxr > 0).sum()):,} with a reaction, "
          f"{int((scr.denovo_n_in_universe > 0).sum()):,} atom-mapped")
    print(f"census  : {len(cen):,} rows | {int(cen.screened.sum()):,} screened, "
          f"{int((~cen.screened).sum()):,} model genes the screen never measured, "
          f"{int(cen.assayed.sum()):,} on the Keio roster, "
          f"{int(cen.essential.sum()):,} essential")
    off = scr[~scr.assayed]
    if len(off):
        print(f"    !! {len(off):,} screened deletions are not on the Keio roster "
              f"(Baba names {len(assayed):,} of ~3,985 targeted): "
              f"{sorted(off.gene)[:8]}")

    out_dir = OUT if a.publish else (HERE / "out" / "gpr")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in (("gpr_gem.parquet", gem_df), ("gpr_denovo.parquet", dn_df)):
        (out_dir / name).unlink(missing_ok=True)
        df.to_parquet(out_dir / name, index=False, compression="zstd")
    (out_dir / "mutant_census.tsv").unlink(missing_ok=True)
    cen.to_csv(out_dir / "mutant_census.tsv", sep="\t", index=False)

    manifest = dict(
        host=HOST, gem_unit_id=gem_id, action="del", cohort=COHORT,
        host_gpr=dict(gem=str(HOST_GEM.relative_to(REPO)),
                      denovo=str(HOST_DENOVO.relative_to(REPO))),
        resolution=str(RESOLUTION.relative_to(REPO)),
        roster=dict(source="Baba 2006 Supplementary Tables 2 and 6",
                    assayed=len(assayed), essential=len(essential)),
        universe=ustats, denovo_lanes=dn_lanes,
        model_genes=int(model.orf.nunique()),
        screened=int(cen.screened.sum()),
        census_rows=int(len(cen)),
        gem=dict(rows=int(len(gem_df)), mnxr=int(gem_df.mnxr.nunique()),
                 genes=int((scr.model_gene != "").sum()),
                 atom_mapped=int((scr.gem_n_in_universe > 0).sum())),
        denovo=dict(rows=int(len(dn_df)), mnxr=int(dn_df.mnxr.nunique()),
                    genes_resolved=int((scr.denovo_feature != "").sum()),
                    genes_with_mnxr=int((scr.denovo_n_mnxr > 0).sum()),
                    atom_mapped=int((scr.denovo_n_in_universe > 0).sum())),
        off_roster=int(len(off)),
    )
    (out_dir / "BUILD_fuhrer_gpr.json").write_text(json.dumps(manifest, indent=2))
    print(f"\n-> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
