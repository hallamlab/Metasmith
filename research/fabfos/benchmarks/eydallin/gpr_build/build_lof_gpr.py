#!/usr/bin/env python3
"""The 2007 deletion arm's edges: the 65 mutants Eydallin REPORTED and the collection
they were screened out of, both read off iML1515.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/eydallin/gpr_build/build_lof_gpr.py
    ... --publish        # writes runs/eydallin_clones/gpr/lof/ and runs/keio/gpr/

ONE SCRIPT, TWO TABLES, ON PURPOSE. The GOF arm splits this in two --
`build_clone_gpr.py` for the 86 reported clones and `build_aska_gpr.py` for the 4,123
assayed ones -- because those two read different channels off different strains through
different id spaces. Here the cohort is a strict row-subset of the library: same model,
same b-number join, same background. Two scripts would be two resolvers over one model,
which is the failure this tree keeps writing warnings about, so there is one resolver and
the cohort is a filter applied at the end.

The reported 65 are `runs/eydallin_clones/gpr/lof/`, beside the GOF cohort's own
`gpr/gpr_gem.parquet` and mirroring how `parse/` already splits `gof/` from `lof/`. The
library is `runs/keio/gpr/`, its own cohort directory the way `runs/aska/` is the ASKA
library's -- the population is the Keio collection, not the Eydallin paper.

A DELETION IS `action=del`, WHICH IS NOT WHAT THE GOF TABLES SAY. Those carry
`action=add`: an ASKA clone is a second copy of a gene the host already has. A Keio
mutant is the gene's absence. The two arms therefore disagree about the sign of every
perturbation by construction, and `action` is the column that records it -- it is part
of the grain key precisely so one condition can carry both.

EVERY MODEL GENE GETS A CENSUS ROW, including one the collection never carried. The
census is what makes the library a denominator: `assayed` says the Keio collection has a
mutant for this gene, and a gene it does not have was never measured, so it is an
unlabelled gene rather than a negative one. Dropping those rows here would hide the
distinction from every statistic downstream; carrying the flag lets the analysis exclude
them and say how many it excluded.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "research" / "fabfos" / "benchmarks" / "keio"))
import baba_roster                                                    # noqa: E402

sys.path.insert(0, str(REPO / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe                                          # noqa: E402

HOST_GPR = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet"
RESOLUTION = REPO / "data/fabfos/runs/eydallin_clones/parse/lof/lof_resolution.tsv"
EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin_2007/extraction.tsv"
COHORT_OUT = REPO / "data/fabfos/runs/eydallin_clones/gpr/lof"
LIBRARY_OUT = REPO / "data/fabfos/runs/keio/gpr"

HOST = "e_coli_bw25113"
COHORT = "eydallin2007"
LIBRARY = "keio"
SOURCE_ORGANISM = "e_coli_bw25113"

EXTENSIONS = ("attribution", "feature", "universe", "cohort")
LANE_SET = "curated"

CENSUS_COLS = ("gene", "condition_id", "b_number", "model_gene", "n_reactions",
               "n_mnxr", "n_in_universe", "assayed", "essential",
               "eydallin_gene", "eydallin_phenotype")


def stamp(hit: pd.DataFrame, cohort: str, cond: str, gem_id: str) -> pd.DataFrame:
    return hit.assign(source=gem_id, build_id=f"direct_{cohort}_{gem_id}", host=HOST,
                      unit_id=gem_id, feature_kind="mutant_gene", condition_id=cond,
                      cohort=cohort, action="del", source_organism=SOURCE_ORGANISM)


def assemble(parts: list, like: pd.DataFrame, cohort: str, gem_id: str) -> pd.DataFrame:
    cols = fe.schema_for(EXTENSIONS)
    df = pd.concat(parts, ignore_index=True) if parts else like.reindex(columns=cols)
    df = (df[cols].sort_values(fe.grain_key(EXTENSIONS), kind="mergesort")
                  .reset_index(drop=True))
    return fe.validate_gpr(df, LANE_SET, None, gem_id, EXTENSIONS)


def write(out_dir: Path, df: pd.DataFrame, census: pd.DataFrame) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # DVC hands a checkout out as a read-only hardlink into its cache, so writing
    # through one would mutate the shared cache object. Unlink for a fresh inode.
    (out_dir / "gpr_gem.parquet").unlink(missing_ok=True)
    df.to_parquet(out_dir / "gpr_gem.parquet", index=False, compression="zstd")
    census[list(CENSUS_COLS)].to_csv(out_dir / "mutant_census.tsv", sep="\t", index=False)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {COHORT_OUT.relative_to(REPO)}/ and "
                         f"{LIBRARY_OUT.relative_to(REPO)}/ rather than beside this script")
    a = ap.parse_args()

    host = pd.read_parquet(HOST_GPR)
    gem_id = str(host["unit_id"].iloc[0])
    model = host[host["feature_kind"] == "gem_gene"]
    by_orf = {o: g for o, g in model.groupby("orf")}
    symbol = {str(o): str(n) for o, n in zip(model["orf"], model["feature_name"]) if n}
    print(f"{HOST}/{gem_id}: {len(host):,} rows, {len(by_orf):,} model genes, "
          f"{host['mnxr'].nunique():,} MNXR "
          f"({host[host['in_atom_universe']]['mnxr'].nunique():,} atom-mapped)")

    assayed, essential = baba_roster.collection()
    print(f"Keio roster: {len(assayed):,} assayed, {len(essential):,} essential "
          f"(Baba 2006 Supplementary Tables 2 and 6)")

    lof = list(csv.DictReader(RESOLUTION.open(), delimiter="\t"))
    pheno = {r["gene"]: r["phenotype"]
             for r in csv.DictReader(EXTRACTION.open(), delimiter="\t")}
    if len(lof) != len(pheno):
        raise SystemExit(f"[lof-gpr] {len(lof)} resolution rows against {len(pheno)} "
                         f"extraction rows -- the two disagree about the cohort")
    # The paper's name, not the model's: `condition_id` is what the digitised Y table
    # is keyed on, and six of these names were retired after 2007.
    cohort_by_orf = {r["b_number"]: r["gene"] for r in lof if r["b_number"]}
    missing_b = [r["gene"] for r in lof if not r["b_number"]]
    if missing_b:
        raise SystemExit(f"[lof-gpr] no b-number for {missing_b}")

    lib_parts, coh_parts, census, by_bnumber = [], [], [], {}
    for orf in sorted(by_orf):
        hit = by_orf[orf]
        gene = symbol.get(orf) or orf
        ey = cohort_by_orf.get(orf, "")
        rec = dict(gene=gene, condition_id=f"{LIBRARY}:{gene}", b_number=orf,
                   model_gene=orf,
                   n_reactions=int(hit["intermediate_id"].nunique()),
                   n_mnxr=int(hit["mnxr"].nunique()),
                   n_in_universe=int(hit[hit["in_atom_universe"]]["mnxr"].nunique()),
                   assayed=orf in assayed, essential=orf in essential,
                   eydallin_gene=ey, eydallin_phenotype=pheno.get(ey, ""))
        census.append(rec)
        by_bnumber[orf] = rec
        lib_parts.append(stamp(hit, LIBRARY, rec["condition_id"], gem_id))
        if ey:
            coh_parts.append(stamp(hit, COHORT, f"{COHORT}:{ey}", gem_id))

    cen = pd.DataFrame(census)
    dupes = cen.gene[cen.gene.duplicated()].tolist()
    if dupes:
        raise SystemExit(f"[lof-gpr] {len(dupes)} model symbols collide as condition "
                         f"ids, e.g. {dupes[:5]} -- the census is keyed on `gene`")

    lib = assemble(lib_parts, host, LIBRARY, gem_id)
    coh = assemble(coh_parts, host, COHORT, gem_id)

    # The cohort census is the PAPER'S 65, not the 38 iML1515 happens to name. A mutant
    # the model cannot see was still built, stained and measured, and it belongs in the
    # denominator of every rate quoted about this arm -- dropping it would turn "23 of
    # 65 measurable" into "23 of 38" and flatter the method by a third.
    coh_cen = pd.DataFrame([
        dict(by_bnumber.get(r["b_number"], {}),
             gene=r["gene"], condition_id=f"{COHORT}:{r['gene']}",
             b_number=r["b_number"], eydallin_gene=r["gene"],
             eydallin_phenotype=pheno[r["gene"]],
             assayed=r["b_number"] in assayed,
             essential=r["b_number"] in essential)
        for r in lof]).reindex(columns=list(CENSUS_COLS))
    for c in ("n_reactions", "n_mnxr", "n_in_universe"):
        coh_cen[c] = coh_cen[c].fillna(0).astype(int)
    coh_cen["model_gene"] = coh_cen.model_gene.fillna("")
    coh_cen = coh_cen.sort_values("gene").reset_index(drop=True)
    unseen = sorted(coh_cen.loc[coh_cen.model_gene == "", "gene"])
    print(f"\ncohort  : {len(coh):,} rows over "
          f"{int((coh_cen.model_gene != '').sum())}/{len(coh_cen)} mutants iML1515 "
          f"names, {int((coh_cen.n_in_universe > 0).sum())} of them atom-mapped\n"
          f"          {len(unseen)} name no model gene at all")
    off_roster = sorted(coh_cen.loc[~coh_cen.assayed, "gene"])
    if off_roster:
        print(f"    !! {len(off_roster)} reported mutant(s) are not on the Keio "
              f"roster: {off_roster}")

    lib_assayed = cen[cen.assayed]
    print(f"library : {len(lib):,} rows over {len(cen):,} model genes | "
          f"{int(cen.assayed.sum()):,} assayed, {int(cen.essential.sum()):,} essential, "
          f"{int((~cen.assayed & ~cen.essential).sum()):,} on neither roster\n"
          f"          {int((lib_assayed.n_in_universe > 0).sum()):,} assayed genes carry "
          f"an atom-mapped reaction")

    cohort_dir = COHORT_OUT if a.publish else (HERE / "out" / "lof_gpr")
    library_dir = LIBRARY_OUT if a.publish else (HERE / "out" / "keio_gpr")
    write(cohort_dir, coh, coh_cen)
    write(library_dir, lib, cen)

    manifest = dict(
        host=HOST, gem_unit_id=gem_id, action="del",
        host_gpr=str(HOST_GPR.relative_to(REPO)),
        roster=dict(source="Baba 2006 Supplementary Tables 2 and 6",
                    assayed=len(assayed), essential=len(essential)),
        model_genes=int(len(cen)),
        cohort=dict(name=COHORT, reported=len(lof), named_in_model=int(len(coh_cen)),
                    atom_mapped=int((coh_cen.n_in_universe > 0).sum()),
                    rows=int(len(coh)), mnxr=int(coh.mnxr.nunique()),
                    not_named=unseen, off_roster=off_roster),
        library=dict(name=LIBRARY, rows=int(len(lib)), mnxr=int(lib.mnxr.nunique()),
                     assayed=int(cen.assayed.sum()),
                     essential=int(cen.essential.sum()),
                     unrostered=int((~cen.assayed & ~cen.essential).sum()),
                     assayed_atom_mapped=int((lib_assayed.n_in_universe > 0).sum())),
    )
    (library_dir / "BUILD_lof_gpr.json").write_text(json.dumps(manifest, indent=2))
    print(f"\n-> {cohort_dir}\n-> {library_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
