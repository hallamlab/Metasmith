#!/usr/bin/env python3
"""The WHOLE ASKA library's edges against MG1655, on both channels. The population Fang
screened, and the background the sweep folds against.

    mamba run -n msm python research/fabfos/benchmarks/fang/gpr_build/build_fang_gpr.py
    ... --publish        # writes data/fabfos/runs/fang/gpr/

`build_aska_gpr.py` does exactly this for the eydallin arm and this is that script with one
substitution: the host. Eydallin screened in AG1, which has no curated model of its own, so
that build keys every gene against DH1's `iECDH1ME8569_1439` as a proxy. Fang screened in
MG1655(DE3) delta-fadE, and **iML1515 is MG1655's own model** -- so the same join is a
direct statement about the assay host rather than a borrowed one. The DE3 lysogeny carries
T7 RNA polymerase and touches no reaction iML1515 curates; fadE's deletion is not applied,
which makes this the wild-type background and every sweep number a difference from it.

NEITHER CHANNEL RUNS AN ANNOTATOR, for the reason `build_aska_gpr.py` gives: an ASKA clone
is a chromosomal E. coli ORF, so the host already carries every gene the library
overexpresses and "what does this clone add" is answerable from the host's own annotation.
The clone is a second copy, not a new gene -- which is also why the sweep MULTIPLIES a
reaction's weight instead of unioning the clone's reactions into the background, where the
whole library would be a no-op.

THE de-novo TABLE'S `in_atom_universe` COLUMN IS ALL-NULL and must not be filtered on.
`runs/e_coli_k12/gpr/gpr_denovo.parquet` leaves it for its consumer to compute; a filter on
it silently yields an empty background, an empty graph, and a run in which nothing is
reachable -- which reads like a result. The universe is recomputed here from the bake, with
transport excluded, exactly as `sink_panel.py` and `probe_axes.py` do.

EVERY CLONE GETS A CENSUS ROW, including one that resolves to nothing, so the population
stays "clones the screen assayed" rather than "clones the model can see". The Fang labels
are attached here rather than downstream: `fang_gene`/`fang_direction`/`fang_ffa_mg_l`
carry the paper's own reverse-genetics verdict onto the roster, and `assayed_individually`
says whether a clone was rebuilt and put through GC at all -- which is the distinction the
whole statistics section of this arm turns on.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _repo_root(start: Path) -> Path:
    for d in [start, *start.parents]:
        if (d / "data/fabfos").is_dir():
            return d
    raise SystemExit(f"no ancestor of {start} contains data/fabfos")


REPO = _repo_root(Path(__file__).resolve())
HERE = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(REPO / "research/fabfos/benchmarks/aska"))
sys.path.insert(0, str(HERE / "parse"))
import build_gof_table as bgt                                         # noqa: E402
from build_extraction import gene_to_bnumber                          # noqa: E402

sys.path.insert(0, str(REPO / "src/fabfos/build_references/resources/buildlib"))
import bench_universe as bu                                           # noqa: E402

sys.path.insert(0, str(REPO / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe                                          # noqa: E402

ROSTER = REPO / "data/fabfos/originals/benchmarks/aska/library/aska_clone_minus.tsv"
MG1655_GBK = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
MG1655_FAA = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.faa"
HOST_GEM = REPO / "data/fabfos/runs/e_coli_k12/gpr/gpr_gem.parquet"
HOST_DENOVO = REPO / "data/fabfos/runs/e_coli_k12/gpr/gpr_denovo.parquet"
BAKE = REPO / "data/fabfos/processed/metabolism_bake"
METANETX = REPO / "data/fabfos/originals/metanetx"
GOF_DIR = REPO / "data/fabfos/runs/fang_clones/parse/gof"
EXTRACTION = REPO / "data/fabfos/benchmarks/aska_ffa/extraction.tsv"
OUT = REPO / "data/fabfos/runs/fang/gpr"

HOST = "e_coli_k12"
COHORT = "fang"
SOURCE_ORGANISM = "e_coli_w3110"

EXTENSIONS = ("attribution", "feature", "universe", "cohort")
GEM_LANE_SET = "curated"
DENOVO_LANE_SET = "chosen_4"


def faa_index(faa: Path) -> tuple[dict[str, str], dict[str, str]]:
    gene, by_tag = {}, {}
    for line in faa.open():
        if not line.startswith(">"):
            continue
        fid = line[1:].split(None, 1)[0]
        g = re.search(r"\[gene=([^\]]+)\]", line)
        t = re.search(r"\[locus_tag=([^\]]+)\]", line)
        if g:
            gene[fid] = g.group(1)
        if t:
            by_tag.setdefault(t.group(1), fid)
    return gene, by_tag


def symbol_for_bnumber(faa: Path) -> dict[str, str]:
    out = {}
    for line in faa.open():
        if not line.startswith(">"):
            continue
        loc = re.search(r"\[locus_tag=([^\]]+)\]", line)
        gene = re.search(r"\[gene=([^\]]+)\]", line)
        if loc and gene:
            out.setdefault(loc.group(1), gene.group(1))
    return out


def read_roster(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", dtype=str).fillna("")
    df.columns = [c.strip() for c in df.columns]
    df["gene"] = df["Gene Name"].str.strip()
    df["jw_id"] = df["JW ID"].str.strip()
    return df[df.gene != ""].reset_index(drop=True)


def fang_labels() -> dict[str, dict]:
    """paper gene -> its own reverse-genetics verdict, read off the two gof tables and the
    study extraction's direction call. A gene the paper never rebuilt gets nothing, which is
    what `assayed_individually` then reports.

    THE DIRECTION HAS TO BE LOOKED UP PER PANEL, not per strain. Five genes were assayed
    twice -- `ydeA`, `setA` and `setB` in both rounds, `yafL` and `rimM` on both backgrounds
    -- and a plain `strain_id -> measured` map silently keeps whichever row is last in the
    file. That turns `yafL`, up 54% on rfaY, into a flat, which is the paper's headline
    second-round hit scored as a negative."""
    obs = (pd.read_csv(EXTRACTION, sep="\t", dtype=str)
           .drop_duplicates("obs_id").fillna(""))
    out = {}
    for fname, (figs, _ctl, _mg, _desc) in bgt.PANELS.items():
        gof = pd.read_csv(GOF_DIR / fname, dtype=str)
        panel = obs[obs.figure.isin(figs)]
        # first figure in the declared order wins, matching `build_gof_table.py`'s own
        # first-wins pass over the same panel list
        order = {f: i for i, f in enumerate(figs)}
        panel = panel.assign(_o=panel.figure.map(order)).sort_values("_o", kind="mergesort")
        direction = {}
        for sid, measured in zip(panel.strain_id, panel.measured):
            direction.setdefault(sid, measured)
        bg = "RF" if fname.endswith("round2.csv") else "F"
        for r in gof.to_dict("records"):
            rec = out.setdefault(r["gene"], {})
            rec[f"fang_direction_{bg}"] = direction.get(r["gene"], "")
            rec[f"fang_ffa_mg_l_{bg}"] = float(r["ffa_titer_mg_l"])
    for gene, rec in out.items():
        # A gene is a Fang BENEFICIAL TARGET if it raised the titer on EITHER background --
        # `yafL` and `rimM` are flat alone and up on rfaY, and the paper's claim is about the
        # combination. `fang_background` records which panel the `up` came from, so a
        # positive is never reported without the background it was measured on.
        up = [bg for bg in ("F", "RF") if rec.get(f"fang_direction_{bg}") == "up"]
        rec["fang_direction"] = "up" if up else (rec.get("fang_direction_F")
                                                 or rec.get("fang_direction_RF") or "")
        rec["fang_background"] = up[0] if up else (
            "F" if "fang_direction_F" in rec else "RF")
        rec["fang_ffa_mg_l"] = rec.get(f"fang_ffa_mg_l_{rec['fang_background']}", np.nan)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--publish", action="store_true",
                    help=f"write into {OUT.relative_to(REPO)} rather than beside this script")
    a = ap.parse_args()
    out_dir = OUT if a.publish else (HERE / "out" / "fang_gpr")

    roster = read_roster(ROSTER)
    genes = sorted(roster.gene.unique())
    print(f"roster {ROSTER.name}: {len(roster):,} clones, {len(genes):,} distinct gene names")

    to_bnum = gene_to_bnumber(MG1655_GBK)
    sym_for_b = symbol_for_bnumber(MG1655_FAA)
    dn_gene, dn_by_tag = faa_index(MG1655_FAA)

    gem = pd.read_parquet(HOST_GEM)
    gem_id = str(gem["unit_id"].iloc[0])
    gem_by_symbol: dict[str, set[str]] = {}
    for name, fid in zip(gem[gem.feature_kind == "gem_gene"]["feature_name"],
                         gem[gem.feature_kind == "gem_gene"]["orf"]):
        if name:
            gem_by_symbol.setdefault(str(name), set()).add(str(fid))

    dn_by_symbol: dict[str, set[str]] = {}
    for fid, g in dn_gene.items():
        dn_by_symbol.setdefault(g, set()).add(fid)

    def resolve(gene: str, current: str, by_symbol: dict, by_tag=None, to_tag=None):
        for key, how in ((current, "current_symbol"), (gene, "roster_symbol")):
            if key and key in by_symbol:
                return by_symbol[key], how
        if by_tag is not None and to_tag is not None:
            tag = to_tag.get(gene) or (to_tag.get(current) if current else None)
            if tag and tag in by_tag:
                return {by_tag[tag]}, "genome_synonym"
        return set(), "unresolved"

    universe, ustats = bu.atom_universe(
        BAKE / "vocab.parquet", BAKE / "atom_pairs.parquet",
        exclude=bu.transport_mnxrs(bu.reac_prop_path(METANETX)))
    print(bu.universe_line(ustats, COHORT))

    dn = pd.read_parquet(HOST_DENOVO)
    dn = dn.assign(in_atom_universe=dn["mnxr"].isin(universe))
    dn_lanes = sorted(dn["channel"].astype(str).unique())

    gem_parts, dn_parts = [], []
    gem_idx = {f: g for f, g in gem.groupby("orf")}
    dn_idx = {f: g for f, g in dn.groupby("orf")}

    per_gene = {}
    for gene in genes:
        b = to_bnum.get(gene, "")
        current = sym_for_b.get(b, "")
        cond = f"{COHORT}:{gene}"

        g_fids, g_how = resolve(gene, current, gem_by_symbol)
        d_fids, d_how = resolve(gene, current, dn_by_symbol, dn_by_tag, to_bnum)

        g_hit = (pd.concat([gem_idx[f] for f in sorted(g_fids) if f in gem_idx])
                 if any(f in gem_idx for f in g_fids) else gem.iloc[0:0])
        d_hit = (pd.concat([dn_idx[f] for f in sorted(d_fids) if f in dn_idx])
                 if any(f in dn_idx for f in d_fids) else dn.iloc[0:0])

        if len(g_hit):
            gem_parts.append(g_hit.assign(
                source=gem_id, build_id=f"direct_{COHORT}_{gem_id}", host=HOST,
                unit_id=gem_id, feature_kind="clone_gene", condition_id=cond,
                cohort=COHORT, action="add", source_organism=SOURCE_ORGANISM))
        if len(d_hit):
            dn_parts.append(d_hit.assign(
                source="aska_orfs",
                build_id=f"denovo_{COHORT}_" + "+".join(dn_lanes), host=HOST,
                unit_id="aska_orfs", feature_kind="clone_gene",
                feature_name=gene, condition_id=cond, cohort=COHORT, action="add",
                source_organism=SOURCE_ORGANISM))

        per_gene[gene] = dict(
            gene=gene, condition_id=cond, b_number=b, current_symbol=current,
            gem_feature=";".join(sorted(g_fids)), gem_resolved_via=g_how,
            gem_n_mnxr=int(g_hit["mnxr"].nunique()),
            gem_n_in_universe=int(g_hit[g_hit["in_atom_universe"].fillna(False)]["mnxr"].nunique()),
            denovo_feature=";".join(sorted(d_fids)), denovo_resolved_via=d_how,
            denovo_n_mnxr=int(d_hit["mnxr"].nunique()),
            denovo_n_in_universe=int(d_hit[d_hit["in_atom_universe"]]["mnxr"].nunique()))

    cen = pd.DataFrame([dict(jw_id=r.jw_id, **per_gene[r.gene])
                        for r in roster.itertuples(index=False)])

    cols = fe.schema_for(EXTENSIONS)

    def assemble(parts, like):
        df = (pd.concat(parts, ignore_index=True) if parts else like.reindex(columns=cols))
        return (df[cols].sort_values(fe.grain_key(EXTENSIONS), kind="mergesort")
                        .reset_index(drop=True))

    gem_df = assemble(gem_parts, gem)
    dn_df = assemble(dn_parts, dn)
    dn_df["raw_score"] = dn_df["raw_score"].astype(np.float32)
    fe.validate_gpr(gem_df, GEM_LANE_SET, None, gem_id, EXTENSIONS)
    fe.validate_gpr(dn_df, DENOVO_LANE_SET, None, "aska_orfs", EXTENSIONS)

    labels = fang_labels()
    roster_by_b, roster_lower = {}, {}
    g1 = cen.drop_duplicates("gene")
    for g, b in zip(g1.gene, g1.b_number.fillna("")):
        if b:
            roster_by_b.setdefault(b, g)
        roster_lower.setdefault(g.lower(), g)
    resolved, unlabelled = {}, []
    for paper_gene, rec in labels.items():
        hit = next((h for h in (roster_lower.get(paper_gene.lower()),
                                roster_by_b.get(to_bnum.get(paper_gene, ""))) if h), None)
        if hit is None:
            unlabelled.append(paper_gene)
        else:
            resolved.setdefault(hit, dict(fang_gene=paper_gene, **rec))
    for col, blank in (("fang_gene", ""), ("fang_direction", ""), ("fang_background", ""),
                       ("fang_direction_F", ""), ("fang_direction_RF", ""),
                       ("fang_ffa_mg_l", np.nan), ("fang_ffa_mg_l_F", np.nan),
                       ("fang_ffa_mg_l_RF", np.nan)):
        cen[col] = cen.gene.map(lambda g, c=col, b=blank: resolved.get(g, {}).get(c, b))
    cen["assayed_individually"] = cen.fang_gene != ""
    cen["is_positive"] = cen.fang_direction == "up"

    out_dir.mkdir(parents=True, exist_ok=True)
    gem_df.to_parquet(out_dir / "gpr_gem.parquet", index=False, compression="zstd")
    dn_df.to_parquet(out_dir / "gpr_denovo.parquet", index=False, compression="zstd")
    cen.to_csv(out_dir / "clone_census.tsv", sep="\t", index=False)
    g1 = cen.drop_duplicates("gene")

    pos = g1[g1.fang_gene != ""]
    manifest = dict(
        roster=str(ROSTER.relative_to(REPO)), roster_clones=int(len(roster)),
        roster_genes=int(len(genes)), host=HOST, cohort=COHORT, gem_unit_id=gem_id,
        denovo_lanes=dn_lanes, universe=ustats,
        gem=dict(rows=int(len(gem_df)),
                 genes_resolved=int((g1.gem_feature != "").sum()),
                 genes_with_mnxr=int((g1.gem_n_mnxr > 0).sum()),
                 genes_in_universe=int((g1.gem_n_in_universe > 0).sum()),
                 mnxr=int(gem_df.mnxr.nunique())),
        denovo=dict(rows=int(len(dn_df)),
                    genes_resolved=int((g1.denovo_feature != "").sum()),
                    genes_with_mnxr=int((g1.denovo_n_mnxr > 0).sum()),
                    genes_in_universe=int((g1.denovo_n_in_universe > 0).sum()),
                    mnxr=int(dn_df.mnxr.nunique())),
        resolved_via=dict(gem=g1.gem_resolved_via.value_counts().to_dict(),
                          denovo=g1.denovo_resolved_via.value_counts().to_dict()),
        fang=dict(assayed=int(len(labels)), labelled=int(len(pos)),
                  unlabelled=sorted(unlabelled),
                  up=int((pos.fang_direction == "up").sum()),
                  down=int((pos.fang_direction == "down").sum()),
                  flat=int((pos.fang_direction == "flat").sum()),
                  gem_in_universe=int((pos.gem_n_in_universe > 0).sum()),
                  denovo_in_universe=int((pos.denovo_n_in_universe > 0).sum())),
    )
    (out_dir / "BUILD_fang_gpr.json").write_text(json.dumps(manifest, indent=2))

    print(f"\ncurated : {len(gem_df):,} rows | {manifest['gem']['genes_resolved']:,} genes "
          f"named in {gem_id}, {manifest['gem']['genes_with_mnxr']:,} with a reaction, "
          f"{manifest['gem']['genes_in_universe']:,} atom-mapped")
    print(f"de-novo : {len(dn_df):,} rows | {manifest['denovo']['genes_resolved']:,} genes "
          f"matched an MG1655 ORF, {manifest['denovo']['genes_with_mnxr']:,} with a "
          f"reaction, {manifest['denovo']['genes_in_universe']:,} atom-mapped")
    print(f"resolved via: {manifest['resolved_via']}")
    f = manifest["fang"]
    print(f"fang: {f['labelled']}/{f['assayed']} individually-assayed genes found on the "
          f"roster ({f['up']} up, {f['down']} down, {f['flat']} flat); "
          f"{f['gem_in_universe']} atom-mapped curated, {f['denovo_in_universe']} de-novo")
    if f["unlabelled"]:
        print(f"    NOT on the ASKA roster under any name/b-number: {f['unlabelled']}")
    print(f"\n-> {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
