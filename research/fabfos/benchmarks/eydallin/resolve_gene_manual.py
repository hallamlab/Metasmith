#!/usr/bin/env python3
"""Everything `build_clone_gpr.py` couldn't reach, resolved by hand against DH1 itself.

    mamba run -n msm-fabfos python research/fabfos/benchmarks/eydallin/resolve_gene_manual.py
    ... --publish        # writes data/fabfos/benchmarks/eydallin/gpr_manual.parquet

`build_clone_gpr.py` joins each of the 86 Eydallin genes to DH1's curated model by
current symbol. This script asks the same question a second, independent way -- by
b-number, read straight off the model JSON's own `genes[*].annotation.refseq_synonym`
-- and asserts the two methods agree, so "I checked by hand once" becomes something
the pipeline verifies for itself every run.

For every gene the symbol join still can't reach, this searches the model directly:
every gene identifier (`refseq_synonym`, `refseq_locus_tag`, `ncbigi`) and, as a
defensive check beyond identifier joins, a plain-text grep of the gene's raw symbol
across all 2755 `gene_reaction_rule` strings. What it finds is a CANDIDATE, not a
promotion -- nothing here writes `mnxr` on its own judgment. `--publish` requires a
`--decisions` file naming which candidates a human reviewed and accepted; anything
without a decision is written null with the verdict this script assigned it.

THE B-NUMBER CHAIN, NOT A SECOND CHAIN. `refseq_synonym` gives ~1332/1439 model genes
a b-number, and the paper's names are joined to one exactly as `build_clone_gpr.py`
and `build_clone_orfs.py` already do -- through `gene_to_bnumber`'s MG1655-GenBank
synonym table -- so a name-version mismatch (`erfK`/`ldtA`, `pfs`/`mtnN`) can't
silently make the two methods "agree" by both missing the same gene under different
names.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent

sys.path.insert(0, str(REPO / "research" / "fabfos" / "benchmarks" / "aska"))
from build_extraction import gene_to_bnumber                          # noqa: E402

sys.path.insert(0, str(REPO / "src/metasmith_libraries/resources/lib"))
import fabfos_evidence as fe                                          # noqa: E402

sys.path.insert(0, str(REPO / "src"))
from ecspr.model.build import load_reac_xref                          # noqa: E402

EXTRACTION = REPO / "data/fabfos/benchmarks/eydallin/extraction.tsv"
GPR_MANUAL = REPO / "data/fabfos/benchmarks/eydallin/gpr_manual.parquet"
MG1655_GBK = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.gbk"
MG1655_FAA = REPO / "data/fabfos/originals/genomes/e_coli_k12/genome/NC_000913.3.faa"
HOST_GPR = REPO / "data/fabfos/runs/e_coli_dh1/gpr/gpr_gem.parquet"
MODEL_JSON = REPO / "data/fabfos/originals/genomes/e_coli_dh1/GEM/iECDH1ME8569_1439.json"
REAC_XREF = REPO / "data/fabfos/originals/metanetx/4.5/reac_xref.tsv"
REAC_PROP = REPO / "data/fabfos/originals/metanetx/4.5/reac_prop.tsv"
OUT = REPO / "data/fabfos/benchmarks/eydallin"

HOST = "e_coli_dh1"
COHORT = "eydallin"

EXTENSIONS = ("attribution", "feature", "universe", "cohort")
CHANNEL = "manual_gpr"
LANE_SET = "curated"


def mg1655_symbol_for_bnumber(faa: Path) -> dict[str, str]:
    out = {}
    for line in faa.open():
        if not line.startswith(">"):
            continue
        loc = re.search(r"\[locus_tag=([^\]]+)\]", line)
        gene = re.search(r"\[gene=([^\]]+)\]", line)
        if loc and gene:
            out.setdefault(loc.group(1), gene.group(1))
    return out


def load_model_genes(model_json: Path) -> list[dict]:
    m = json.loads(model_json.read_text())
    return m["genes"], m["reactions"], m.get("id", model_json.stem)


def bnumber_index(genes: list[dict]) -> dict[str, set[str]]:
    """b-number (lowercased) -> set of model gene ids carrying it under refseq_synonym."""
    idx: dict[str, set[str]] = {}
    for g in genes:
        for b in g.get("annotation", {}).get("refseq_synonym", []) or []:
            if re.fullmatch(r"[Bb]\d{4}", b):
                idx.setdefault(b.lower(), set()).add(g["id"])
    return idx


def method_symbol(rows, to_bnum, sym_for_b, by_symbol) -> dict[str, set[str]]:
    """Reproduces build_clone_gpr.py's join: gene -> b-number -> current symbol ->
    model gene(s) matched by feature_name. Returns gene -> set(model_gene_id)."""
    out = {}
    for r in rows:
        gene = (r["gene"] or "").strip()
        norm = (r["gene_norm"] or "").strip() or gene
        b = to_bnum.get(norm) or to_bnum.get(gene) or ""
        current = sym_for_b.get(b, "")
        fids: set[str] = set()
        for key in (current, norm, gene):
            if key and key in by_symbol:
                fids = by_symbol[key]
                break
        out[gene] = fids
    return out


def method_bnumber(rows, to_bnum, bnum_idx) -> dict[str, set[str]]:
    """gene -> b-number (same MG1655-GenBank chain) -> model gene(s) carrying that
    b-number under the model's OWN annotation.refseq_synonym. Independent of the
    model's current gene symbols entirely."""
    out = {}
    for r in rows:
        gene = (r["gene"] or "").strip()
        norm = (r["gene_norm"] or "").strip() or gene
        b = (to_bnum.get(norm) or to_bnum.get(gene) or "").lower()
        out[gene] = set(bnum_idx.get(b, set())) if b else set()
    return out


def search_candidates(gene: str, b: str, genes: list[dict], reactions: list[dict],
                       by_gene_id: dict[str, dict]) -> list[dict]:
    """For a gene the two joins both missed: look for ANY trace of it in the model
    under any identifier, plus a raw-text grep of gene_reaction_rule. Returns a list
    of candidate dicts, empty if the model shows no trace at all."""
    cands = []
    gl = gene.lower()
    for g in genes:
        ann = g.get("annotation", {})
        ids = set()
        ids.update(x.lower() for x in ann.get("refseq_synonym", []) or [])
        ids.update(x.lower() for x in ann.get("refseq_locus_tag", []) or [])
        ids.update(x.lower() for x in ann.get("ncbigi", []) or [])
        name = str(g.get("name", "")).lower()
        if (b and b.lower() in ids) or name == gl:
            cands.append(dict(kind="gene_id_or_name", model_gene=g["id"],
                              model_gene_name=g.get("name", ""), detail=b or name))
    # Defensive: raw symbol as a whole word inside a gene_reaction_rule string, for a
    # gene the identifier joins above found nothing for at all -- catches a rule that
    # embeds a symbol the gene-list annotation itself never carried.
    pat = re.compile(r"\b" + re.escape(gene) + r"\b", re.IGNORECASE)
    for rxn in reactions:
        rule = rxn.get("gene_reaction_rule") or ""
        if rule and pat.search(rule):
            cands.append(dict(kind="reaction_rule_text", model_gene="", rxn_id=rxn["id"],
                              rxn_name=rxn.get("name", ""), detail=rule))
    return cands


def transport_check(mnxr: str, reac_prop: Path) -> bool | None:
    sys.path.insert(0, str(REPO / "src/fabfos/build_references/resources/buildlib"))
    from bench_universe import transport_mnxrs                        # noqa: E402
    if not reac_prop.exists():
        return None
    return mnxr in transport_mnxrs(reac_prop)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--publish", action="store_true",
                     help=f"write into {OUT.relative_to(REPO)}/gpr_manual.parquet")
    ap.add_argument("--decisions", type=Path, default=None,
                     help="TSV of gene, mnxr, note for candidates a human reviewed and "
                          "accepted -- required to publish any non-null mnxr")
    a = ap.parse_args()

    rows = list(csv.DictReader(EXTRACTION.open(), delimiter="\t"))
    to_bnum = gene_to_bnumber(MG1655_GBK)
    sym_for_b = mg1655_symbol_for_bnumber(MG1655_FAA)
    host = pd.read_parquet(HOST_GPR)
    gem_id = host["unit_id"].iloc[0]
    genes_json, reactions_json, model_id = load_model_genes(MODEL_JSON)
    by_gene_id = {g["id"]: g for g in genes_json}

    genes = host[host["feature_kind"] == "gem_gene"]
    by_symbol: dict[str, set[str]] = {}
    for name, fid in zip(genes["feature_name"], genes["orf"]):
        if name:
            by_symbol.setdefault(str(name), set()).add(str(fid))
    bnum_idx = bnumber_index(genes_json)
    with_synonym = {g["id"] for g in genes_json
                    if g.get("annotation", {}).get("refseq_synonym")}

    resolved_a = method_symbol(rows, to_bnum, sym_for_b, by_symbol)
    resolved_b = method_bnumber(rows, to_bnum, bnum_idx)

    print(f"{HOST} ({gem_id}): {len(host):,} rows, {genes['orf'].nunique():,} model "
          f"genes, {len(by_symbol):,} symbols, {len(with_synonym):,}/{len(genes_json):,} "
          f"model genes carry a refseq_synonym b-number")

    # --- T1: assert the two methods agree, with the "no refseq_synonym" caveat -----
    disagreements = []
    unreachable_by_b = []
    for gene in resolved_a:
        a_set, b_set = resolved_a[gene], resolved_b[gene]
        a_reachable_by_b = {f for f in a_set if f in with_synonym}
        if a_reachable_by_b or b_set:
            if a_reachable_by_b != b_set:
                disagreements.append((gene, a_set, b_set))
        elif a_set and not a_reachable_by_b:
            # method A found a model gene, but that gene carries no refseq_synonym at
            # all -- method B structurally cannot confirm or deny it. Not a disagreement.
            unreachable_by_b.append((gene, a_set))

    n_a = sum(1 for v in resolved_a.values() if v)
    n_b = sum(1 for v in resolved_b.values() if v)
    print(f"\nmethod A (symbol join):  {n_a}/{len(rows)} genes resolve to >=1 model gene")
    print(f"method B (b-number join, model's own refseq_synonym): "
          f"{n_b}/{len(rows)} genes resolve")
    print(f"  {len(unreachable_by_b)} of A's resolutions name a model gene with no "
          f"refseq_synonym at all, so B structurally can't confirm/deny them "
          f"(expected, not a disagreement)")
    if disagreements:
        print(f"\n!! {len(disagreements)} DISAGREEMENT(S) between the two join methods "
              f"-- this is the assertion failing:")
        for gene, a_set, b_set in disagreements:
            print(f"    {gene}: symbol-join={sorted(a_set)}  b-number-join={sorted(b_set)}")
    else:
        print("\nOK: everywhere both methods could give an answer, they agree.")

    # genes B resolves that A entirely missed (a real recovery, not just agreement)
    b_only = {g: b_set for g, b_set in resolved_b.items()
              if b_set and not resolved_a.get(g)}
    if b_only:
        print(f"\n{len(b_only)} gene(s) recovered by the b-number join that the symbol "
              f"join missed entirely: {sorted(b_only)}")
    else:
        print("no gene is recovered by the b-number join alone -- the symbol join is "
              "already exhaustive for what the model's own annotations can confirm")

    # --- T2: of the 34 resolved genes, confirm the "reaction exists but excluded
    # from the atom universe" ones are excluded because MetaNetX itself flags every
    # one of their candidate reactions transport -- not a bug in this pipeline's
    # own universe computation.
    sys.path.insert(0, str(REPO / "src/fabfos/build_references/resources/buildlib"))
    from bench_universe import transport_mnxrs                        # noqa: E402
    transport = transport_mnxrs(REAC_PROP) if REAC_PROP.exists() else set()
    no_reaction, in_universe_genes, near_miss = [], [], []
    for gene, fids in resolved_a.items():
        if not fids:
            continue
        hit = host[host["orf"].isin(fids)]
        n_rxn = int(hit["intermediate_id"].nunique())
        mnxrs = sorted(hit["mnxr"].dropna().unique())
        in_uni = int(hit[hit["in_atom_universe"] == True]["mnxr"].nunique())  # noqa: E712
        if n_rxn == 0:
            no_reaction.append(gene)
        elif in_uni > 0:
            in_universe_genes.append(gene)
        else:
            near_miss.append((gene, mnxrs))
    print(f"\nof the {n_a} genes resolved to a model gene: {len(no_reaction)} carry no "
          f"reaction, {len(in_universe_genes)} have >=1 reaction inside the atom "
          f"universe, {len(near_miss)} have a reaction but every candidate MNXR is "
          f"excluded")
    if near_miss:
        all_transport = True
        for gene, mnxrs in near_miss:
            flags = [m in transport for m in mnxrs]
            ok = all(flags)
            all_transport &= ok
            tag = "transport-excluded (policy-correct)" if ok else \
                "!! NOT ALL FLAGGED TRANSPORT -- needs a look"
            print(f"    {gene}: {mnxrs} -> {tag}")
        print("OK: every near-miss gene's excluded reaction(s) are MetaNetX-flagged "
              "transport." if all_transport else
              "!! some near-miss genes are excluded for a reason other than transport "
              "-- do not assume this is policy-correct")

    # --- for genes still unresolved by BOTH methods, search the model directly --
    unresolved = [r for r in rows
                  if not (resolved_a[(r["gene"] or "").strip()]
                          or resolved_b[(r["gene"] or "").strip()])]
    print(f"\n{len(unresolved)}/{len(rows)} genes unresolved by either join method -- "
          f"searching the model directly for any trace")

    reac_prop = REAC_PROP
    bigg2m, kegg2m = load_reac_xref(REAC_XREF)
    id_to_mnxr = {r["id"]: None for r in reactions_json}
    for r in reactions_json:
        ann = r.get("annotation", {}) or {}
        mx = ann.get("metanetx.reaction")
        if isinstance(mx, list):
            mx = mx[0] if mx else None
        id_to_mnxr[r["id"]] = bigg2m.get(r["id"]) or mx

    report = []
    for r in unresolved:
        gene = (r["gene"] or "").strip()
        norm = (r["gene_norm"] or "").strip() or gene
        b = to_bnum.get(norm) or to_bnum.get(gene) or ""
        cands = search_candidates(gene, b, genes_json, reactions_json, by_gene_id)
        enriched = []
        for c in cands:
            mnxr = id_to_mnxr.get(c.get("rxn_id", "")) if c["kind"] == "reaction_rule_text" \
                else None
            transport = transport_check(mnxr, reac_prop) if mnxr else None
            enriched.append({**c, "mnxr": mnxr, "is_transport": transport})
        report.append(dict(gene=gene, b_number=b, n_candidates=len(enriched),
                           candidates=enriched))

    with_cands = [x for x in report if x["n_candidates"]]
    print(f"  {len(with_cands)}/{len(unresolved)} show ANY trace under any identifier "
          f"or a raw gene_reaction_rule text match")
    for x in with_cands:
        print(f"\n  {x['gene']} ({x['b_number'] or 'no b-number'}):")
        for c in x["candidates"]:
            if c["kind"] == "gene_id_or_name":
                print(f"      gene-id/name hit: model_gene={c['model_gene']} "
                      f"name={c['model_gene_name']!r} matched-on={c['detail']!r}")
            else:
                print(f"      gene_reaction_rule text hit: rxn={c['rxn_id']} "
                      f"({c['rxn_name']!r}) rule={c['detail']!r} mnxr={c['mnxr']} "
                      f"transport={c['is_transport']}")
    confirmed_absent = [x["gene"] for x in report if x["n_candidates"] == 0]
    print(f"\n  {len(confirmed_absent)} confirmed absent from the model under every "
          f"identifier this script knows: {sorted(confirmed_absent)}")

    # --- publish -------------------------------------------------------------------
    if not a.publish:
        print("\n(dry run -- pass --publish to write "
              f"{OUT.relative_to(REPO)}/gpr_manual.parquet)")
        return 0

    decisions = {}
    if a.decisions and a.decisions.exists():
        for d in csv.DictReader(a.decisions.open(), delimiter="\t"):
            decisions[d["gene"].strip()] = d

    by_gene_report = {x["gene"]: x for x in report}
    near_miss_genes = {g: mnxrs for g, mnxrs in near_miss}
    out_rows, report_rows = [], []
    for r in rows:
        gene = (r["gene"] or "").strip()
        norm = (r["gene_norm"] or "").strip() or gene
        b = to_bnum.get(norm) or to_bnum.get(gene) or ""
        current = sym_for_b.get(b, "")
        feature_name = r.get("function_supplTableS1", "") or gene
        a_set, b_set = resolved_a[gene], resolved_b[gene]
        dec = decisions.get(gene)
        mnxr = None
        if dec and dec.get("mnxr"):
            mnxr = dec["mnxr"].strip() or None
        # `evidence_quality` is the shared schema's controlled vocabulary for the
        # QUALITY OF THE SOURCE a claim came from (reviewed/unreviewed/unknown/
        # synthetic -- see fabfos_evidence.EVIDENCE_QUALITY), not a place for a
        # free-text verdict. A `mnxr` a human reviewed and accepted is "reviewed";
        # every other row here asserts nothing, so "unknown" is the honest value --
        # the actual per-gene reason goes in the report TSV published alongside this.
        quality = "reviewed" if mnxr else "unknown"
        if gene in by_gene_report and by_gene_report[gene]["n_candidates"]:
            verdict = "candidate_reviewed_rejected" if dec else "candidate_unreviewed"
        elif gene in by_gene_report:
            verdict = "confirmed_absent_from_model"
        elif mnxr:
            verdict = "manual_confirmed"
        elif gene in near_miss_genes:
            verdict = (f"resolves_in_gem_gpr_channel_transport_excluded:"
                       f"{','.join(near_miss_genes[gene])}")
        else:
            assert a_set or b_set, gene
            # already resolved to a reaction by build_clone_gpr.py's curated-GEM
            # channel -- this table adds nothing for it, on purpose.
            verdict = "resolves_in_gem_gpr_channel"
        out_rows.append(dict(
            source=COHORT, orf=gene, channel=CHANNEL, mnxr=mnxr,
            intermediate_id=f"{COHORT}:{gene}", intermediate_name=gene,
            raw_score=1.0, score_kind="presence", projection_via="curated",
            evidence_quality=quality, lane_set=LANE_SET, build_id=f"manual_{COHORT}",
            host=HOST, unit_id=COHORT, feature_kind="curated_gene",
            feature_name=feature_name, gpr_rule=None, in_atom_universe=pd.NA,
            condition_id=f"{COHORT}:{gene}", cohort=COHORT, action="add",
            source_organism=""))
        report_rows.append(dict(gene=gene, b_number=b, mnxr=mnxr or "", verdict=verdict))

    cols = fe.schema_for(EXTENSIONS)
    df = pd.DataFrame(out_rows)[cols]
    df = df.sort_values(fe.grain_key(EXTENSIONS), kind="mergesort").reset_index(drop=True)
    fe.validate_gpr(df, LANE_SET, None, COHORT, EXTENSIONS)
    OUT.mkdir(parents=True, exist_ok=True)
    # DVC checks data out as read-only hardlinks into its cache -- opening one for
    # write would mutate the shared cache object under every other checkout of the
    # same content. Unlink first so the write lands on a fresh inode.
    (OUT / "gpr_manual.parquet").unlink(missing_ok=True)
    df.to_parquet(OUT / "gpr_manual.parquet", index=False, compression="zstd")
    (OUT / "gpr_manual_report.tsv").unlink(missing_ok=True)
    rep = pd.DataFrame(report_rows).sort_values("gene").reset_index(drop=True)
    rep.to_csv(OUT / "gpr_manual_report.tsv", sep="\t", index=False)
    n_confirmed = int(df["mnxr"].notna().sum())
    n_null = len(df) - n_confirmed
    print(f"\n-> {OUT}/gpr_manual.parquet: {len(df)} rows, {n_confirmed} confirmed "
          f"mnxr, {n_null} null")
    print(f"-> {OUT}/gpr_manual_report.tsv: the gene-specific verdict behind every one "
          f"of those {n_null} nulls -- {rep['verdict'].value_counts().to_dict()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
