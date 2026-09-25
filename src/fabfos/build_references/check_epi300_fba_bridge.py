"""Assert data/fabfos/runs/e_coli_dh10b/fba/ against what it claims.

    /home/tony/lib/miniforge3/envs/build-refs-cobra/bin/python \\
        src/fabfos/build_references/check_epi300_fba_bridge.py [--fba DIR]

Checks the outputs of derive_epi300_fba_bridge.py against sources that script did not
write wherever one exists: the chromosome FASTA for every gene sequence, NCBI's two
protein FASTAs for every mapped pair, and the model's own rules for every rewritten one.
"""
from __future__ import annotations

import argparse
import filecmp
import random
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_epi300_identity import (EXPECTED_LOST_REACTIONS, model_genes,  # noqa: E402
                                   model_rules, rule_holds)
from derive_epi300_fba_bridge import (DH10B_GENOME, EPI300_GENOME, EPI300_SEQ,  # noqa: E402
                                      EVIDENCE_LIB, EXTENSIONS, GEM, GPR_GEM, LANE_SET,
                                      OUT, SPONTANEOUS, parse_location, read_faa,
                                      read_genbank, revcomp, rule_genes,
                                      translate)

FILES = {"iECDH10B_1368.json", "EPI300.json", f"{EPI300_SEQ}.fna", f"{EPI300_SEQ}.gbk",
         "epi300_genes.fna", "epi300_genes.gtf", "gpr_gem_epi300.csv", "README.md"}
PSEUDO_IN_BOTH = {"ECDH10B_1939", "fruB", "mglA", "ytfT", "gatZ", "mhpD", "nupG", "nrfB"}
PSEUDO_IN_EPI300_ONLY = {"ECDH10B_0130"}
PSEUDO_IN_ANNOTATION_ONLY = {"proV"}
SPLIT = {"nupG", "nrfB", "ECDH10B_0130"}
START_VARIANTS = {"ECDH10B_1259", "ECDH10B_1354"}
N_DUPLICATED = 42
NULLABLE = ("gene", "mnxr", "feature_name", "gpr_rule")

problems: list[str] = []


def check(ok: bool, msg: str):
    print(f"  {'ok  ' if ok else 'FAIL'} {msg}", flush=True)
    if not ok:
        problems.append(msg)


def read_fasta(path: Path) -> dict[str, tuple[str, str]]:
    out, cur = {}, None
    for line in path.read_text().splitlines():
        if line.startswith(">"):
            cur, _, desc = line[1:].partition(" ")
            if cur in out:
                problems.append(f"FASTA id {cur} appears twice")
            out[cur] = (desc, [])
        else:
            out[cur][1].append(line)
    return {k: (d, "".join(s)) for k, (d, s) in out.items()}


def read_gtf(path: Path):
    genes, exons = {}, defaultdict(list)
    for line in path.read_text().splitlines():
        if line.startswith("#"):
            continue
        seq, _, kind, a, b, _, strand, _, attrs = line.split("\t")
        at = dict(re.findall(r'(\S+) "([^"]*)";', attrs))
        row = (seq, int(a), int(b), strand, at)
        if kind == "gene":
            genes[at["gene_id"]] = row
        else:
            exons[at["gene_id"]].append(row)
    return genes, exons


def read_csv_as_parquet_dtypes(path: Path, like):
    import pandas as pd
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for c in df.columns:
        if c == "llr":
            df[c] = df[c].astype(like[c].dtype)
        elif c == "in_atom_universe":
            df[c] = df[c].map({"True": True, "False": False}).astype(bool)
        elif c in NULLABLE:
            df[c] = df[c].where(df[c] != "", None)
    return df


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fba", type=Path, default=OUT)
    a = ap.parse_args()
    fba = a.fba

    print("files")
    check({p.name for p in fba.iterdir()} == FILES,
          f"the folder holds exactly {sorted(FILES)}")
    for name, src in ((GEM.name, GEM),
                      (f"{EPI300_SEQ}.fna", EPI300_GENOME / f"{EPI300_SEQ}.fna"),
                      (f"{EPI300_SEQ}.gbk", EPI300_GENOME / f"{EPI300_SEQ}.gbk")):
        check(filecmp.cmp(fba / name, src, shallow=False), f"{name} is byte-identical to {src}")

    print("FASTA and GTF")
    chrom = "".join(l.strip() for l in (fba / f"{EPI300_SEQ}.fna").read_text().splitlines()
                    if not l.startswith(">")).upper()
    fasta = read_fasta(fba / "epi300_genes.fna")
    genes_gtf, exons = read_gtf(fba / "epi300_genes.gtf")
    _, features = read_genbank(fba / f"{EPI300_SEQ}.gbk")
    products = [f for f in features if f.kind in ("CDS", "tRNA", "rRNA", "ncRNA", "tmRNA")]
    by_locus = {f.locus: f for f in products}
    locus_of = {fid: re.search(r"\[locus_tag=([^\]]+)\]", d).group(1)
                for fid, (d, _) in fasta.items()}
    check(len(fasta) == len(products) == len(by_locus) and
          set(locus_of.values()) == set(by_locus),
          f"{len(fasta):,} FASTA records, one per GenBank gene feature ({len(products):,})")
    check(set(exons) == set(fasta), "every FASTA id has GTF exons and every GTF gene_id "
                                     "is a FASTA id")

    bad_seq, bad_coord, bad_prot = [], [], []
    faa = {i: s for i, s in read_faa(EPI300_GENOME / f"{EPI300_SEQ}.faa").values()}
    for fid, (desc, seq) in fasta.items():
        ex = sorted(exons[fid], key=lambda r: int(r[4]["exon_number"]))
        rebuilt = "".join(chrom[a - 1:b] if st == "+" else revcomp(chrom[a - 1:b])
                          for _, a, b, st, _ in ex)
        if rebuilt != seq:
            bad_seq.append(fid)
        f = by_locus[locus_of[fid]]
        if [(a, b, "+" if s > 0 else "-") for a, b, s in parse_location(f.location)] != \
                [(a, b, st) for _, a, b, st, _ in ex]:
            bad_coord.append(fid)
        if f.kind == "CDS" and "[pseudo=false]" in desc:
            mine, prot = translate(seq), faa[fid]
            sec = "transl_except" in f.quals
            if len(mine) != len(prot) or any(
                    x != y and not (sec and x == "*" and y == "U") for x, y in zip(mine, prot)):
                bad_prot.append(fid)
    check(not bad_seq, f"every FASTA sequence equals its GTF exons cut from the chromosome "
                       f"({len(bad_seq)} differ {bad_seq[:3]})")
    check(not bad_coord, f"every GTF exon set equals the GenBank location "
                         f"({len(bad_coord)} differ {bad_coord[:3]})")
    n_intact = sum("[gbkey=CDS]" in d and "[pseudo=false]" in d for d, _ in fasta.values())
    check(not bad_prot, f"all {n_intact:,} intact CDS translate to their protein in NCBI's "
                        f".faa ({len(bad_prot)} differ {bad_prot[:3]})")
    wrap = next(fid for fid in fasta if locus_of[fid] == "ACP6EX_00005")
    check(wrap not in bad_prot and wrap not in genes_gtf and len(exons[wrap]) == 2,
          "the origin-spanning rhsA CDS translates correctly, as two exons with no gene line")
    check(any(r[3] == "-" for e in exons.values() for r in e) and not bad_prot,
          "minus-strand CDS are among those translated")

    print("GPR table")
    import pandas as pd
    sys.path.insert(0, str(EVIDENCE_LIB))
    import fabfos_evidence as fe
    src = pd.read_parquet(GPR_GEM)
    df = read_csv_as_parquet_dtypes(fba / "gpr_gem_epi300.csv", src)
    check(list(df.columns) == list(src.columns), "same 19 columns in the same order")
    check(df["orf"].notna().all() and set(df["orf"]) <= set(fasta),
          "every orf is a FASTA id, none empty")
    check((df["feature_kind"] == "gem_gene").all() and df["gpr_rule"].notna().all(),
          "every row is a gene row with a rule")
    try:
        fe.validate_gpr(df, LANE_SET, set(fasta), src["source"].iloc[0], EXTENSIONS)
        check(True, "validate_gpr accepts the table read back with the parquet dtypes")
    except SystemExit as e:
        check(False, f"validate_gpr: {e}")
    import json
    # Read from the file, not from cobra, which drops redundant outer brackets on load.
    written = {r["id"]: r["gene_reaction_rule"]
               for r in json.loads((fba / "EPI300.json").read_text())["reactions"]}
    expected_rows = {(rid, o) for rid, rule in written.items()
                     if rid not in EXPECTED_LOST_REACTIONS
                     for o in rule_genes(rule) - {SPONTANEOUS}}
    got_rows = set(zip(df["intermediate_id"], df["orf"]))
    check(got_rows == expected_rows and len(df) == len(got_rows),
          f"one row per ORF in each EPI300.json rule, fhuA's closed reactions left out "
          f"({len(got_rows):,} rows over {df['intermediate_id'].nunique():,} reactions)")
    check(all(written[r] == g for r, g in zip(df["intermediate_id"], df["gpr_rule"])),
          "every gpr_rule is EPI300.json's rule for that reaction, verbatim")
    keep = ["intermediate_id", "mnxr", "in_atom_universe", "projection_via"]
    ref = src[keep].drop_duplicates().set_index("intermediate_id")
    mine = df[keep].drop_duplicates().set_index("intermediate_id")
    check(mine.index.is_unique and mine.equals(ref.loc[mine.index]),
          "every reaction keeps gpr_gem.parquet's MNXR, projection and universe flag")
    orf_set = set(df["orf"])

    print("mapping")
    genes = model_genes(GEM)
    epi_json = json.loads((fba / "EPI300.json").read_text())
    orfs_of = defaultdict(set)
    for g in epi_json["genes"]:
        if g["id"] != SPONTANEOUS:
            orfs_of[g["notes"]["model_gene"][0]].add(g["id"])
    real = [g for g in genes if g != SPONTANEOUS]
    check(set(orfs_of) == set(real), f"all {len(real):,} real model genes map to at least "
                                     f"one EPI300 ORF in EPI300.json, and nothing else does")
    check(all(o in orfs_of[g] for g, o in zip(df["feature_name"], df["orf"])),
          "every row's feature_name is its ORF's model gene in EPI300.json")
    pseudo_fid = {fid for fid, (d, _) in fasta.items() if "[pseudo=true]" in d}
    two = {g for g, o in orfs_of.items() if len(o) == 2}
    dup = {g for g in two if not (orfs_of[g] & pseudo_fid)}
    check(len(dup) == N_DUPLICATED and two - dup == SPLIT and
          all(len(o) <= 2 for o in orfs_of.values()),
          f"{len(dup)} duplicated genes map to two intact ORFs, the split genes "
          f"{sorted(two - dup)} to two pieces, nothing to more")
    with_pseudo = {g for g, o in orfs_of.items() if o & pseudo_fid}
    check(with_pseudo == PSEUDO_IN_BOTH | PSEUDO_IN_EPI300_ONLY | PSEUDO_IN_ANNOTATION_ONLY,
          f"the model genes with a pseudo EPI300 ORF are exactly {sorted(with_pseudo)}")

    _, dh = read_genbank(DH10B_GENOME / "NC_010473.1.gbk")
    dh_faa = read_faa(DH10B_GENOME / "NC_010473.1.faa")
    dh_prot = {}
    for f in dh:
        if f.kind == "CDS" and "pseudo" not in f.quals:
            for k in (f.old_locus, f.gene):
                if k:
                    dh_prot.setdefault(k, set()).add(dh_faa[f.locus][1])
    mismatched = []
    for g in real:
        if g in with_pseudo:
            continue
        for fid in orfs_of[g]:
            p = faa[fid]
            ok = p in dh_prot.get(g, ()) or (
                g in START_VARIANTS and any(q.endswith(p[1:]) for q in dh_prot.get(g, ())))
            if not ok:
                mismatched.append((g, fid))
    check(not mismatched, f"every intact pair is NCBI-identical protein, the start variants "
                          f"{sorted(START_VARIANTS)} a suffix ({mismatched[:3]})")

    print("rules")
    rules = dict(model_rules(GEM))
    check(set(written) == set(rules) and
          all(bool(rules[r]) == bool(written[r]) for r in rules) and
          all((SPONTANEOUS in rule_genes(rules[r])) == (SPONTANEOUS in rule_genes(written[r]))
              for r in rules),
          "EPI300.json has a rule exactly where the model does, and keeps every s0001")
    rng = random.Random(0)
    diverged = set()
    for _ in range(300):
        on = {g for g in genes if rng.random() < 0.8}
        on_orfs = {o for g in on for o in orfs_of.get(g, ())} | ({SPONTANEOUS} & on)
        for rid, rule in rules.items():
            if rule_holds(rule, on) != rule_holds(written[rid], on_orfs):
                diverged.add(rid)
    check(not diverged, f"rewritten rules agree with the model's under 300 random gene sets, "
                        f"s0001 included ({sorted(diverged)[:3]})")
    lost_orfs = {o for g in PSEUDO_IN_EPI300_ONLY for o in orfs_of[g]}
    model_live = {r for r, rule in rules.items() if rule_holds(rule, set(genes))}
    epi_live = {r for r in model_live
                if rule_holds(written[r], (set(fasta) | {SPONTANEOUS}) - lost_orfs)}
    check(model_live - epi_live == EXPECTED_LOST_REACTIONS,
          f"EPI300 loses exactly the three fhuA reactions: {sorted(model_live - epi_live)}")

    print("EPI300.json")
    import cobra
    epi_m = cobra.io.load_json_model(str(fba / "EPI300.json"))
    dh_m = cobra.io.load_json_model(str(GEM))
    check(not any("|" in i for i in set(fasta) | orf_set), "no ORF id carries a '|'")
    check([r.id for r in epi_m.reactions] == [r.id for r in dh_m.reactions] and
          [m.id for m in epi_m.metabolites] == [m.id for m in dh_m.metabolites] and
          all({m.id: c for m, c in a.metabolites.items()} ==
              {m.id: c for m, c in b.metabolites.items()}
              for a, b in zip(epi_m.reactions, dh_m.reactions)),
          "same reactions, metabolites and stoichiometry as iECDH10B_1368")
    moved = {r.id for r, s in zip(epi_m.reactions, dh_m.reactions) if r.bounds != s.bounds}
    check(moved == EXPECTED_LOST_REACTIONS and
          all(epi_m.reactions.get_by_id(r).bounds == (0, 0) for r in moved),
          f"bounds differ only on the fhuA reactions, closed ({sorted(moved)})")
    bad_rule = [r.id for r in epi_m.reactions
                if {g.id for g in r.genes} != set(rule_genes(written[r.id]))]
    check(not bad_rule, f"cobra parses every rule into exactly its tokens ({bad_rule[:3]})")
    check({g.id for g in epi_m.genes} - {SPONTANEOUS} <= set(fasta) and
          SPONTANEOUS in {g.id for g in epi_m.genes},
          f"every model gene is a FASTA id, except s0001 as in iECDH10B_1368 "
          f"({len(epi_m.genes):,} genes)")
    mu_e, mu_d = epi_m.slim_optimize(), dh_m.slim_optimize()
    check(mu_e > 0 and abs(mu_e - mu_d) < 1e-6,
          f"grows on the model's default bounds as iECDH10B_1368 does ({mu_e:.4f} vs {mu_d:.4f})")

    print()
    for p in problems:
        print(f"FAIL: {p}")
    if not problems:
        print(f"{fba} holds what it claims")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
