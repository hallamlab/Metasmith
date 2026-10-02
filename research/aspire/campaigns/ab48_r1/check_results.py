#!/usr/bin/env python3
"""Check a retrieved AB48 run against its acceptance criteria.

    python research/aspire/campaigns/ab48_r1/run_ab48.py --study ab48_e5 fetch-intermediates \\
        filter_table asv_mag_link module_mag_anchors
    python research/aspire/campaigns/ab48_r1/check_results.py --intermediates DIR [--results DIR]

The results default to data/aspire/<name>. The intermediates are the directory
fetch-intermediates printed, one subdirectory per transform. The July DADA2 lane's per-cohort
ASVs and contig map come from --july. Prints every failure and exits non-zero if there is one.

The independent BLAST runs blastn from the aspire image over the linker's own barrnap catalogue,
so it re-derives the pairing rule rather than the 16S extraction.
"""

import argparse
import csv
import gzip
import subprocess
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))
from _campaign import aspire_image  # noqa: E402
from run_ab48 import AB48_COHORTS, ANALYSES  # noqa: E402

SODALINEMA = "1-15-"
MIN_PIDENT, MIN_QCOV = 97.0, 90.0
TOP = 10


class Missing(Exception):
    pass


def product(root: Path, dtype: str) -> Path:
    hits = sorted((root / dtype.replace("::", "-")).glob("*"))
    if not hits:
        raise Missing(f"{dtype} absent under {root}")
    return hits[0]


def step_output(inter: Path, transform: str, pattern: str, header: str = "") -> Path:
    for p in sorted((inter / transform).glob(pattern)):
        if not header or (p.is_file() and open(p).readline().startswith(header)):
            return p
    raise Missing(f"{transform}: no [{pattern}] output starting [{header}] under {inter}")


def read_fasta(path: Path) -> dict[str, str]:
    opener = gzip.open if path.suffix == ".gz" else open
    seqs, name = {}, None
    with opener(path, "rt") as f:
        for line in f:
            line = line.strip()
            if line.startswith(">"):
                name = line[1:].split()[0].split(";")[0]
                seqs[name] = ""
            elif name:
                seqs[name] += line.upper()
    return seqs


def genus(taxon: str) -> str:
    for rank in str(taxon).split(";"):
        rank = rank.strip()
        if rank.startswith("g__") and len(rank) > 3:
            return rank[3:]
    return "unassigned"


def related(a: str, b: str) -> bool:
    return a in b or b in a


def independent_candidates(seqs: Path, magl: Path, work: Path) -> dict[str, set[str]]:
    ref_genome = {r["ref_id"]: r["genome_id"] for r in
                  csv.DictReader(open(magl / "references" / "barrnap_16s_reference_catalog.tsv"), delimiter="\t")}
    work.mkdir(parents=True, exist_ok=True)
    query = work / "asvs.fasta"
    with open(query, "w") as f:
        for name, s in read_fasta(seqs).items():
            f.write(f">{name}\n{s}\n")
    subject = magl / "references" / "barrnap_16s_sequences.fasta"
    out = work / "independent_blast.tsv"
    subprocess.run(["docker", "run", "--rm", "-u", "1001:1001", "-v", f"{work}:/w", "-v", f"{subject.parent}:/r:ro",
                    aspire_image(), "blastn", "-query", "/w/asvs.fasta", "-subject", f"/r/{subject.name}",
                    "-outfmt", "6 qseqid sseqid pident length qlen", "-out", "/w/independent_blast.tsv"], check=True)
    cands: dict[str, set[str]] = {}
    for q, s, pident, length, qlen in csv.reader(open(out), delimiter="\t"):
        if float(pident) >= MIN_PIDENT and 100.0 * int(length) / int(qlen) >= MIN_QCOV:
            cands.setdefault(q, set()).add(ref_genome[s])
    return cands


def rrna_loci(magl: Path) -> dict[str, list[tuple[int, int, str]]]:
    """The 16S loci barrnap called, by contig as the July map names it (the revio assembly's
    contigs carry an ABC- prefix there and none in the bins)."""
    loci: dict[str, list[tuple[int, int, str]]] = {}
    with open(magl / "references" / "barrnap_16s_reference_catalog.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            loci.setdefault(f"ABC-{r['seq_id']}", []).append((int(r["start"]), int(r["end"]), r["genome_id"]))
    return loci


def load_july(root: Path, cohorts: list[str]):
    """The July lane ran one DADA2 workflow per cohort. Its ASV ids are md5 sums of the
    sequence, so the cohorts merge on id."""
    seqs, totals, tax, hits = {}, [], [], []
    for c in cohorts:
        d = root / c
        seqs.update(read_fasta(next((d / "asv_seqs").glob("*"))))
        totals.append(pd.read_csv(next((d / "asv_table").glob("*")), sep="\t", index_col=0).sum(axis=1))
        tax.append(pd.read_csv(next((d / "asv_taxonomy").glob("*")), sep="\t", index_col=0)["Taxon"])
        hits += open(next((d / "asv_contig_map").glob("*"))).read().splitlines()
    total = pd.concat(totals).groupby(level=0).sum().sort_values(ascending=False)
    taxon = pd.concat(tax).groupby(level=0).first()
    return seqs, total, taxon, hits


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", default="ab48_e5_r1")
    ap.add_argument("--results", type=Path)
    ap.add_argument("--intermediates", type=Path, required=True)
    ap.add_argument("--july", type=Path, default=REPO / "cache" / "aspire" / "t10_mags" / "july")
    args = ap.parse_args()
    results = (args.results or REPO / "data" / "aspire" / args.name).resolve()
    inter = args.intermediates.resolve()
    fails: list[str] = []

    for dtype in ANALYSES:
        if not list((results / dtype.replace("::", "-")).glob("*")):
            fails.append(f"{dtype}: no product, so its step failed or never ran")

    clean = pd.read_csv(product(results, "aspire::counts_clean"), sep="\t", index_col=0)
    tax = pd.read_csv(product(results, "amplicon::asv_taxonomy"), sep="\t", index_col=0)
    tax.index = [i.split(";")[0] for i in tax.index]
    taxon = tax["Taxon"].reindex(clean.index).fillna("")
    totals = clean.sum(axis=1).sort_values(ascending=False)
    seqs_path = step_output(inter, "filter_table", "*.fasta.gz")
    seqs = read_fasta(seqs_path)
    pairing = pd.read_csv(product(results, "aspire::asv_mag_pairing"), sep="\t", dtype=str).set_index("ASV_ID")
    magl = next(p for p in (inter / "asv_mag_link").iterdir() if p.is_dir())
    print(f"{clean.shape[1]} samples, {len(clean)} curated ASVs, "
          f"{pairing['pairing_status'].value_counts().to_dict()} over {len(pairing)} filtered ASVs")

    # 1. The Sodalinema MAG carries the dominant cyanobacterium's 16S.
    cyano = [a for a in totals.index if "p__Cyanobacteria" in taxon[a] and "Chloroplast" not in taxon[a]]
    if not cyano:
        fails.append("no cyanobacterial ASV in the curated table")
    else:
        top = cyano[0]
        row = pairing.loc[top]
        print(f"dominant cyanobacterium {top} ({totals[top] / totals.sum():.0%} of reads, {genus(taxon[top])}): "
              f"{row['pairing_status']} {row['genome_id']} at {row['link_pident']}%")
        if not str(row["genome_id"]).startswith(SODALINEMA) or float(row["link_pident"] or 0) < 99.0:
            fails.append(f"dominant cyanobacterium {top} pairs with {row['genome_id']} at {row['link_pident']}%, "
                         f"not bin 1-15 at >=99%")

    # 2. An independent blastn agrees with the pair table, ASV by ASV.
    cands = independent_candidates(seqs_path, magl, REPO / "cache" / "aspire" / args.name / "check")
    disagree = []
    for asv, r in pairing.iterrows():
        mine = cands.get(asv, set())
        status = r["pairing_status"]
        want = "unpaired" if not mine else "paired_unique" if len(mine) == 1 else "paired_ambiguous"
        if status != want or (mine and r["genome_id"] not in mine):
            disagree.append(f"{asv}: pipeline {status} {r['genome_id']}, independent {want} {sorted(mine)}")
    print(f"independent blastn: {sum(1 for v in cands.values() if v)} ASVs with a candidate MAG, "
          f"{len(disagree)} disagreements")
    fails += [f"pairing: {d}" for d in disagree[:20]]

    # 3. The July contig map agrees: an ASV whose sequence lies inside a binned 16S gene pairs
    # with that bin. A July hit outside every 16S locus is an off-target amplicon of genomic DNA,
    # which the linker rightly cannot pair, since it pairs through 16S genes only.
    cohorts = AB48_COHORTS if args.name.startswith("ab48_r1") else ["lab_25-07-23_Enrichment5"]
    july, july_tot, july_tax, july_hits = load_july(args.july, cohorts)
    loci = rrna_loci(magl)
    july_bins: dict[str, set[str]] = {}
    off_target = set()
    for line in july_hits:
        q, contig, pident, length, *_, s0, s1 = line.split("\t")[:10]
        if float(pident) != 100.0 or int(length) != len(july[q]):
            continue
        lo, hi = sorted((int(s0), int(s1)))
        inside = {g for a, b, g in loci.get(contig, []) if a <= lo and hi <= b}
        if inside:
            july_bins.setdefault(q, set()).update(inside)
        elif contig.startswith("ABC-"):
            off_target.add(q)
    print(f"July contig map: {len(off_target - july_bins.keys())} July ASVs hit the revio assembly "
          f"outside every 16S locus")
    checked = 0
    for q, bins in july_bins.items():
        for asv, s in seqs.items():
            if asv in pairing.index and related(s, july[q]):
                checked += 1
                mine = cands.get(asv, set())
                if not bins & mine:
                    fails.append(f"July {q} hits {sorted(bins)}; its match {asv} pairs with {sorted(mine) or 'nothing'}")
    print(f"July contig map: {len(july_bins)} July ASVs inside a binned 16S gene, {checked} port ASVs checked against them")
    if not checked:
        fails.append("no port ASV shares a sequence with a July ASV on a binned contig")

    # 4. The dominant ASVs are the July lane's dominant ASVs, and the dominant genera agree.
    july_top = [july[q] for q in july_tot.index[:3 * TOP] if q in july]
    unmatched = [a for a in totals.index[:TOP] if not any(related(seqs[a], j) for j in july_top)]
    print(f"top {TOP} ASVs: {TOP - len(unmatched)} match one of the July lane's top {3 * TOP} by sequence")
    if len(unmatched) > TOP // 5:
        fails.append(f"top ASVs with no July counterpart: {unmatched}")
    port_genera = clean.groupby(taxon.map(genus)).sum().sum(axis=1).drop("unassigned", errors="ignore")
    july_genera = july_tot.groupby(july_tax.reindex(july_tot.index).fillna("").map(genus)).sum()
    july_genera = july_genera.drop("unassigned", errors="ignore")
    pg, jg = list(port_genera.nlargest(5).index), list(july_genera.nlargest(5).index)
    print(f"top genera: port {pg}, July {jg}")
    if len(set(pg) & set(jg)) < 3:
        fails.append(f"top-5 genera share {len(set(pg) & set(jg))} of 5 with the July lane")

    # 5. The network is non-empty and some module holds more than one node.
    net = product(results, "aspire::network_outputs")
    nodes = pd.read_csv(net / "spieceasi_node_features.csv")
    modules = pd.read_csv(net / "spieceasi_modules_all.tsv", sep="\t")
    sizes = modules.groupby("module_id").size()
    print(f"SpiecEasi: {len(nodes)} nodes, {int((nodes['Degree'] > 0).sum())} with an edge, "
          f"{int((sizes > 1).sum())} modules of more than one node, largest {sizes.max()}")
    if not (nodes["Degree"] > 0).any() or not (sizes > 1).any():
        fails.append("network: no edge, or no module of more than one node")

    # 6. Indicator species ran for every label the study sheet carries.
    study = pd.read_csv(REPO / "cache" / "aspire" / args.name / "inputs.xgdb" / "study_metadata.tsv", sep="\t")
    isa = product(results, "aspire::indicspecies_results")
    for label in study.columns[1:]:
        if study[label].dropna().nunique() >= 2 and not (isa / f"{label}_indicator_species_results.tsv").exists():
            fails.append(f"indicspecies: no results for label {label}")

    # 7. The MAG network, the anchors and the master summary carry real pairs.
    mnet = product(results, "aspire::asv_mag_network_outputs")
    accepted = pd.read_csv(mnet / "mapping" / "asv_mag_network_accepted_mappings.tsv", sep="\t")
    anchors = pd.read_csv(step_output(inter, "module_mag_anchors", "*.tsv", "Taxon\tmodule_id"), sep="\t")
    n_anchor = int(anchors["has_mag_pair"].astype(str).eq("True").sum())
    print(f"MAG network: {len(accepted)} accepted ASV-MAG mappings; anchors: {n_anchor} module ASVs with a MAG pair")
    if accepted.empty:
        fails.append("asv_mag_network: no accepted mapping")
    if not n_anchor:
        fails.append("module_mag_anchors: no module ASV has a MAG pair")
    if list((results / "aspire-master_long").glob("*")):
        path = product(results, "aspire::master_long")
        col = next(c for c in open(path).readline().rstrip("\n").split("\t")
                   if c.startswith("asv_mag_link__") and c.endswith("asv2mag_pairing__pairing_status"))
        long = pd.read_csv(path, sep="\t", dtype=str, usecols=["ASV_ID", col])
        paired = long.loc[long[col].str.startswith("paired", na=False), "ASV_ID"].nunique()
        print(f"master summary: {paired} paired ASVs")
        if not paired:
            fails.append("master_summary: no ASV carries a pairing")

    for f in fails:
        print(f"FAIL {f}")
    print("OK" if not fails else f"{len(fails)} failures")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
