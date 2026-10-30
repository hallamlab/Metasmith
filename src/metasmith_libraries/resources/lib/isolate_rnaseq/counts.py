"""Turn a featureCounts strandedness sweep into one annotated gene count table and a per-library QC table.

    counts.py <sweep_dir> <genomic.gff> <genome.fna> <raw_count_table.tsv> <count_qc.tsv>

`<sweep_dir>` holds `samples.tsv` (library names in BAM order) and featureCounts tables named
`s{0,1,2}_Q{0,10}.txt`, each with its `.summary`.
"""
import hashlib
import sys
from pathlib import Path
from urllib.parse import unquote

import pandas as pd

ANNOTATION = ["Geneid", "Chr", "Start", "End", "Strand", "Length", "gene_biotype", "Name"]
COMPLEMENT = str.maketrans("ACGTNacgtn", "TGCANtgcan")

# An unstranded count always assigns at least as many fragments as either stranded one, so the
# highest assignment rate is the wrong rule. A library is stranded when one direction carries
# this many times the other.
STRANDED_RATIO = 3.0


def read_counts(sweep: Path, s: int, q: int, samples: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    table = pd.read_csv(sweep / f"s{s}_Q{q}.txt", sep="\t", comment="#")
    table.columns = ANNOTATION + samples
    summary = pd.read_csv(sweep / f"s{s}_Q{q}.txt.summary", sep="\t", index_col=0)
    summary.columns = samples
    return table.set_index("Geneid"), summary


def choose_strand(sweep: Path, samples: list[str]) -> tuple[int, pd.Series]:
    assigned = {s: read_counts(sweep, s, 0, samples)[1].loc["Assigned"] for s in (1, 2)}
    ratio = (assigned[2] + 1) / (assigned[1] + 1)
    total = ratio.prod() ** (1 / len(ratio))
    if total >= STRANDED_RATIO:
        return 2, ratio
    if total <= 1 / STRANDED_RATIO:
        return 1, ratio
    return 0, ratio


def products(gff: Path) -> pd.DataFrame:
    genes, children = {}, {}
    for line in gff.read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        f = line.split("\t")
        if len(f) < 9:
            continue
        attrs = dict(kv.split("=", 1) for kv in f[8].split(";") if "=" in kv)
        attrs = {k: unquote(v) for k, v in attrs.items()}
        if f[2] == "gene" and "locus_tag" in attrs:
            genes[attrs["ID"]] = attrs
        elif "Parent" in attrs and attrs["Parent"] not in children:
            children[attrs["Parent"]] = attrs
    rows = []
    for gid, g in genes.items():
        c = children.get(gid, {})
        rows.append({"locus_tag": g["locus_tag"], "gene_name": g.get("gene", ""),
                     "product": c.get("product", ""), "protein_id": c.get("protein_id", "")})
    return pd.DataFrame(rows).set_index("locus_tag")


def read_fasta(path: Path) -> dict[str, str]:
    seqs, name, parts = {}, None, []
    for line in path.read_text().splitlines():
        if line.startswith(">"):
            if name:
                seqs[name] = "".join(parts)
            name, parts = line[1:].split()[0], []
        else:
            parts.append(line.strip())
    if name:
        seqs[name] = "".join(parts)
    return seqs


def copy_groups(table: pd.DataFrame, genome: dict[str, str]) -> pd.Series:
    """Name every gene by the set of genes sharing its exact sequence, either strand."""
    keys = {}
    for gid, r in table.iterrows():
        seq = genome[r.Chr][int(r.Start) - 1:int(r.End)].upper()
        rc = seq.translate(COMPLEMENT)[::-1]
        keys[gid] = hashlib.sha1(min(seq, rc).encode()).hexdigest()
    keys = pd.Series(keys)
    members = keys.groupby(keys).apply(lambda s: "+".join(s.index))
    return keys.map(members)


def main(sweep: Path, gff: Path, fna: Path, out_counts: Path, out_qc: Path) -> None:
    samples = (sweep / "samples.tsv").read_text().split()
    strand, ratio = choose_strand(sweep, samples)
    q0, summary0 = read_counts(sweep, strand, 0, samples)
    q10, summary10 = read_counts(sweep, strand, 10, samples)

    # Identical copies split their reads at random under MAPQ 0 and lose all of them under
    # MAPQ 10, so only their sum is a measurement.
    group = copy_groups(q0, read_fasta(fna))
    ann = q0[["Length", "gene_biotype"]].join(products(gff))
    ann["gene_name"] = ann.gene_name.replace("", pd.NA).fillna(q0.Name)
    merged = q0[samples].groupby(group).sum()
    merged_q10 = q10[samples].groupby(group).sum()
    first = ann.groupby(group).first()
    first["copies"] = [len(g.split("+")) for g in first.index]
    first["multimap_fraction"] = (1 - merged_q10.sum(axis=1) / merged.sum(axis=1).clip(lower=1)).round(4)
    table = first.join(merged)
    table.index.name = "gene_id"
    order = [g for g in dict.fromkeys(group.loc[q0.index])]
    table = table.loc[order]
    table.to_csv(out_counts, sep="\t")

    biotype = q0.gene_biotype
    qc = pd.DataFrame({
        "strand_setting": strand,
        "reverse_to_forward_ratio": ratio.round(2),
        "total_fragments": summary0.sum(),
        "assigned_mapq0": summary0.loc["Assigned"],
        "assigned_mapq10": summary10.loc["Assigned"],
        "unassigned_low_mapq": summary10.loc["Unassigned_MappingQuality"],
        "unassigned_no_feature": summary0.loc["Unassigned_NoFeatures"],
        "unassigned_unmapped": summary0.loc["Unassigned_Unmapped"],
        "rRNA_fragments": q0.loc[biotype == "rRNA", samples].sum(),
        "tRNA_fragments": q0.loc[biotype == "tRNA", samples].sum(),
        "protein_coding_fragments": q0.loc[biotype == "protein_coding", samples].sum(),
    })
    qc["percent_rRNA"] = (100 * qc.rRNA_fragments / qc.total_fragments).round(2)
    qc["percent_assigned"] = (100 * qc.assigned_mapq0 / qc.total_fragments).round(2)
    qc.index.name = "library"
    qc.to_csv(out_qc, sep="\t")
    dup = table[table.copies > 1]
    print(f"strand -s {strand} (reverse/forward {ratio.round(1).to_dict()}); {len(q0)} genes -> "
          f"{len(table)} rows, {len(dup)} identical-copy groups")


if __name__ == "__main__":
    main(*(Path(a) for a in sys.argv[1:6]))
