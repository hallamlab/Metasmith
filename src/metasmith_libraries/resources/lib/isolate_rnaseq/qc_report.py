"""One HTML QC report over an isolate RNA-seq experiment.

    qc_report.py <title> <count_qc.tsv> <out.html> --sample <library> <fastp.json> <samtools_stats.txt> [--sample ...]
"""
import html
import json
import sys

import pandas as pd


def fastp(path: str) -> dict:
    j = json.load(open(path))
    before, after = j["summary"]["before_filtering"], j["summary"]["after_filtering"]
    return {
        "read_pairs_in": before["total_reads"] // 2,
        "read_pairs_after_trim": after["total_reads"] // 2,
        "percent_kept": round(100 * after["total_reads"] / max(before["total_reads"], 1), 2),
        "q30_after_trim": round(100 * after["q30_rate"], 2),
        "mean_length_after_trim": after["read1_mean_length"],
        "percent_duplication": round(100 * j.get("duplication", {}).get("rate", float("nan")), 2),
    }


def samtools(path: str) -> dict:
    sn = {}
    for line in open(path):
        if line.startswith("SN\t"):
            key, value = line.split("\t")[1:3]
            sn[key.rstrip(":")] = value.strip()
    total = float(sn["raw total sequences"])
    return {
        "percent_mapped": round(100 * float(sn["reads mapped"]) / total, 2),
        "percent_properly_paired": round(100 * float(sn["reads properly paired"]) / total, 2),
        "percent_mapq0": round(100 * float(sn["reads MQ0"]) / total, 2),
        "error_rate": float(sn["error rate"]),
        "insert_size_mean": float(sn["insert size average"]),
    }


def main(argv: list[str]) -> None:
    title, qc_path, out = argv[:3]
    rows = {}
    rest = argv[3:]
    while rest:
        flag, library, fj, st, *rest = rest
        assert flag == "--sample", flag
        rows[library] = {**fastp(fj), **samtools(st)}
    reads = pd.DataFrame(rows).T
    counts = pd.read_csv(qc_path, sep="\t", index_col="library")
    reads = reads.loc[[i for i in counts.index if i in reads.index]]
    strand = {0: "unstranded", 1: "forward (-s 1)", 2: "reverse (-s 2)"}[int(counts.strand_setting.iloc[0])]
    notes = [
        f"Strandedness: {strand}; reverse/forward assignment ratio per library: "
        + ", ".join(f"{i} {r:g}" for i, r in counts.reverse_to_forward_ratio.items()),
        "Counts use MAPQ 0, so reads on a duplicated gene are kept; unassigned_low_mapq is what a MAPQ 10 filter would drop.",
    ]
    page = [
        "<!doctype html><html><head><meta charset='utf-8'><title>%s</title>" % html.escape(title),
        "<style>body{font-family:system-ui,sans-serif;margin:24px;color:#222}"
        "table{border-collapse:collapse;margin:12px 0 28px}td,th{border:1px solid #ccd;padding:4px 8px;text-align:right}"
        "th{background:#eef1f6}td:first-child,th:first-child{text-align:left}</style></head><body>",
        f"<h1>{html.escape(title)}</h1>",
        "".join(f"<p>{html.escape(n)}</p>" for n in notes),
        "<h2>Trimming (fastp) and mapping (bowtie2, samtools stats)</h2>",
        reads.to_html(border=0, float_format=lambda v: f"{v:,.4g}"),
        "<h2>Counting (featureCounts on the RefSeq genes)</h2>",
        counts.to_html(border=0, float_format=lambda v: f"{v:,.4g}"),
        "</body></html>",
    ]
    open(out, "w").write("\n".join(page))
    print(f"{len(reads)} libraries -> {out}")


if __name__ == "__main__":
    main(sys.argv[1:])
