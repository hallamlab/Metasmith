"""Hypergeometric over-representation of GO terms in a gene set.

    go_overrepresentation.py <gene_set.txt> <interproscan_results.csv> <out.tsv>

The background is the annotated genome -- every ORF InterProScan matched -- not
the gene set. Taking the background from the set itself makes every term's
observed frequency equal its expected one, so nothing is ever significant and the
result reads as a real negative.

Reports raw p, Benjamini-Hochberg q, and the counts each was computed from.
"""
import csv
import sys

from scipy.stats import hypergeom

GO_COLUMN = "Ontology_term"
ORF_COLUMN = "orf"


def read_annotations(path: str) -> dict[str, set[str]]:
    by_orf: dict[str, set[str]] = {}
    with open(path, newline="", errors="replace") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames and GO_COLUMN in reader.fieldnames, (
            f"[{path}] has no [{GO_COLUMN}] column -- it was written by a parse "
            "that dropped InterProScan's GO terms, and there is no enrichment "
            "to compute without them"
        )
        for row in reader:
            orf = (row.get(ORF_COLUMN) or "").strip()
            if not orf:
                continue
            terms = {t.strip() for t in (row.get(GO_COLUMN) or "").split("|") if t.strip()}
            if terms:
                by_orf.setdefault(orf, set()).update(terms)
    return by_orf


def benjamini_hochberg(pvalues: list[float]) -> list[float]:
    n = len(pvalues)
    order = sorted(range(n), key=lambda i: pvalues[i])
    qs = [0.0] * n
    running = 1.0
    for rank, i in enumerate(reversed(order), start=1):
        running = min(running, pvalues[i] * n / (n - rank + 1))
        qs[i] = running
    return qs


def main(argv: list[str]) -> int:
    genes_path, ipr_path, out_path = argv
    with open(genes_path, errors="replace") as fh:
        wanted = {l.strip() for l in fh if l.strip()}
    by_orf = read_annotations(ipr_path)

    background = set(by_orf)
    selected = wanted & background
    N, n = len(background), len(selected)
    assert N, f"[{ipr_path}] annotated no ORF at all"

    terms: dict[str, tuple[int, int]] = {}
    for orf, go in by_orf.items():
        in_set = orf in selected
        for term in go:
            k, K = terms.get(term, (0, 0))
            terms[term] = (k + (1 if in_set else 0), K + 1)

    rows = []
    for term, (k, K) in sorted(terms.items()):
        # sf(k-1) is P(X >= k): the survival function is over X > k.
        p = float(hypergeom.sf(k - 1, N, K, n)) if n and k else 1.0
        rows.append({
            "go_term": term,
            "in_set": k,
            "in_background": K,
            "set_size": n,
            "background_size": N,
            "p_value": p,
        })
    for row, q in zip(rows, benjamini_hochberg([r["p_value"] for r in rows])):
        row["q_value"] = q

    rows.sort(key=lambda r: (r["p_value"], r["go_term"]))
    with open(out_path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]) if rows else
                                ["go_term", "in_set", "in_background", "set_size",
                                 "background_size", "p_value", "q_value"],
                                delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
    print(f"{len(rows)} GO terms; {n} of {len(wanted)} requested genes are annotated, "
          f"background {N} -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
