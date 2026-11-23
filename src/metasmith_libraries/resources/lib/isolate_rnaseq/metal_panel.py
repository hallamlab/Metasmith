"""Expression and dose trend of the copper homeostasis genes and the BacMet metal-resistance hits.

    metal_panel.py <trend.csv> <bacmet.tsv> <genomic.gff> <out_dir>

`<out_dir>` gets `metal_gene_panel.{png,svg}` (the named copper genes plus every BacMet hit with a
significant trend) and `metal_gene_panel.csv` (every row either source names).
"""
import re
import sys
from pathlib import Path
from urllib.parse import unquote

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

ALPHA = 0.05
MIN_IDENTITY = 50.0
MAX_EVALUE = 1e-10
MAX_BACMET_ROWS = 40

# E. coli copper homeostasis: the cue and cus systems, CopA, the Cpx envelope-stress response
# that copper triggers, and the smaller copper-tolerance genes. Names as RefSeq annotates K-12.
COPPER_GENES = ["copA", "cueO", "cueR", "cusS", "cusR", "cusC", "cusF", "cusB", "cusA",
                "cpxP", "cpxR", "cpxA", "spy", "cutC", "cutA", "yobA", "copD", "comR",
                "bhsA", "zntA", "zraP", "mntH", "soxS", "sodA"]

# BacMet2 titles end with the organism, then the gene: "... [Mycobacterium smegmatis] [ctpD]".
BACMET = ["qseqid", "sseqid", "pident", "length", "mismatch", "gapopen", "qstart", "qend",
          "sstart", "send", "evalue", "bitscore", "stitle"]


def protein_to_locus(gff: Path) -> dict[str, str]:
    genes, out = {}, {}
    for line in gff.read_text().splitlines():
        f = line.split("\t")
        if line.startswith("#") or len(f) < 9:
            continue
        attrs = {k: unquote(v) for k, v in (kv.split("=", 1) for kv in f[8].split(";") if "=" in kv)}
        if f[2] == "gene" and "locus_tag" in attrs:
            genes[attrs["ID"]] = attrs["locus_tag"]
        elif f[2] == "CDS" and "protein_id" in attrs and attrs.get("Parent") in genes:
            out.setdefault(attrs["protein_id"], []).append(genes[attrs["Parent"]])
    return out


def bacmet_hits(path: Path, loci: dict, row_of: dict) -> pd.DataFrame:
    hits = pd.read_csv(path, sep="\t", names=BACMET, header=None)
    hits = hits[(hits.pident >= MIN_IDENTITY) & (hits.evalue <= MAX_EVALUE)]
    hits = hits.sort_values("bitscore", ascending=False).drop_duplicates("qseqid")
    rows = []
    for _, h in hits.iterrows():
        for locus in loci.get(h.qseqid, []):
            if locus in row_of:
                named = re.findall(r"\[([^\]]+)\]", h.stitle)
                rows.append({"gene_id": row_of[locus], "bacmet_gene": named[-1] if named else h.sseqid,
                             "bacmet_identity": h.pident, "bacmet_description": h.stitle})
    return pd.DataFrame(rows, columns=["gene_id", "bacmet_gene", "bacmet_identity", "bacmet_description"]
                        ).drop_duplicates("gene_id").set_index("gene_id")


def draw(panel: pd.DataFrame, tpm_cols: list[str], out: Path) -> None:
    names = [n if isinstance(n, str) and n else g for g, n in zip(panel.index, panel.gene_name)]
    names = [f"{n} ({b})" if isinstance(b, str) and b and b.lower() != str(n).lower() else n
             for n, b in zip(names, panel.bacmet_gene)]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 0.32 * len(panel) + 2), gridspec_kw={"width_ratios": [3, 1]})
    im = a1.imshow(np.log10(panel[tpm_cols].values + 1), aspect="auto", cmap="viridis")
    a1.set_xticks(range(len(tpm_cols)), [c.removeprefix("TPM_").removesuffix("ppm") for c in tpm_cols])
    a1.set_yticks(range(len(panel)), names)
    a1.set_xlabel("CuSO4 (ppm)")
    a1.set_title("Expression, log10(TPM + 1)")
    fig.colorbar(im, ax=a1, fraction=0.03)
    a2.barh(range(len(panel)), panel.log2FC_per_10x_copper.values,
            color=np.where(panel.padj < ALPHA, "#c0392b", "#c4c8d0"))
    a2.set_yticks(range(len(panel)), names)
    a2.set_ylim(len(panel) - 0.5, -0.5)
    a2.axvline(0, c="#555", lw=0.8)
    a2.set_xlabel("log2FC per 10x copper")
    a2.set_title(f"Trend (red: padj < {ALPHA})")
    fig.tight_layout()
    fig.savefig(out / "metal_gene_panel.png", dpi=200)
    fig.savefig(out / "metal_gene_panel.svg")
    plt.close(fig)


def main(trend_path: str, bacmet_path: str, gff: str, out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    trend = pd.read_csv(trend_path, index_col="gene_id")
    row_of = {locus: gid for gid in trend.index for locus in gid.split("+")}
    hits = bacmet_hits(Path(bacmet_path), protein_to_locus(Path(gff)), row_of)
    table = trend.join(hits)
    named = table.gene_name.isin(COPPER_GENES)
    table["source"] = np.where(named & table.bacmet_gene.notna(), "copper list + BacMet",
                               np.where(named, "copper list", "BacMet"))
    listed = table[named | table.bacmet_gene.notna()]
    tpm_cols = [c for c in trend.columns if c.startswith("TPM_")]

    copper = listed[named.loc[listed.index]].copy()
    copper["order"] = [COPPER_GENES.index(n) for n in copper.gene_name]
    copper = copper.sort_values("order").drop(columns="order")
    extra = listed[~named.loc[listed.index] & (listed.padj < ALPHA)]
    extra = extra.reindex(extra.log2FC_per_10x_copper.abs().sort_values(ascending=False).index)[:MAX_BACMET_ROWS]
    draw(pd.concat([copper, extra]), tpm_cols, out)

    keep = ["source", "gene_name", "product", "bacmet_gene", "bacmet_identity", "bacmet_description",
            "copies", "multimap_fraction", "log2FC_per_10x_copper", "padj"] + tpm_cols
    listed[keep].sort_values("padj").to_csv(out / "metal_gene_panel.csv")
    print(f"{len(copper)} named copper genes, {int(table.bacmet_gene.notna().sum())} BacMet hits, "
          f"{len(extra)} of them drawn with a significant trend")


if __name__ == "__main__":
    main(*sys.argv[1:5])
