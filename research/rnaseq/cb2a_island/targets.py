import re, sys
import pandas as pd
D = sys.argv[1]; G = sys.argv[2]
gff = pd.read_csv(f"{G}/genomic.gff", sep="\t", comment="#", header=None,
                  names=["seq","src","type","start","end","score","strand","phase","attr"])
cds = gff[gff.type == "CDS"].copy()
cds["gene_id"] = cds.attr.str.extract(r"locus_tag=([^;]+)")
cds["protein_id"] = cds.attr.str.extract(r"protein_id=([^;]+)")
cds["product"] = cds.attr.str.extract(r"product=([^;]+)")
cds = cds.dropna(subset=["protein_id"]).drop_duplicates("gene_id")
tpm = pd.read_csv(f"{D}/cb2a_pipeline_from_raw_reads/tpm_table.tsv", sep="\t")
idcol = tpm.columns[0]
t = cds.merge(tpm.rename(columns={idcol: "gene_id"})[["gene_id","TPM_cb2a_cu3","count_cb2a_cu3"]], on="gene_id", how="left")
TPM = [c for c in t.columns if c.startswith("TPM")][0]
island = {f"ACP82L_RS{n:05d}" for n in range(7640, 7930, 5)}
top = set(t.nlargest(30, TPM).gene_id)
t["in_island"] = t.gene_id.isin(island); t["in_top30"] = t.gene_id.isin(top)
sel = t[t.in_island | t.in_top30].copy()
unnamed = re.compile(r"hypothetical|DUF|domain-containing|family protein|PepSY|SRPBCC|uncharacterized", re.I)
sel["unnamed"] = sel["product"].str.contains(unnamed, na=True)
seqs, cur = {}, None
for line in open(f"{G}/protein.faa"):
    if line.startswith(">"): cur = line[1:].split()[0]; seqs[cur] = []
    else: seqs[cur].append(line.strip())
sel["aa"] = sel.protein_id.map(lambda p: "".join(seqs[p]))
sel["len"] = sel.aa.str.len()
sel = sel.sort_values("start")
sel[["gene_id","protein_id","start","end","strand","product",TPM,"in_island","in_top30","unnamed","len"]].to_csv(f"{D}/targets.tsv", sep="\t", index=False)
with open(f"{D}/targets.faa","w") as f:
    for r in sel.itertuples(): f.write(f">{r.gene_id}\n{r.aa}\n")
print(len(sel), "proteins;", sel.in_island.sum(), "island;", sel.in_top30.sum(), "top30;", sel.unnamed.sum(), "unnamed;", (sel.in_top30 & ~sel.in_island).sum(), "top30 outside island")
print(sel[sel.unnamed][["gene_id","product",TPM,"len","in_island","in_top30"]].to_string())
