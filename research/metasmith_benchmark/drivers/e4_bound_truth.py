import csv, gzip, re, sys
from pathlib import Path

BOUND = Path(sys.argv[1])
REF = Path("/scratch/phyberos/e4_ablation/refs/carveme166_bigg_proteins.faa")

# Every BiGG model in the same order as the test organism leaves the DIAMOND reference, so a
# curated genome meets the references at the distance a MAG does rather than finding itself.
MASK = {
    "iML1515": ["iML1515", "STM_v1_0", "iYL1228", "iSSON_1240", "iSF_1195", "iSbBS512_1146", "iSDY_1059", "iPC815"],
    "iYO844": ["iYO844", "iYS854"],
    "iJN1463": ["iJN1463", "iCN718"],
    "iSynCJ816": ["iSynCJ816"],
    "iAF987": ["iAF987"],
    "iLJ478": ["iLJ478"],
}


def fasta(lines):
    name, seq = None, []
    for l in lines:
        if l.startswith(">"):
            if name:
                yield name, "".join(seq)
            name, seq = l[1:].rstrip("\n"), []
        else:
            seq.append(l.strip())
    if name:
        yield name, "".join(seq)


ref = list(fasta(open(REF)))
model_genes = {}
for h, _ in ref:
    m, g = h.split(".", 1)
    model_genes.setdefault(m, set()).add(g.replace("_", ""))

truth = BOUND / "truth"
rows = csv.writer(open(truth / "truth.tsv", "w"), delimiter="\t")
rows.writerow(["id", "model", "locus", "in_model", "length"])
with open(truth / "queries.faa", "w") as q:
    for m, masked in MASK.items():
        seen = set()
        for h, s in fasta(gzip.open(truth / "raw" / f"{m}.faa.gz", "rt")):
            tag = re.search(r"\[locus_tag=([^\]]+)\]", h)
            if not tag or tag.group(1) in seen or "[pseudo=true]" in h:
                continue
            seen.add(tag.group(1))
            sid = f"{m}__{tag.group(1)}"
            q.write(f">{sid}\n{s.rstrip('*')}\n")
            rows.writerow([sid, m, tag.group(1), int(tag.group(1).replace("_", "") in model_genes[m]), len(s)])
        with open(truth / f"ref_{m}.faa", "w") as r:
            for h, s in ref:
                if h.split(".", 1)[0] not in masked:
                    r.write(f">{h}\n{s}\n")
