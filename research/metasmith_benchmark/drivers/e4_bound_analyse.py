import csv, re, statistics, sys
from collections import Counter, defaultdict
from pathlib import Path

exec(open(Path(__file__).with_name("e4_orf_mnx.py")).read().split("w = csv.writer")[0])

B = Path(sys.argv[1])
TIERS_OUT, RECALL_OUT = Path(sys.argv[2]), Path(sys.argv[3])
PFAM = Path("/scratch/phyberos/refs/vibrant_1.2.1/databases/Pfam-A_v32.HMM")
MODELS = ["iML1515", "iYO844", "iJN1463", "iSynCJ816", "iAF987", "iLJ478"]
CAPS = (60, 50, 40)
UNINFORMATIVE = re.compile(r"uncharacteri[sz]ed|hypothetical|unknown function|DUF\d|domain-containing protein$|^protein$|putative protein", re.I)

sample = list(csv.DictReader(open(B / "samples/sample.tsv"), delimiter="\t"))
truth = list(csv.DictReader(open(B / "truth/truth.tsv"), delimiter="\t"))

uniref = defaultdict(list)
for l in open(B / "out/uniref50.tsv"):
    f = l.rstrip("\n").split("\t")
    m = re.match(r"\S+ (.*?) n=\d+", f[6])
    uniref[f[0]].append(m.group(1) if m else f[6])

pfam = defaultdict(set)
for l in open(B / "out/pfam.tblout"):
    if not l.startswith("#"):
        f = l.split()
        pfam[f[0]].add(f[2])
desc = {}
with open(PFAM) as fh:
    for l in fh:
        if l.startswith("NAME"):
            name = l.split(None, 1)[1].strip()
        elif l.startswith("DESC"):
            desc[name] = l.split(None, 1)[1].strip()
duf = {n for n, d in desc.items() if n.startswith("DUF") or re.search(r"unknown function|uncharacteri[sz]ed", d, re.I)}

clean = {}
for r in csv.DictReader(open(B / "out/clean.tsv"), delimiter="\t"):
    s = float(r["clean_score"]) if r["clean_score"] not in ("", "NA") else 0.0
    if s > clean.get(r["Query ID"], (0.0, ""))[0]:
        clean[r["Query ID"]] = (s, r["Predicted EC number"])


def bigg_hits(path, cap=None):
    out = defaultdict(set)
    best = {}
    for l in open(path):
        q, s, pid = l.split("\t")[:3]
        if cap is not None and float(pid) >= cap:
            continue
        out[q].add(s)
        best[q] = max(best.get(q, 0.0), float(pid))
    return out, best


def reaction_orfs(hits):
    return {q for q, gs in hits.items() if any(bigg_mnxr.get(r) for g in gs for r in gene_rxns.get(g, ()))}


kofam = defaultdict(set)
for l in open(B / "out/kofam_truth.tsv"):
    if l.startswith("*"):
        f = l.rstrip("\n").split("\t")
        kofam[f[1]].add(f[2])
ko_rxn = {q for q, ks in kofam.items() if any(ko_mnxr.get(k) for k in ks)}

pipeline = {}
for m in MODELS:
    hits, best = bigg_hits(B / f"out/bigg_{m}.tsv")
    capped = {c: reaction_orfs(bigg_hits(B / f"out/bigg_{m}.tsv", c)[0]) for c in CAPS}
    pipeline[m] = {"hits": hits, "best": best, "d": reaction_orfs(hits), "capped": capped}

# Pfam families are scored metabolic from the MAG reaction and no-reaction pools alone, so neither the
# remainder nor the curated genomes they are then applied to vote on their own labels.
label = {r["id"]: r["pool"] == "reaction" for r in sample if r["pool"] != "remainder"}
carriers = defaultdict(list)
for i, lab in label.items():
    for fam in pfam.get(i, ()):
        carriers[fam].append(lab)
metabolic = {f for f, v in carriers.items() if len(v) >= 5 and sum(v) / len(v) >= 0.5}
print(f"Pfam families hit: {len(carriers)}; metabolic (>=5 carriers, >=50% with a reaction): {len(metabolic)}; "
      f"DUF/unknown: {len(duf & set(carriers))}", file=sys.stderr)


def tier(i):
    fams = pfam.get(i, set())
    s = clean.get(i, (0.0, ""))[0]
    met = bool(fams & metabolic)
    if met and s >= 0.9:
        return "metabolic_strong"
    if met or s >= 0.5:
        return "metabolic_plausible"
    if (fams - duf) or any(not UNINFORMATIVE.search(n) for n in uniref.get(i, ())):
        return "protein_known_nonmetabolic"
    if i in uniref or fams:
        return "protein_unknown_function"
    return "no_homolog"


TIER_NAMES = ["metabolic_strong", "metabolic_plausible", "protein_known_nonmetabolic", "protein_unknown_function", "no_homolog"]
groups = defaultdict(list)
for r in sample:
    groups[f"mag_{r['pool']}"].append(r)
for r in truth:
    m, i = r["model"], r["id"]
    rx = i in pipeline[m]["d"] or i in ko_rxn
    ann = i in pipeline[m]["hits"] or i in kofam
    g = "truth_model_gene" if r["in_model"] == "1" else "truth_other_reaction" if rx else "truth_other_annotated" if ann else "truth_remainder"
    groups[g].append(r)

cols = ["group", "orfs", "median_length", "short_lt100", "partial", "uniref50_hit", "uniref50_named", "pfam_any",
        "pfam_metabolic", "pfam_duf_only", "clean_ge_0.5", "clean_ge_0.9", *TIER_NAMES]
with open(TIERS_OUT, "w") as fh:
    w = csv.writer(fh, delimiter="\t")
    w.writerow(cols)
    for g in ["mag_remainder", "mag_no_reaction", "mag_reaction", "truth_remainder", "truth_other_annotated",
              "truth_other_reaction", "truth_model_gene"]:
        rows = groups[g]
        n = len(rows)
        ids = [r["id"] for r in rows]
        frac = lambda k: round(k / n, 4) if n else ""
        t = Counter(tier(i) for i in ids)
        w.writerow([g, n, statistics.median(int(r["length"]) for r in rows), frac(sum(int(r["length"]) < 100 for r in rows)),
                    frac(sum(r.get("partial", "00") != "00" for r in rows)) if g.startswith("mag") else "",
                    frac(sum(i in uniref for i in ids)),
                    frac(sum(any(not UNINFORMATIVE.search(x) for x in uniref.get(i, ())) for i in ids)),
                    frac(sum(bool(pfam.get(i)) for i in ids)), frac(sum(bool(pfam.get(i, set()) & metabolic) for i in ids)),
                    frac(sum(bool(pfam.get(i)) and pfam[i] <= duf for i in ids)),
                    frac(sum(clean.get(i, (0,))[0] >= 0.5 for i in ids)), frac(sum(clean.get(i, (0,))[0] >= 0.9 for i in ids)),
                    *(frac(t[x]) for x in TIER_NAMES)])

rcols = ["model", "proteins", "model_genes", "model_gene_share", "pipeline_reaction_orfs", "pipeline_share",
         "recall_diamond", "recall_kofam", "recall_union", *(f"recall_union_pident_lt{c}" for c in CAPS),
         "non_model_reaction_orfs", "best_pident_median", "missed_model_genes", "missed_clean_ge_0.5",
         "missed_pfam_metabolic", "missed_either", "remainder_share"]
with open(RECALL_OUT, "w") as fh:
    w = csv.writer(fh, delimiter="\t")
    w.writerow(rcols)
    for m in MODELS:
        rows = [r for r in truth if r["model"] == m]
        ids = {r["id"] for r in rows}
        T = {r["id"] for r in rows if r["in_model"] == "1"}
        p = pipeline[m]
        d, k = p["d"] & ids, ko_rxn & ids
        u = d | k
        missed = T - u
        cm = lambda xs: sum(clean.get(i, (0,))[0] >= 0.5 for i in xs)
        pm = lambda xs: sum(bool(pfam.get(i, set()) & metabolic) for i in xs)
        either = sum(clean.get(i, (0,))[0] >= 0.5 or bool(pfam.get(i, set()) & metabolic) for i in missed)
        rem = ids - set(p["hits"]) - set(kofam)
        r3 = lambda x: round(x, 4)
        w.writerow([m, len(ids), len(T), r3(len(T) / len(ids)), len(u), r3(len(u) / len(ids)),
                    r3(len(d & T) / len(T)), r3(len(k & T) / len(T)), r3(len(u & T) / len(T)),
                    *(r3(len(((p["capped"][c] & ids) | k) & T) / len(T)) for c in CAPS),
                    len(u - T), round(statistics.median(v for q, v in p["best"].items() if q in ids), 1),
                    len(missed), cm(missed), pm(missed), either, r3(len(rem) / len(ids))])

hist = [l.split("\t") for l in open(B / "samples/mag_best_pident.tsv")][1:]
expanded = sorted((int(a), int(b)) for a, b in hist)
total, acc = sum(b for _, b in expanded), 0
for a, b in expanded:
    acc += b
    if acc >= total / 2:
        print(f"MAG DIAMOND best-hit identity, median over ORFs with a hit: {a}%", file=sys.stderr)
        break
sizes = list(csv.DictReader(open(B / "samples/pool_sizes.tsv"), delimiter="\t"))
for p in ("reaction", "no_reaction", "remainder"):
    print(f"MAG {p} share, median over bins: {statistics.median(int(r[p]) / int(r['orfs']) for r in sizes):.4f}", file=sys.stderr)
