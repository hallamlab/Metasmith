import csv, gzip, random, re, sys
from collections import Counter
from pathlib import Path

exec(open(Path(__file__).with_name("e4_orf_mnx.py")).read().split("w = csv.writer")[0])

OUT = Path(sys.argv[1])
RATE = {"remainder": 0.01, "no_reaction": 0.012, "reaction": 0.0045}
rng = random.Random(20261029)
ec_ko = {r["knum"] for r in csv.DictReader(open("/project/6004975/phyberos/lib/kofamscan/ko_list.tsv"), delimiter="\t")
         if "[EC:" in r["definition"]}


def read_faa(path):
    seqs, partial, name = {}, {}, None
    for l in open(path):
        if l.startswith(">"):
            name = l[1:].split()[0]
            m = re.search(r"partial=(\d\d)", l)
            partial[name] = m.group(1) if m else "00"
            seqs[name] = []
        else:
            seqs[name].append(l.strip())
    return {k: "".join(v).rstrip("*") for k, v in seqs.items()}, partial


def best_pident(path):
    best = {}
    with gzip.open(path, "rt") as fh:
        for l in fh:
            f = l.split("\t")
            if len(f) > 2:
                best[f[0]] = max(best.get(f[0], 0.0), float(f[2]))
    return best


OUT.mkdir(parents=True, exist_ok=True)
faa_out = open(OUT / "sample.faa", "w")
meta = csv.writer(open(OUT / "sample.tsv", "w"), delimiter="\t")
meta.writerow(["id", "bin", "pool", "length", "partial", "best_pident", "kos", "ko_ec"])
sizes = csv.writer(open(OUT / "pool_sizes.tsv", "w"), delimiter="\t")
sizes.writerow(["bin", "orfs", *RATE])
pident_hist = Counter()
for faa in sorted(glob.glob("/scratch/phyberos/e4_ablation/published/*/proteins/*.faa")):
    b = Path(faa).stem
    dh = W / f"models/hits_diamond/{b}.tsv.gz"
    if not dh.exists() or b not in kofam_raw:
        continue
    seqs, partial = read_faa(faa)
    d = diamond_by_orf(dh)
    kos = kos_by_orf(kofam_raw[b])
    rxn = {o for o, gs in d.items() if any(bigg_mnxr.get(r) for g in gs for r in gene_rxns.get(g, ()))}
    rxn |= {o for o, ks in kos.items() if any(ko_mnxr.get(k) for k in ks)}
    best = best_pident(dh)
    pident_hist.update(int(v) for v in best.values())
    pools = Counter()
    for o, s in seqs.items():
        pool = "reaction" if o in rxn else "no_reaction" if (o in d or o in kos) else "remainder"
        pools[pool] += 1
        if rng.random() >= RATE[pool]:
            continue
        sid = f"{b}__{o}"
        faa_out.write(f">{sid}\n{s}\n")
        ks = sorted(kos.get(o, ()))
        meta.writerow([sid, b, pool, len(s), partial[o], best.get(o, ""), ",".join(ks), int(any(k in ec_ko for k in ks))])
    sizes.writerow([b, len(seqs), *(pools[p] for p in RATE)])
faa_out.close()
with open(OUT / "mag_best_pident.tsv", "w") as fh:
    fh.write("pident_floor\torfs\n")
    for k in sorted(pident_hist):
        fh.write(f"{k}\t{pident_hist[k]}\n")
