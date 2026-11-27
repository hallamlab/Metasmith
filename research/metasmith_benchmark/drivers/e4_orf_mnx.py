import csv, glob, gzip, sys
from collections import defaultdict
from pathlib import Path

import yaml

sys.path.insert(0, "/scratch/phyberos/bench/checkout/41382ec2/research/metasmith_benchmark/drivers")
from e4_evidence import cache_shards, product_file
from e4_tally import bins_of

W = Path("/scratch/phyberos/e4_ablation/evidence")
R = Path("/scratch/phyberos/e4_ablation/metasmith/runs/mxQRJ8Dc/results")
index = yaml.load(open(R / "_metadata/index.yml"), Loader=yaml.CSafeLoader)["manifest"]
owner = bins_of(index)
shards = cache_shards(R.parents[2])
kofam_raw = {owner[p]: product_file(R, p, shards) for p, e in index.items()
             if e.get("type") == "annotation::kofamscan_results_chunk" and owner[p]}

MNX = W / "tmp/mnx"

# route_ko in src/fabfos/build_references/resources/buildlib/mnxr_lookup.py:
# KO -> KEGG reaction (ko2reaction) -> MNXR (reac_xref kegg.reaction / keggR)
kegg_mnxr, bigg_mnxr = defaultdict(set), defaultdict(set)
for line in open(MNX / "reac_xref.tsv"):
    if line.startswith("#"):
        continue
    src, mnxr = line.split("\t", 2)[:2]
    if not mnxr.startswith("MNXR"):
        continue
    ns, _, rid = src.partition(":")
    if ns in ("kegg.reaction", "keggR"):
        kegg_mnxr[rid].add(mnxr)
    elif ns in ("bigg.reaction", "biggR"):
        bigg_mnxr[rid].add(mnxr)
ko_mnxr = defaultdict(set)
for line in open(MNX / "ko2reaction.tsv"):
    ko, r = (x.split(":")[-1] for x in line.rstrip("\n").split("\t"))
    ko_mnxr[ko] |= kegg_mnxr.get(r, set())

gene_rxns = defaultdict(set)
for row in csv.DictReader(gzip.open(W.parent / "refs/carveme166_bigg_gprs.csv.gz", "rt")):
    gene_rxns[f"{row['model']}.{row['gene'][2:]}"].add(row["reaction"][2:])
universe_bigg = set().union(*gene_rxns.values())
universe_mnxr = set().union(*(bigg_mnxr.get(r, set()) for r in universe_bigg))
any_bigg_mnxr = set().union(*bigg_mnxr.values())
print(f"universe: {len(universe_bigg)} BiGG reactions with a GPR -> {len(universe_mnxr)} MNXR; "
      f"{sum(1 for r in universe_bigg if r in bigg_mnxr)} of them have an MNXR", file=sys.stderr)
print(f"KO with >=1 MNXR: {sum(1 for v in ko_mnxr.values() if v)} of {len(ko_mnxr)}", file=sys.stderr)


def kos_by_orf(path):
    out = defaultdict(set)
    for r in csv.DictReader(open(path)):
        if r["thrshld"] != "-" and float(r["score"]) >= float(r["thrshld"]):
            out[r["gene_name"]].add(r["KO"])
    return out


def diamond_by_orf(path):
    out = defaultdict(set)
    with gzip.open(path, "rt") as fh:
        for l in fh:
            if l.strip():
                orf, gene = l.split("\t", 2)[:2]
                out[orf].add(gene)
    return out


w = csv.writer(sys.stdout, delimiter="\t")
w.writerow(["bin", "orfs", "d_orfs", "d_mnx_orfs", "k_orfs", "k_mnx_orfs", "union_mnx_orfs",
            "k_only_mnx_orfs", "d_mnxr", "k_mnxr", "union_mnxr", "k_mnxr_not_d",
            "k_mnxr_not_d_in_universe", "k_mnxr_not_d_any_bigg", "k_only_orfs_universe",
            "k_mnxr_per_orf", "d_mnxr_per_orf"])
for faa in sorted(glob.glob("/scratch/phyberos/e4_ablation/published/*/proteins/*.faa")):
    b = Path(faa).stem
    dh = W / f"models/hits_diamond/{b}.tsv.gz"
    if not dh.exists() or b not in kofam_raw:
        continue
    n = sum(1 for l in open(faa) if l.startswith(">"))
    d = {o: set().union(*(bigg_mnxr.get(r, set()) for g in gs for r in gene_rxns.get(g, ())))
         for o, gs in diamond_by_orf(dh).items()}
    k = {o: set().union(*(ko_mnxr.get(x, set()) for x in ks)) for o, ks in kos_by_orf(kofam_raw[b]).items()}
    dm = {o for o, s in d.items() if s}
    km = {o for o, s in k.items() if s}
    d_r = set().union(*(d[o] for o in dm)) if dm else set()
    k_r = set().union(*(k[o] for o in km)) if km else set()
    extra = k_r - d_r
    k_only = km - dm
    k_only_univ = sum(1 for o in k_only if k[o] & universe_mnxr)
    w.writerow([b, n, len(d), len(dm), len(k), len(km), len(dm | km), len(k_only),
                len(d_r), len(k_r), len(d_r | k_r), len(extra), len(extra & universe_mnxr),
                len(extra & any_bigg_mnxr), k_only_univ,
                round(sum(len(k[o]) for o in km) / max(len(km), 1), 2),
                round(sum(len(d[o]) for o in dm) / max(len(dm), 1), 2)])
