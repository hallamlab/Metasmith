#!/usr/bin/env python3
import os, re, csv, glob, collections

MSM = "/scratch/phyberos/gmcf3495/metasmith/runs"
INV = "/scratch/phyberos/gmcf3495/inv"

def merges(key, transform):
    out = set()
    with open(f"{INV}/attrib_{key}.tsv") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["transform"] == transform:
                out.add((r["sample"], r["task_dir"]))
    return sorted(out)

FAMILIES = {
    "merge_proteinbert":     ["*proteinbert_embeddings_chunk", "*proteinbert_index_chunk"],
    "merge_eggnog_mapper":   ["*eggnog_results_chunk"],
    "merge_diamond_uniref50":["*diamond_uniref50_results_chunk"],
    "merge_kofamscan":       ["*kofamscan_results_chunk"],
}

for key in ("QkqCNJOo", "hEYVT7HY"):
    print(f"== {key}")
    known = {r["transform"] for r in csv.DictReader(open(f"{INV}/attrib_{key}.tsv"), delimiter="\t")}
    for tf, globs in FAMILIES.items():
        if tf not in known:
            print(f"   {tf:26s} NO SUCH TRANSFORM in attribution")
            continue
        pool = set()
        for g in globs:
            for p in glob.glob(f"{MSM}/{key}/results/{g}/*"):
                pool.add(os.path.basename(p))
        seen = set()
        for _, td in merges(key, tf):
            sh = open(f"{td}/.command.sh", errors="replace").read()
            seen |= {b for b in pool if b in sh}
        orphans = sorted(pool - seen)
        flag = "OK" if not orphans else f"*** {len(orphans)} ORPHANED ***"
        print(f"   {tf:26s} pool={len(pool):5d} consumed={len(seen):5d}  {flag}")
        for o in orphans[:12]:
            print(f"        {o}")
