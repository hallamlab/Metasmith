#!/usr/bin/env python3
import os, re, sys, csv, json, glob, collections

MSM = "/scratch/phyberos/gmcf3495/metasmith/runs"
INV = "/scratch/phyberos/gmcf3495/inv"
EMB, IDX = "jnwshMUg", "spk7Jvqs"

def merges(key):
    out = []
    with open(f"{INV}/attrib_{key}.tsv") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["transform"] == "merge_proteinbert":
                out.append((r["sample"], r["task_dir"]))
    return sorted(set(out))

def consumed(task_dir):
    sh = open(f"{task_dir}/.command.sh", errors="replace").read()
    return set(re.findall(r'[\w.\-]+-(?:%s|%s)\.(?:parquet|csv)' % (EMB, IDX), sh))

rows = []
for key in sys.argv[1:]:
    pool = {os.path.basename(p)
            for p in glob.glob(f"{MSM}/{key}/results/*proteinbert_*_chunk/*")}
    assigned, owner = set(), {}
    for sample, td in merges(key):
        c = consumed(td) & pool
        for b in c:
            owner[b] = sample
        assigned |= c
    left = sorted(pool - assigned)
    per = collections.Counter(owner.values())
    print(f"== {key}: {len(pool)} chunk files, {len(assigned)} consumed, "
          f"{len(left)} orphaned")
    for s, n in sorted(per.items(), key=lambda kv: -kv[1]):
        print(f"   {s:5s} {n//2:4d} chunk pairs")
    if left:
        print("   ORPHANED:")
        for b in left:
            print(f"     {b}")
    rows.append((key, owner, left))

with open(f"{INV}/pbert_chunk_owner.json", "w") as fh:
    json.dump({k: {"owner": o, "orphans": l} for k, o, l in rows}, fh, indent=1)
print(f"\nwrote {INV}/pbert_chunk_owner.json")
