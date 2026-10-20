"""Compare each run in out/ with its fp32 twin: embedding cosine, top-EC agreement, EC-set agreement."""
import collections
import glob
import json
import os
import sys

import numpy as np

B = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))


def calls(prefix):
    by = collections.defaultdict(list)
    for line in open(f"{prefix}.calls.tsv"):
        orf, ec, score = line.rstrip("\n").split("\t")
        by[orf].append((float(score), ec))
    return {o: (max(v)[1], frozenset(e for _, e in v)) for o, v in by.items()}


rows = []
for js in sorted(glob.glob(f"{B}/out/*.json")):
    m = json.load(open(js))
    prefix = js[:-5]
    name = os.path.basename(prefix)
    row = {k: m.get(k) for k in ("mode", "procs", "threads", "orfs", "embed_wall_s", "orf_per_s",
                                 "orf_per_s_per_core", "maxsep_wall_s")}
    row["name"] = name
    twin = prefix.replace(f"_{m['mode']}_", "_fp32_")
    ref = f"{twin}.json" if os.path.exists(f"{twin}.json") else min(glob.glob(f"{twin.rsplit('_', 1)[0]}_*.json"), default=None)
    if m["mode"] != "fp32" and ref and os.path.exists(ref[:-5] + ".calls.tsv") and os.path.exists(f"{prefix}.calls.tsv"):
        r = json.load(open(ref))
        a, b = np.load(f"{prefix}.npy"), np.load(ref[:-5] + ".npy")
        idx = {o: i for i, o in enumerate(r["ids"])}
        b = b[[idx[o] for o in m["ids"]]]
        cos = (a * b).sum(1) / np.linalg.norm(a, axis=1) / np.linalg.norm(b, axis=1)
        ca, cb = calls(prefix), calls(ref[:-5])
        common = [o for o in ca if o in cb]
        row["cos_min"] = round(float(cos.min()), 5)
        row["cos_mean"] = round(float(cos.mean()), 6)
        row["top_ec_agree"] = round(sum(ca[o][0] == cb[o][0] for o in common) / len(common), 4)
        row["ec_set_agree"] = round(sum(ca[o][1] == cb[o][1] for o in common) / len(common), 4)
    rows.append(row)
for r in rows:
    print(json.dumps(r))
