"""Run CLEAN's max-separation EC calls on embeddings from embed.py, and time that step on CPU.

Runs inside the external_clean image with a writable --work. Writes <prefix>.calls.tsv
(orf, ec, score) and adds maxsep_wall_s to <prefix>.json.
"""
import argparse
import csv
import json
import os
import time

import numpy as np
import torch

p = argparse.ArgumentParser()
p.add_argument("--prefix", required=True)
p.add_argument("--work", required=True)
p.add_argument("--threads", type=int, default=8)
a = p.parse_args()
torch.set_num_threads(a.threads)

meta = json.load(open(f"{a.prefix}.json"))
emb = np.load(f"{a.prefix}.npy")
W = a.work
for d in ("data/inputs", "data/esm_data", "results/inputs"):
    os.makedirs(os.path.join(W, d), exist_ok=True)
for src, dst in (("/app/data/pretrained", "data/pretrained"), ("/app/data/split100.csv", "data/split100.csv")):
    if not os.path.lexists(os.path.join(W, dst)):
        os.symlink(src, os.path.join(W, dst))
for i, orf in enumerate(meta["ids"]):
    torch.save({"label": orf, "mean_representations": {33: torch.from_numpy(emb[i])}},
               os.path.join(W, "data/esm_data", f"{orf}.pt"))
name = os.path.basename(a.prefix)
with open(os.path.join(W, "data/inputs", f"{name}.csv"), "w", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    w.writerow(["Entry", "EC number", "Sequence"])
    for orf in meta["ids"]:
        w.writerow([orf, " ", " "])

os.chdir(W)
from CLEAN.infer import infer_maxsep  # noqa: E402

t0 = time.time()
infer_maxsep("split100", f"inputs/{name}", report_metrics=False, pretrained=True,
             gmm="./data/pretrained/gmm_ensumble.pkl")
meta["maxsep_wall_s"] = round(time.time() - t0, 1)
with open(f"{a.prefix}.calls.tsv", "w") as out:
    for line in open(f"results/inputs/{name}_maxsep.csv"):
        parts = line.rstrip("\n").split(",")
        for tok in parts[1:]:
            ec, score = tok[3:].rsplit("/", 1)
            out.write(f"{parts[0]}\t{ec}\t{score}\n")
json.dump(meta, open(f"{a.prefix}.json", "w"))
print(json.dumps({"prefix": name, "maxsep_wall_s": meta["maxsep_wall_s"], "orfs": len(meta["ids"])}), flush=True)
