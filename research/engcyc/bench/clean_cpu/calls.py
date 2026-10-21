"""Run CLEAN's max-separation EC calls on embeddings from embed.py, and time each stage on CPU.

Runs inside the external_clean image with a writable --work. Two paths over the same embeddings:

  stock  CLEAN's own infer_maxsep stages (per-ORF .pt round trip, dict-of-dicts distance map,
         pandas nsmallest, GMM pickle reloaded for every EC call), timed stage by stage, plus
         the same writer with the GMM list loaded once ("cachedgmm") to isolate that cost.
  fast   in memory: one MLP pass, a batched distance matrix, a stable top-10, CLEAN's own
         maximum_separation, and each GMM's predict_proba over all chosen distances at once.

Writes <prefix>.calls.tsv (stock), <prefix>.fast.calls.tsv, and adds the timings to <prefix>.json.
"""
import argparse
import csv
import json
import os
import pickle
import resource
import time
import warnings

import numpy as np
import torch

warnings.filterwarnings("ignore", category=UserWarning)

p = argparse.ArgumentParser()
p.add_argument("--prefix", required=True)
p.add_argument("--work", required=True)
p.add_argument("--threads", type=int, default=8)
p.add_argument("--skip-stock", action="store_true")
a = p.parse_args()
torch.set_num_threads(a.threads)

meta = json.load(open(f"{a.prefix}.json"))
emb = np.load(f"{a.prefix}.npy")
ids = meta["ids"]
W = a.work
GMM = "./data/pretrained/gmm_ensumble.pkl"
for d in ("data/inputs", "data/esm_data", "results/inputs"):
    os.makedirs(os.path.join(W, d), exist_ok=True)
for src, dst in (("/app/data/pretrained", "data/pretrained"), ("/app/data/split100.csv", "data/split100.csv")):
    if not os.path.lexists(os.path.join(W, dst)):
        os.symlink(src, os.path.join(W, dst))
name = os.path.basename(a.prefix)
os.chdir(W)

from CLEAN.distance_map import get_dist_map_test  # noqa: E402
from CLEAN.evaluate import infer_confidence_gmm, maximum_separation, write_max_sep_choices  # noqa: E402
from CLEAN.model import LayerNormNet  # noqa: E402
from CLEAN.utils import get_ec_id_dict, model_embedding_test, seed_everything  # noqa: E402
import pandas as pd  # noqa: E402

T = {}


def tick(key, t0):
    T[key] = round(time.time() - t0, 3)
    return time.time()


dev, dt = torch.device("cpu"), torch.float32
t = time.time()
id_ec_train, ec_id_dict_train = get_ec_id_dict("./data/split100.csv")
model = LayerNormNet(512, 128, dev, dt)
model.load_state_dict(torch.load("./data/pretrained/split100.pth", map_location=dev))
model.eval()
emb_train = torch.load("./data/pretrained/100.pt", map_location=dev)
t = tick("t_setup", t)

if not a.skip_stock:
    t = time.time()
    for orf, v in zip(ids, emb):
        torch.save({"label": orf, "mean_representations": {33: torch.from_numpy(v)}},
                   os.path.join("data/esm_data", f"{orf}.pt"))
    with open(os.path.join("data/inputs", f"{name}.csv"), "w", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["Entry", "EC number", "Sequence"])
        for orf in ids:
            w.writerow([orf, " ", " "])
    t = tick("t_write_pt", t)
    id_ec_test, _ = get_ec_id_dict(f"./data/inputs/{name}.csv")
    with torch.no_grad():
        emb_test = model_embedding_test(id_ec_test, model, dev, dt)
    t = tick("t_load_pt_and_mlp", t)
    with torch.no_grad():
        eval_dist = get_dist_map_test(emb_train, emb_test, ec_id_dict_train, id_ec_test, dev, dt)
    t = tick("t_distmap_stock", t)
    seed_everything()
    eval_df = pd.DataFrame.from_dict(eval_dist)
    del eval_dist
    t = tick("t_dataframe_stock", t)
    write_max_sep_choices(eval_df, f"results/inputs/{name}", gmm=GMM)
    t = tick("t_maxsep_stock", t)

    gmm_once = pickle.load(open(GMM, "rb"))
    out = open(f"results/inputs/{name}_cached_maxsep.csv", "w", newline="")
    cw = csv.writer(out, delimiter=",")
    for col in eval_df.columns:
        s10 = eval_df[col].nsmallest(10)
        k = maximum_separation(list(s10), True, False)
        cw.writerow([col] + [f"EC:{s10.index[i]}/{infer_confidence_gmm(s10[i], gmm_once):.4f}" for i in range(k + 1)])
    out.close()
    t = tick("t_maxsep_cachedgmm", t)
    del eval_df
    meta["maxrss_gb_after_stock"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6, 2)
    with open(f"{a.prefix}.calls.tsv", "w") as fo:
        for line in open(f"results/inputs/{name}_maxsep.csv"):
            parts = line.rstrip("\n").split(",")
            for tok in parts[1:]:
                ec, score = tok[3:].rsplit("/", 1)
                fo.write(f"{parts[0]}\t{ec}\t{score}\n")
    same = sum(x == y for x, y in zip(open(f"results/inputs/{name}_maxsep.csv"),
                                      open(f"results/inputs/{name}_cached_maxsep.csv")))
    meta["cachedgmm_rows_identical"] = same

t0 = time.time()
ecs = list(ec_id_dict_train.keys())
centers = []
i0 = 0
for ec in ecs:
    n = len(ec_id_dict_train[ec])
    centers.append(emb_train[i0:i0 + n].mean(dim=0))
    i0 += n
lookup = torch.stack(centers)
gmms = pickle.load(open(GMM, "rb"))
true_idx = [0 if g.means_[0][0] < g.means_[1][0] else 1 for g in gmms]
with torch.no_grad():
    q = model(torch.from_numpy(emb).to(dt))
    rows = []
    for c in range(0, len(q), 64):
        d = (q[c:c + 64, None, :] - lookup[None]).norm(dim=2, p=2).numpy()
        top = np.argsort(d, axis=1, kind="stable")[:, :10]
        rows.append((top, np.take_along_axis(d, top, axis=1)))
top = np.concatenate([r[0] for r in rows])
dist = np.concatenate([r[1] for r in rows])
ks = [maximum_separation(list(dl), True, False) for dl in dist]
sel = [(i, j) for i, k in enumerate(ks) for j in range(k + 1)]
dsel = np.array([[dist[i, j]] for i, j in sel])
conf = np.mean([g.predict_proba(dsel)[:, ti] for g, ti in zip(gmms, true_idx)], axis=0)
with open(f"{a.prefix}.fast.calls.tsv", "w") as fo:
    for (i, j), c in zip(sel, conf):
        fo.write(f"{ids[i]}\t{ecs[top[i, j]]}\t{c:.4f}\n")
T["t_fast_total"] = round(time.time() - t0, 3)

if not a.skip_stock:
    stock = sorted(open(f"{a.prefix}.calls.tsv"))
    fast = sorted(open(f"{a.prefix}.fast.calls.tsv"))
    meta["fast_vs_stock_rows_identical"] = len(set(stock) & set(fast))
    meta["stock_rows"] = len(stock)
    meta["maxsep_wall_s"] = round(sum(T[k] for k in ("t_write_pt", "t_load_pt_and_mlp", "t_distmap_stock",
                                                      "t_dataframe_stock", "t_maxsep_stock")), 2)
meta["maxsep_timings"] = T
meta["maxrss_gb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6, 2)
meta["maxsep_threads"] = a.threads
json.dump(meta, open(f"{a.prefix}.json", "w"))
print(json.dumps({"prefix": name, "orfs": len(ids), **T,
                  **{k: meta.get(k) for k in ("maxsep_wall_s", "fast_vs_stock_rows_identical", "stock_rows",
                                              "cachedgmm_rows_identical", "maxrss_gb")}}), flush=True)
