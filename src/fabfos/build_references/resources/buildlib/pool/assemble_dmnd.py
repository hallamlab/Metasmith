import argparse as _argparse
import ast as _ast

_p = _argparse.ArgumentParser()
_p.add_argument("--index-name", required=True)
_p.add_argument("--pool", required=True)
_p.add_argument("--source-name", required=True)
_p.add_argument("--stack-name", required=True)
A = _p.parse_args()

import shutil
import numpy as np
import pandas as pd
from pathlib import Path

SHARDS = Path("pbert_output")
POOL = Path(A.pool)
POOL.mkdir(parents=True, exist_ok=True)

# The embedder writes one .npy per shard plus a csv index naming the sequences in order.
# Both are read in the SAME sorted shard order, so row i of the stack is sequence i of
# the index by construction rather than by coincidence.
npys = sorted(SHARDS.glob("*.npy"))
csvs = sorted(SHARDS.glob("*.csv"))
if not npys or not csvs:
    raise SystemExit(f"embedder produced no output under {SHARDS}")
# Narrow and downcast EACH shard before stacking, never after. Written the other way
# round -- vstack the full shards, then slice to 512 and cast -- the peak is every
# shard at full ProteinBERT width in its native dtype, PLUS vstack's own copy of all
# of it, and only then is 99% of that thrown away. On ~222k sequences that is the
# difference between tens of GB of transient and a couple.
stack = np.vstack([np.load(f)[:, -512:].astype(np.float32) for f in npys])
idx = pd.concat([pd.read_csv(f) for f in csvs], ignore_index=True)
if len(idx) != len(stack):
    raise SystemExit(f"index has {len(idx)} rows but the stack has {len(stack)} -- "
                     f"pairing them would misindex every row silently")

labels = pd.read_parquet("_pool_labels.parquet")
# `pbert` names its id column `id`; the run-side transform renames it to
# `sequence_id` on the way out. This reads the embedder's raw output, so it takes
# either -- and refuses rather than producing an unlabelled pool if neither is there.
for _cand in ("sequence_id", "id"):
    if _cand in idx.columns:
        idx = idx.rename(columns={_cand: "orf"})
        break
else:
    raise SystemExit(f"the embedder index has no id column: {list(idx.columns)}")
idx["row"] = np.arange(len(idx), dtype=np.int64)
idx["role"] = "reference"
merged = idx.merge(labels.rename(columns={"accession": "orf"}), on="orf", how="left")
n_unlabelled = int(merged["mnxr_list"].isna().sum())
merged["mnxr_list"] = merged["mnxr_list"].fillna("")

np.save(POOL / A.stack_name, stack)
merged[["role", "row", "orf", "mnxr_list"]].to_parquet(POOL / A.index_name, index=False)
shutil.copy("_pool_source.txt", POOL / A.source_name)
print(f"[pool] {len(merged):,} reference embeddings, {n_unlabelled:,} unlabelled, "
      f"{merged['mnxr_list'].str.split(';').explode().replace('', None).nunique():,} "
      f"distinct MNXR", flush=True)
# Every sequence written was selected BECAUSE it had labels, so an unlabelled row here is
# the embedder having dropped or renamed an id between the FASTA and its index -- which
# would misalign the merge rather than merely thin the pool.
if n_unlabelled:
    raise SystemExit(f"{n_unlabelled:,} embedded sequences carry no label, but the pool "
                     f"was selected on having one -- the embedder's index ids do not "
                     f"match the FASTA headers this transform wrote")
