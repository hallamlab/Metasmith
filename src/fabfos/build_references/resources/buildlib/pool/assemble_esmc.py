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

POOL = Path(A.pool)
POOL.mkdir(parents=True, exist_ok=True)

stack = np.load("_esmc_emb.npy")
idx = pd.read_csv("_esmc_index.csv")
if len(idx) != len(stack):
    raise SystemExit(f"index has {len(idx)} rows but the stack has {len(stack)} -- "
                     f"pairing them would misindex every row silently")

labels = pd.read_parquet("_pool_labels.parquet")
idx = idx.rename(columns={"sequence_id": "orf"})
idx["row"] = np.arange(len(idx), dtype=np.int64)
idx["role"] = "reference"
merged = idx.merge(labels.rename(columns={"accession": "orf"}), on="orf", how="left")
n_unlabelled = int(merged["mnxr_list"].isna().sum())
merged["mnxr_list"] = merged["mnxr_list"].fillna("")

np.save(POOL / A.stack_name, stack)
merged[["role", "row", "orf", "mnxr_list"]].to_parquet(POOL / A.index_name, index=False)
shutil.copy("_pool_source.txt", POOL / A.source_name)
print(f"[pool-esmc] {len(merged):,} reference embeddings, {n_unlabelled:,} "
      f"unlabelled, "
      f"{merged['mnxr_list'].str.split(';').explode().replace('', None).nunique():,} "
      f"distinct MNXR", flush=True)
# Every sequence written was selected BECAUSE it had labels, so an unlabelled row here
# is the embedder having dropped or renamed an id between the FASTA and its index --
# which misaligns the merge rather than merely thinning the pool.
if n_unlabelled:
    raise SystemExit(f"{n_unlabelled:,} embedded sequences carry no label, but the "
                     f"pool was selected on having one -- the embedder's index ids do "
                     f"not match the FASTA headers this transform wrote")
