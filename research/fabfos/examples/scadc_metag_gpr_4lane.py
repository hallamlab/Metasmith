#!/usr/bin/env python3
"""Add the ProteinBERT lane to the metagenome GPR table -> `gpr_4lane.parquet`.

    python examples/scadc_metag_gpr_4lane.py --lane       # the expensive half, resumable
    python examples/scadc_metag_gpr_4lane.py --assemble   # 3 lanes + pbert -> gpr_4lane
    python examples/scadc_metag_gpr_4lane.py --lane --assemble

WHY THIS EXISTS
---------------
`data/fabfos/runs/scadc_metagenome/gpr/gpr_3lane.parquet` carries clean/kofam/uniref50
but not `pbert`, because when it was compiled the embed-transfer lane needed a
dense (222,019 x 13,112) float32 indicator matrix -- 10.84 GiB, 99.97% zeros --
and running it over 1,442,614 ORFs was not schedulable. Two things have changed:
the lane's vote is now a CSR-shaped gather (see `gpr_4lane.py::lane_embed`), and
the metagenome's ProteinBERT embeddings are already computed and pinned at
`annotations/proteinbert/`. So the lane is now a few CPU-hours, not new
engineering.

That matters because the OBSERVED fosmid measurement
(`data/fabfos/runs/scadc_fosmids/gpr/gpr_4lane.parquet`) is 4-lane while the null it
was scored against was drawn from a 3-lane pool. The bases did not match. This
closes that.

THE VOTE IS NOT REIMPLEMENTED HERE
----------------------------------
`lane_embed` is lifted out of the live transform's DRIVER string and executed,
the same way `tests/test_gpr_4lane_sparse_transfer.py` lifts it -- a copy would
keep passing after the transform changed, and this lane's whole claim is that
the metagenome pool is scored by the SAME rule as the fosmid units it is the
null for. Only `_load_query` is replaced, because the metagenome embeddings are
a float16 `.npy` stack with a `contig,orf` index while the transform's input is
a parquet with a `sequence_id` index. The arithmetic is untouched.

ROW ORDER WAS VERIFIED, NOT ASSUMED
-----------------------------------
`examples/fir/repack_lanes.py` records that the legacy ProteinBERT embeddings'
row order is NOT the fasta's (the combiner concatenated chunk files in
lexicographic name order, so `chunk_10` preceded `chunk_2`), which would
attribute every vote to the wrong ORF while producing a full, schema-valid
table. This stack is clean: `metag.pbert.index.csv` is exactly fasta order minus
15 ORFs -- one dropped at the tail of each of 16 embedding chunks, positions
90162 + k*90163 -- and embedding dim 133 correlates with log ORF length at
r = -0.60 in EVERY one of those 16 blocks under this pairing (0.01 under a
shuffled control). A block permutation would read ~0. `--check-alignment`
re-runs that.

Runs locally: the pool is 455 MB, one similarity block is 227 MB, and the query
stack is 2.95 GB as float32. Slabbed at 50,000 queries so a kill costs one slab.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TRANSFORM = ROOT / "src" / "metasmith_libraries" / "transforms" / "fabfos" / "gpr_4lane.py"
LIB = ROOT / "src" / "metasmith_libraries" / "resources" / "lib"

METAG = ROOT / "data" / "fabfos" / "runs" / "scadc_metagenome"
EMB = METAG / "annotations" / "proteinbert" / "metag.pbert.npy"
EMB_IDX = METAG / "annotations" / "proteinbert" / "metag.pbert.index.csv"
ORFS_CSV = METAG / "sequences" / "metag.orfs.csv"
GPR3 = METAG / "gpr" / "gpr_3lane.parquet"
GPR4 = METAG / "gpr" / "gpr_4lane.parquet"
POOL = ROOT / "data" / "fabfos" / "processed" / "reference_label_pool" / "pool"

SLABS = ROOT / "data" / "fabfos" / "scratch" / "metag_pbert_lane"

SOURCE = "metag"
LANE_SET = "chosen_4"
SLAB = 50_000
THREADS = int(os.environ.get("GPR_THREADS", "16"))


def load_lane_ns():
    src = TRANSFORM.read_text()
    body = re.search(r"DRIVER = r'''\n(.*?)\n'''", src, re.S).group(1)
    prelude = body[: body.index("def main():")]
    filled = prelude.format(ev_lib=str(LIB / "fabfos_evidence.py"),
                            lane_set=LANE_SET, source=SOURCE, threads=THREADS)
    ns = {"__name__": "gpr_4lane_driver"}
    exec(compile(filled, str(TRANSFORM) + "::DRIVER", "exec"), ns)  # noqa: S102
    return ns


def query_ids(pd):
    idx = pd.read_csv(EMB_IDX)
    return (idx["contig"].astype(str) + "_" + idx["orf"].astype(str)).to_numpy()


def install_loader(ns, np, pd, lo, hi):
    ids = query_ids(pd)
    stack = np.load(EMB, mmap_mode="r")
    if len(ids) != len(stack):
        raise SystemExit(
            f"[metag-pbert] the index has {len(ids):,} rows and the stack has "
            f"{len(stack):,}. The lane addresses the stack BY ROW, so these "
            f"cannot be paired -- every vote would be attributed to the wrong ORF")

    def _load_query(_parquet, _index_csv):
        return ids[lo:hi], np.asarray(stack[lo:hi], dtype=np.float32)

    ns["_load_query"] = _load_query
    return len(ids)


def run_lane():
    ns = load_lane_ns()
    np, pd = ns["np"], ns["pd"]
    SLABS.mkdir(parents=True, exist_ok=True)
    n = len(query_ids(pd))
    bounds = [(s, min(s + SLAB, n)) for s in range(0, n, SLAB)]
    print(f"[metag-pbert] {n:,} embedded ORFs, {len(bounds)} slabs of {SLAB:,}", flush=True)

    for i, (lo, hi) in enumerate(bounds):
        out = SLABS / f"slab_{i:03d}.parquet"
        if out.exists():
            print(f"[metag-pbert] slab {i:03d} present, skipping", flush=True)
            continue
        install_loader(ns, np, pd, lo, hi)
        t0 = time.time()
        df = ns["lane_embed"](None, None, str(POOL), "emb_pbert.npy", "pbert", ns["PBERT_FLOOR"])
        tmp = out.with_suffix(".partial")
        df.to_parquet(tmp, index=False)
        tmp.rename(out)
        print(f"[metag-pbert] slab {i:03d} rows[{lo:,}:{hi:,}] -> {len(df):,} rows "
              f"in {time.time()-t0:.0f}s", flush=True)


def assemble():
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    sys.path.insert(0, str(LIB))
    import fabfos_evidence as fe

    slabs = sorted(SLABS.glob("slab_*.parquet"))
    if not slabs:
        raise SystemExit(f"[metag-pbert] no slabs in {SLABS} -- run --lane first")
    n = len(query_ids(pd))
    want = (n + SLAB - 1) // SLAB
    if len(slabs) != want:
        raise SystemExit(
            f"[metag-pbert] {len(slabs)} slabs on disk, {want} expected. Assembling "
            f"a partial lane would produce a full, schema-valid table missing an "
            f"arbitrary slice of the ORFs -- re-run --lane to fill the gaps")

    pbert = pa.concat_tables([pq.read_table(s) for s in slabs])
    three = pq.read_table(GPR3)
    if three.schema.names != pbert.schema.names:
        raise SystemExit(f"[metag-pbert] schema drift:\n  3lane {three.schema.names}\n"
                         f"  pbert {pbert.schema.names}")
    print(f"[metag-pbert] 3-lane {three.num_rows:,} rows + pbert {pbert.num_rows:,} rows",
          flush=True)

    tbl = pa.concat_tables([three.cast(pbert.schema), pbert])
    del three, pbert
    key = ["source", "orf", "channel", "intermediate_id", "mnxr"]
    import pyarrow.compute as pc
    tbl = tbl.take(pc.sort_indices(tbl, sort_keys=[(k, "ascending") for k in key]))

    df = tbl.to_pandas()
    del tbl
    orf_ids = pd.read_csv(ORFS_CSV)["orf"].to_numpy()
    fe.validate_gpr(df, LANE_SET, orf_ids, SOURCE)

    tmp = GPR4.with_suffix(".partial")
    df.to_parquet(tmp, index=False)
    tmp.rename(GPR4)
    print(f"[metag-pbert] wrote {len(df):,} rows -> {GPR4}", flush=True)


def check_alignment():
    import numpy as np
    import pandas as pd
    ids = query_ids(pd)
    E = np.load(EMB, mmap_mode="r")
    o = pd.read_csv(ORFS_CSV)
    lens = o.set_index("orf")["length"]
    missing = [x for x in o["orf"].to_numpy() if x not in set(ids)]
    kept = np.array([x for x in o["orf"].to_numpy() if x not in set(missing)])
    print(f"[align] index {len(ids):,} rows, stack {len(E):,} rows, "
          f"{len(missing)} ORFs unembedded")
    print(f"[align] index is exact fasta order minus those: {bool((kept == ids).all())}")
    L = np.log(lens.reindex(ids).to_numpy().astype(float))
    DIM, B = 133, 16
    step = len(ids) // B
    rs = []
    for b in range(B):
        sel = np.arange(b * step, min(b * step + 4000, len(ids)))
        x = np.asarray(E[sel][:, DIM], dtype=np.float32)
        rs.append(float(np.corrcoef(x, L[sel])[0, 1]))
    print(f"[align] per-block corr(emb[:,{DIM}], log len): "
          f"min {min(rs):+.3f} max {max(rs):+.3f}")
    if max(rs) > -0.3:
        raise SystemExit("[align] at least one block does not carry the length "
                         "signal -- the stack is not in the index's order")
    print("[align] OK -- every block carries it; the pairing holds")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check-alignment", action="store_true")
    ap.add_argument("--lane", action="store_true", help="compute the pbert lane (slabbed)")
    ap.add_argument("--assemble", action="store_true", help="3 lanes + pbert -> gpr_4lane")
    a = ap.parse_args()
    if not any((a.check_alignment, a.lane, a.assemble)):
        ap.error("pick at least one of --check-alignment / --lane / --assemble")
    if a.check_alignment:
        check_alignment()
    if a.lane:
        run_lane()
    if a.assemble:
        assemble()
