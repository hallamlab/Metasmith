#!/usr/bin/env python3
import os, re, sys, csv, json, glob, subprocess

MSM  = "/scratch/phyberos/gmcf3495/metasmith/runs"
INV  = "/scratch/phyberos/gmcf3495/inv"
OUT  = "/scratch/phyberos/gmcf3495/gapfill/pbert"
SIF  = "/scratch/phyberos/cache/apptainer/docker..quay.io_hallamlab_polars..1.38.1.sif"
EMB, IDX = "jnwshMUg", "spk7Jvqs"

ORPHAN_OWNER = {
    "1-1-1.P98wZ9iESa7lA4K3": "S19",
    "1-1-1.VQ9KctJvVVCIbbCj": "S12",
    "1-1-1.e1Mnniv6RhD9Eubs": "S12",
    "1-1-1.kaZZZgCj2ykxgCSQ": "S25",
    "1-1-1.5djZXcu5ev4G9yid": "S13",
    "1-1-1.DXhUt7iN2ukBuvwF": "S13",
    "1-1-1.OIa4dDob8T3K3BtA": "S13",
    "1-1-1.pM1lOM0HfsPcPSjA": "S13",
}

MERGER = r'''
import sys, json
import polars as pl
from pathlib import Path
manifest = json.loads(Path(sys.argv[1]).read_text())
out_emb, out_idx = Path(sys.argv[2]), Path(sys.argv[3])
emb_frames, idx_frames, row_offset = [], [], 0
for emb_path, idx_path in manifest:
    emb = pl.read_parquet(emb_path)
    idx = pl.read_csv(idx_path)
    n = emb.height
    if idx.height != n:
        print(f"WARN: idx rows {idx.height} != emb rows {n} for {emb_path}", flush=True)
    idx = idx.with_columns((pl.arange(0, idx.height) + row_offset).alias("global_row"))
    row_offset += n
    emb_frames.append(emb); idx_frames.append(idx)
pl.concat(emb_frames, how="vertical").write_parquet(out_emb)
pl.concat(idx_frames, how="vertical_relaxed").write_csv(out_idx)
print(f"merged {len(manifest)} chunk pair(s) -> {row_offset} rows", flush=True)
'''

def chunk_paths(key):
    m = {}
    for p in glob.glob(f"{MSM}/{key}/results/*proteinbert_*_chunk/*"):
        m[os.path.basename(p)] = p
    return m


def merges(key):
    out = []
    with open(f"{INV}/attrib_{key}.tsv") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["transform"] == "merge_proteinbert":
                out.append((r["sample"], r["task_dir"]))
    return sorted(set(out))


def work_paths(key):
    m = {}
    for _, td in merges(key):
        sh = open(f"{td}/.command.sh", errors="replace").read()
        for full in re.findall(
                r'[\w./\-]*nxf_work/[\w/]+/[\w.\-]+-(?:%s|%s)\.(?:parquet|csv)'
                % (EMB, IDX), sh):
            m[os.path.basename(full)] = full[full.index("nxf_work/"):]
    survivors = f"{INV}/pbert_workpaths_{key}.txt"
    if os.path.exists(survivors):
        for line in open(survivors):
            full = line.strip()
            if full:
                m.setdefault(os.path.basename(full), full[full.index("nxf_work/"):])
    return m


def main(targets):
    owners = json.load(open(f"{INV}/pbert_chunk_owner.json"))
    os.makedirs(OUT, exist_ok=True)
    for key, info in owners.items():
        paths = chunk_paths(key)
        order = work_paths(key)
        by_sample = {}
        for base, sample in info["owner"].items():
            by_sample.setdefault(sample, set()).add(base)
        for base in info["orphans"]:
            stem = base.rsplit("-", 1)[0]
            s = ORPHAN_OWNER.get(stem)
            if s is None:
                raise SystemExit(f"unassigned orphan {base}; refusing to guess")
            by_sample.setdefault(s, set()).add(base)

        for sample in sorted(by_sample):
            if targets and sample not in targets:
                continue
            missing = sorted(b for b in by_sample[sample] if b not in order)
            if missing:
                raise SystemExit(f"{sample}: no work dir recorded for {missing}")
            bases = sorted(by_sample[sample], key=lambda b: order[b])
            embs = [b for b in bases if f"-{EMB}." in b]
            idxs = [b for b in bases if f"-{IDX}." in b]
            if len(embs) != len(idxs):
                raise SystemExit(f"{sample}: {len(embs)} emb vs {len(idxs)} idx chunks")
            manifest = [[paths[e], paths[i]] for e, i in zip(embs, idxs)]
            mf = f"{OUT}/{sample}.manifest.json"
            open(mf, "w").write(json.dumps(manifest))
            open(f"{OUT}/_merge.py", "w").write(MERGER)
            print(f"== {sample} ({key}): {len(manifest)} chunk pairs", flush=True)
            subprocess.run([
                "apptainer", "exec", "--cleanenv", "--no-home",
                "--bind", "/scratch/phyberos", "--pwd", OUT, SIF,
                "python", f"{OUT}/_merge.py", mf,
                f"{OUT}/{sample}.parquet", f"{OUT}/{sample}.csv",
            ], check=True)

if __name__ == "__main__":
    main(set(sys.argv[1:]))
