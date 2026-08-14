"""chunkOrfsForAnnotation — length-balanced fan-out of a per-sample ORF FASTA
into ~CHUNK_SIZE-ORF chunks for CPU/IO-bound annotators (deepec, interproscan,
proteinbert, dram, kofamscan, eggnog, diamond_*, busco, predictf, deeptfactor).

Sibling of `shardFasta`: same length-balanced round-robin logic, but emits
`sequences::orf_chunk` (a distinct sibling of `sequences::orfs`, NOT a subtype)
so the solver routes chunks to annotators without entangling them with the
ML-embedder `orfs_shard` path (saprot/esmfold/ankh, 6000/shard).

CHUNK_SIZE=5000 ≈ E. coli proteome; bounds each annotator task at ~1.3 MB raw
FASTA and ~134-268 MB worst-case intermediate state.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::polars.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk = model.AddProduct(lib.GetType("sequences::orf_chunk"))

CHUNK_SIZE = 5000

CHUNKER = r'''
import argparse, math, os

p = argparse.ArgumentParser()
p.add_argument("--fasta", required=True)
p.add_argument("--out-dir", required=True)
p.add_argument("--chunk-size", type=int, default=5000)
a = p.parse_args()

seqs = []
sid, buf = None, []
with open(a.fasta) as f:
    for line in f:
        line = line.rstrip()
        if line.startswith(">"):
            if sid is not None:
                seqs.append((sid, "".join(buf)))
            sid = line[1:].split()[0]
            buf = []
        else:
            buf.append(line)
    if sid is not None:
        seqs.append((sid, "".join(buf)))

n = len(seqs)
n_chunks = max(1, math.ceil(n / a.chunk_size))
print(f"chunking {n} seqs into {n_chunks} chunks (target {a.chunk_size}/chunk)", flush=True)

seqs.sort(key=lambda x: -len(x[1]))
bins = [[] for _ in range(n_chunks)]
for i, item in enumerate(seqs):
    bins[i % n_chunks].append(item)

os.makedirs(a.out_dir, exist_ok=True)
for chunk_id, items in enumerate(bins):
    items.sort(key=lambda x: -len(x[1]))
    path = os.path.join(a.out_dir, f"chunk_{chunk_id:04d}.faa")
    with open(path, "w") as f:
        for sid_s, seq in items:
            f.write(f">{sid_s}\n{seq}\n")
    print(f"  chunk {chunk_id:04d}: {len(items)} seqs -> {path}", flush=True)
'''


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    chunk_size = CHUNK_SIZE

    import os, shutil
    staging = "_chunk_staging"
    os.makedirs(staging, exist_ok=True)
    script = "_chunk_fasta.py"
    with open(script, "w") as f:
        f.write(CHUNKER)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"python {script} --fasta {iorfs.container} "
            f"--out-dir {staging} --chunk-size {chunk_size}",
    )

    chunk_files = sorted(f for f in os.listdir(staging) if f.startswith("chunk_") and f.endswith(".faa"))
    manifest_entries = []
    for i, cf in enumerate(chunk_files):
        out_path = context.Output(chunk, i=i)
        shutil.move(os.path.join(staging, cf), out_path.local)
        manifest_entries.append({chunk: out_path.local})
    shutil.rmtree(staging, ignore_errors=True)

    return ExecutionResult(
        manifest=manifest_entries,
        success=all(e[chunk].exists() for e in manifest_entries),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
