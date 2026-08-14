"""shardFasta — length-balanced fan-out of a protein FASTA into N shards.

Each shard targets ~SHARD_SIZE sequences. Sequences are length-sorted then
distributed in a round-robin stripe across N shards. The longest seq goes
to shard 0, the second-longest to shard 1, … so every shard ends up with
the same length-mix instead of one shard owning the long tail (which was
the qKsXpL4y OOM failure mode).

Per-sequence OOM (a single seq too long for the slice) is *not* solved by
striping — it's bounded downstream by each embedding transform's
--max-len + sliding-window chunking.

Output: N instances of `sequences::orfs_shard`. Embedding transforms that
consume `sequences::orfs_shard` (instead of `sequences::orfs`) will fan
out across these N instances automatically.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::polars.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
shard = model.AddProduct(lib.GetType("sequences::orfs_shard"))

# Per-shard target. Sized so esmfold (~28 ORFs/min on gpu_med) finishes one
# shard within the 5h SLURM walltime: 6000 ORFs / 28 ≈ 3.6h, leaves headroom
# for model load + tar stage-back. 1.44M metag ORFs → ~241 shards.
# Per metasmith/dev guidance (msg #153): edit this constant per-run instead
# of routing values through context.params (the user_params channel was
# reverted on dev).
SHARD_SIZE = 6000

SHARDER = r'''
import argparse, math, os, sys

p = argparse.ArgumentParser()
p.add_argument("--fasta", required=True)
p.add_argument("--out-dir", required=True)
p.add_argument("--shard-size", type=int, default=1024)
a = p.parse_args()

# Read all sequences (id, full_sequence_string)
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
n_shards = max(1, math.ceil(n / a.shard_size))
print(f"sharding {n} seqs into {n_shards} shards (target {a.shard_size}/shard)", flush=True)

# Sort longest-first, then round-robin distribute. Shard k receives seqs
# at sorted positions k, k+N, k+2N, ... so each shard's max length is
# seqs[k] — the per-shard max-length differential across shards is tiny
# (seqs[0] - seqs[N-1]), and no single shard owns the long tail.
seqs.sort(key=lambda x: -len(x[1]))
bins = [[] for _ in range(n_shards)]
for i, item in enumerate(seqs):
    bins[i % n_shards].append(item)

os.makedirs(a.out_dir, exist_ok=True)
shard_totals = []
for shard_id, items in enumerate(bins):
    # Within-shard length-desc keeps padding tight inside each batch
    items.sort(key=lambda x: -len(x[1]))
    total = sum(len(s) for _, s in items)
    max_len = len(items[0][1]) if items else 0
    path = os.path.join(a.out_dir, f"shard_{shard_id:04d}.faa")
    with open(path, "w") as f:
        for sid_s, seq in items:
            f.write(f">{sid_s}\n{seq}\n")
    shard_totals.append((shard_id, len(items), total, max_len, path))
    print(f"  shard {shard_id:04d}: {len(items)} seqs, {total} aa, max_len={max_len} -> {path}", flush=True)

with open(os.path.join(a.out_dir, "_shard_index.tsv"), "w") as f:
    f.write("shard_id\tn_seqs\ttotal_aa\tmax_len\tpath\n")
    for shard_id, n_items, total, max_len, path in shard_totals:
        f.write(f"{shard_id}\t{n_items}\t{total}\t{max_len}\t{os.path.basename(path)}\n")
'''


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    shard_size = SHARD_SIZE

    # Run the sharder locally first so we can discover how many shards we'll
    # produce (the count depends on the input FASTA length and shard_size).
    # We write to a staging dir, then materialize each shard at the
    # `context.Output(shard, i)` path the planner generated.
    import os, shutil
    staging = "_shard_staging"
    os.makedirs(staging, exist_ok=True)
    script = "_shard_fasta.py"
    with open(script, "w") as f:
        f.write(SHARDER)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"python {script} --fasta {iorfs.container} "
            f"--out-dir {staging} --shard-size {shard_size}",
    )

    shard_files = sorted(f for f in os.listdir(staging) if f.startswith("shard_") and f.endswith(".faa"))
    manifest_entries = []
    for i, sf in enumerate(shard_files):
        out_path = context.Output(shard, i=i)
        shutil.move(os.path.join(staging, sf), out_path.local)
        manifest_entries.append({shard: out_path.local})
    shutil.rmtree(staging, ignore_errors=True)

    return ExecutionResult(
        manifest=manifest_entries,
        success=all(e[shard].exists() for e in manifest_entries),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
