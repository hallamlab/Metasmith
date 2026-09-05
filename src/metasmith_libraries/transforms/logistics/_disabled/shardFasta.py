from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::polars.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
shard = model.AddProduct(lib.GetType("sequences::orfs_shard"))

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

    import os, shutil
    staging = "_shard_staging"
    os.makedirs(staging, exist_ok=True)
    script = "_shard_fasta.py"
    with open(script, "w") as f:
        f.write(SHARDER)

    context.ExecWithEnv(
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
