import os
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::prott5.env"))
weights  = model.AddRequirement(lib.GetType("ref::prott5_xl_uniref50_weights"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs_shard"))
out_emb  = model.AddProduct(lib.GetType("annotation::prott5_embeddings"))
out_idx  = model.AddProduct(lib.GetType("annotation::prott5_index"))


# Per metasmith/dev guidance (msg #153): runtime knobs baked at module top.
# ProtT5-XL uses T5 relative position bias which extrapolates well; cap at
# 4096 (ProtTrans paper precedent) drops only 27/1.44M metag ORFs.
# Sliding-window aggregation covers any seq longer than the cap.
BATCH_SIZE   = 4
MAX_LEN      = 4096
CHUNK_OVERLAP = 128

INFERENCE = r'''
import argparse, sys, re, time
import numpy as np
import pandas as pd
import torch
from transformers import T5EncoderModel, T5Tokenizer

p = argparse.ArgumentParser()
p.add_argument("--weights", required=True)
p.add_argument("--fasta", required=True)
p.add_argument("--out-parquet", required=True)
p.add_argument("--out-index", required=True)
p.add_argument("--device", default="cuda")
p.add_argument("--batch-size", type=int, default=4)
p.add_argument("--max-len", type=int, default=4096)
p.add_argument("--chunk-overlap", type=int, default=128)
a = p.parse_args()

device = torch.device(a.device if (a.device == "cpu" or torch.cuda.is_available()) else "cpu")
tok = T5Tokenizer.from_pretrained(a.weights, do_lower_case=False)
mdl = T5EncoderModel.from_pretrained(a.weights).to(device).eval()
if device.type == "cuda":
    mdl = mdl.to(dtype=torch.bfloat16)
print(f"loaded prott5 on {device}; batch={a.batch_size}; max_len={a.max_len}; overlap={a.chunk_overlap}", flush=True)

ids, seqs = [], []
with open(a.fasta) as f:
    sid, buf = None, []
    for line in f:
        line = line.rstrip()
        if line.startswith(">"):
            if sid is not None:
                ids.append(sid); seqs.append("".join(buf))
            sid = line[1:].split()[0]; buf = []
        else:
            buf.append(line)
    if sid is not None:
        ids.append(sid); seqs.append("".join(buf))
print(f"loaded {len(seqs)} sequences", flush=True)

# ProtT5 expects whitespace-separated residues and U/Z/O/B mapped to X.
# Chunk on the raw residue string (so cap is residue-count, not whitespace-
# expanded length); apply prep() per chunk before tokenization.
def prep(s):
    s = re.sub(r"[UZOB]", "X", s.upper())
    return " ".join(list(s))

def chunkify(seq, cap, overlap):
    if len(seq) <= cap:
        return [(seq, len(seq))]
    stride = cap - overlap
    out = []
    for start in range(0, len(seq), stride):
        end = min(start + cap, len(seq))
        weight = (end - start) if start == 0 else max(end - start - overlap, 1)
        out.append((seq[start:end], weight))
        if end == len(seq):
            break
    return out

flat = []
n_long = 0
for si, s in enumerate(seqs):
    parts = chunkify(s, a.max_len, a.chunk_overlap)
    if len(parts) > 1: n_long += 1
    for chunk, w in parts:
        flat.append((si, chunk, w))
print(f"  {n_long}/{len(seqs)} sequences exceeded max_len; total chunks: {len(flat)}", flush=True)

t0 = time.time()
acc_vec = [None] * len(seqs)
acc_w   = [0.0]  * len(seqs)
with torch.no_grad():
    for i in range(0, len(flat), a.batch_size):
        batch_entries = flat[i:i+a.batch_size]
        batch = [prep(c) for _, c, _ in batch_entries]
        enc = tok.batch_encode_plus(batch, add_special_tokens=True, padding="longest", return_tensors="pt")
        ids_t = enc.input_ids.to(device)
        mask  = enc.attention_mask.to(device)
        out = mdl(input_ids=ids_t, attention_mask=mask).last_hidden_state
        for k, (si, _, w) in enumerate(batch_entries):
            n = int(mask[k].sum().item()) - 1
            vec = out[k, :n].mean(dim=0).float().cpu().numpy() * w
            if acc_vec[si] is None: acc_vec[si] = vec
            else:                   acc_vec[si] = acc_vec[si] + vec
            acc_w[si] += w
        if (i // a.batch_size) % 10 == 0:
            elapsed = time.time() - t0
            done = i + len(batch)
            rate = done / elapsed if elapsed > 0 else 0
            print(f"  {done}/{len(flat)} chunks ({rate:.1f} chunk/s)", flush=True)

embeddings = [acc_vec[i] / acc_w[i] for i in range(len(seqs))]
emb = np.vstack(embeddings)
cols = [f"dim_{i}" for i in range(emb.shape[1])]
pd.DataFrame(emb, columns=cols).to_parquet(a.out_parquet, index=False)
pd.DataFrame({"sequence_id": ids, "index": list(range(len(ids)))}).to_csv(a.out_index, index=False)
print(f"done: {len(embeddings)} embeddings in {time.time()-t0:.1f}s", flush=True)
'''


def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    iw      = context.Input(weights)
    iemb    = context.Output(out_emb)
    iidx    = context.Output(out_idx)

    device = "cuda"   # inference script falls back to cpu via torch.cuda.is_available()

    # Extract weights tarball locally so we can bind the directory into the container
    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")

    script = Path("infer_prott5.py")
    with open(script, "w") as f:
        f.write(INFERENCE)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"weights", "/weights"),
            (context.external_cwd/script.name, f"/work/{script.name}"),
        ],
        args=["--nv", "--env", f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES','')}"],
        cmd=f"""
            python /work/{script.name} \
                --weights /weights \
                --fasta {iorfs.container} \
                --out-parquet {iemb.container} \
                --out-index {iidx.container} \
                --device {device} \
                --batch-size {BATCH_SIZE} \
                --max-len {MAX_LEN} \
                --chunk-overlap {CHUNK_OVERLAP}
        """,
    )

    return ExecutionResult(
        manifest=[{out_emb: iemb.local, out_idx: iidx.local}],
        success=iemb.local.exists() and iidx.local.exists(),
    )


# NOTE: GPU execution is requested via context.params["gpus"]; the
# Resources(...) API has no gpus= field yet (see plan follow-ups for
# Nextflow/SLURM GPU plumbing).
TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(24),
        duration=Duration(hours=3),
    ),
)
