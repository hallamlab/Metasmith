import os
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::saprot.env"))
weights  = model.AddRequirement(lib.GetType("ref::saprot_650m_weights"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs_shard"))
tokens   = model.AddRequirement(lib.GetType("sequences::structure_3di_tokens"), parents={orfs})
out_emb  = model.AddProduct(lib.GetType("annotation::saprot_embeddings"))
out_idx  = model.AddProduct(lib.GetType("annotation::saprot_index"))


# SaProt consumes structure-aware sequences where each token is the AA letter
# concatenated with the lowercased 3Di letter (e.g., "Ad", "Rv"). We pair the
# ORF FASTA against the 3Di parquet by sequence_id; any ORF without a matching
# 3Di entry is skipped (logged to stderr).
#
# Per metasmith/dev guidance (msg #153): runtime knobs baked at module top.
# SaProt is ESM-2 based and trained at 1024 context. Sliding window chunks
# AA and 3Di streams in lockstep by residue position.
BATCH_SIZE   = 16
MAX_LEN      = 1024
CHUNK_OVERLAP = 128

INFERENCE = r'''
import argparse, sys, time
import numpy as np
import pandas as pd
import torch
from transformers import EsmTokenizer, EsmModel

p = argparse.ArgumentParser()
p.add_argument("--weights", required=True)
p.add_argument("--fasta", required=True)
p.add_argument("--di3", required=True)
p.add_argument("--out-parquet", required=True)
p.add_argument("--out-index", required=True)
p.add_argument("--device", default="cuda")
p.add_argument("--batch-size", type=int, default=16)
p.add_argument("--max-len", type=int, default=1024)
p.add_argument("--chunk-overlap", type=int, default=128)
a = p.parse_args()

device = torch.device(a.device if (a.device == "cpu" or torch.cuda.is_available()) else "cpu")
tok = EsmTokenizer.from_pretrained(a.weights)
mdl = EsmModel.from_pretrained(a.weights).to(device).eval()
if device.type == "cuda":
    mdl = mdl.to(dtype=torch.bfloat16)
print(f"loaded saprot on {device}; batch={a.batch_size}; max_len={a.max_len}; overlap={a.chunk_overlap}", flush=True)

di3_df = pd.read_parquet(a.di3)
di3_map = dict(zip(di3_df["sequence_id"], di3_df["di3_sequence"]))

# Parse FASTA, pair with 3Di. Keep aa+d3 as a (residue-indexed) pair so
# sliding-window chunking can split both streams identically by position.
ids, aas, d3s = [], [], []
with open(a.fasta) as f:
    sid, buf = None, []
    def flush():
        if sid is None:
            return
        aa = "".join(buf).upper()
        d3 = di3_map.get(sid)
        if d3 is None or len(d3) != len(aa):
            sys.stderr.write(f"skip {sid}: missing or length-mismatched 3Di\n")
            return
        ids.append(sid); aas.append(aa); d3s.append(d3)
    for line in f:
        line = line.rstrip()
        if line.startswith(">"):
            flush()
            sid = line[1:].split()[0]; buf = []
        else:
            buf.append(line)
    flush()
print(f"loaded {len(ids)} paired (aa, 3Di) sequences", flush=True)

def chunkify(aa, d3, cap, overlap):
    L = len(aa)
    if L <= cap:
        return [(aa, d3, L)]
    stride = cap - overlap
    out = []
    for start in range(0, L, stride):
        end = min(start + cap, L)
        weight = (end - start) if start == 0 else max(end - start - overlap, 1)
        out.append((aa[start:end], d3[start:end], weight))
        if end == L:
            break
    return out

flat = []  # (seq_idx, sa_string, weight)
n_long = 0
for si in range(len(ids)):
    parts = chunkify(aas[si], d3s[si], a.max_len, a.chunk_overlap)
    if len(parts) > 1: n_long += 1
    for aa_c, d3_c, w in parts:
        sa = "".join(x + y.lower() for x, y in zip(aa_c, d3_c))
        flat.append((si, sa, w))
print(f"  {n_long}/{len(ids)} sequences exceeded max_len; total chunks: {len(flat)}", flush=True)

t0 = time.time()
acc_vec = [None] * len(ids)
acc_w   = [0.0]  * len(ids)
with torch.no_grad():
    for i in range(0, len(flat), a.batch_size):
        batch_entries = flat[i:i+a.batch_size]
        batch = [s for _, s, _ in batch_entries]
        enc = tok(batch, padding="longest", return_tensors="pt", add_special_tokens=True)
        ids_t = enc.input_ids.to(device)
        mask  = enc.attention_mask.to(device)
        out = mdl(input_ids=ids_t, attention_mask=mask).last_hidden_state
        for k, (si, _, w) in enumerate(batch_entries):
            n = int(mask[k].sum().item()) - 1  # drop EOS
            vec = out[k, 1:n].mean(dim=0).float().cpu().numpy() * w  # skip BOS
            if acc_vec[si] is None: acc_vec[si] = vec
            else:                   acc_vec[si] = acc_vec[si] + vec
            acc_w[si] += w
        if (i // a.batch_size) % 10 == 0:
            elapsed = time.time() - t0
            done = i + len(batch)
            rate = done / elapsed if elapsed > 0 else 0
            print(f"  {done}/{len(flat)} chunks ({rate:.1f} chunk/s)", flush=True)

embeddings = [acc_vec[i] / acc_w[i] for i in range(len(ids))]
emb = np.vstack(embeddings) if embeddings else np.zeros((0, 0))
cols = [f"dim_{i}" for i in range(emb.shape[1])]
pd.DataFrame(emb, columns=cols).to_parquet(a.out_parquet, index=False)
pd.DataFrame({"sequence_id": ids, "index": list(range(len(ids)))}).to_csv(a.out_index, index=False)
print(f"done: {len(embeddings)} embeddings in {time.time()-t0:.1f}s", flush=True)
'''


def protocol(context: ExecutionContext):
    iorfs    = context.Input(orfs)
    iw       = context.Input(weights)
    itok     = context.Input(tokens)
    iemb     = context.Output(out_emb)
    iidx     = context.Output(out_idx)

    device = "cuda"   # inference script falls back to cpu via torch.cuda.is_available()

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")

    script = Path("infer_saprot.py")
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
                --di3 {itok.container} \
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


# NOTE: GPU via context.params["gpus"]; Resources has no gpus= field yet.
TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    ),
)
