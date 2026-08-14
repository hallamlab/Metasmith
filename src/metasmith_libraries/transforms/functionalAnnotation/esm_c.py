import os
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::esmc.env"))
weights  = model.AddRequirement(lib.GetType("ref::esm_c_300m_weights"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs_shard"))
out_emb  = model.AddProduct(lib.GetType("annotation::esm_c_embeddings"))
out_idx  = model.AddProduct(lib.GetType("annotation::esm_c_index"))


# Module-level constants per metasmith/dev guidance (msg #153): the
# user_params runtime override channel was reverted, so all tuning lives
# here. ESM-C 300M trained at 2048 context; sliding-window aggregation
# handles longer ORFs (~380/1.44M for metag) via length-weighted mean.
BATCH_SIZE   = 32
MAX_LEN      = 2048
CHUNK_OVERLAP = 128

# Uses the EvolutionaryScale `esm` SDK (>=3.x). ESMC.from_pretrained only
# accepts registered model names ("esmc_300m" / "esmc_600m"), not paths.
# To use local weights we set INFRA_PROVIDER=local (makes data_root() return
# Path("")) and chdir into the weights bundle, so the relative path
# data/weights/<file>.pth resolves locally.
INFERENCE = r'''
import argparse, os, sys, time
import numpy as np
import pandas as pd
import torch

p = argparse.ArgumentParser()
p.add_argument("--weights", required=True, help="dir containing data/weights/<file>.pth")
p.add_argument("--model-name", default="esmc_300m", choices=["esmc_300m", "esmc_600m"])
p.add_argument("--fasta", required=True)
p.add_argument("--out-parquet", required=True)
p.add_argument("--out-index", required=True)
p.add_argument("--device", default="cuda")
p.add_argument("--batch-size", type=int, default=32)
p.add_argument("--max-len", type=int, default=2048)
p.add_argument("--chunk-overlap", type=int, default=128)
a = p.parse_args()

os.environ["INFRA_PROVIDER"] = "local"
weights_dir = os.path.abspath(a.weights)
fasta = os.path.abspath(a.fasta)
out_parquet = os.path.abspath(a.out_parquet)
out_index = os.path.abspath(a.out_index)
os.chdir(weights_dir)

from esm.models.esmc import ESMC

device = a.device if (a.device == "cpu" or torch.cuda.is_available()) else "cpu"
client = ESMC.from_pretrained(a.model_name).to(device).eval()
if device == "cuda":
    client = client.to(dtype=torch.bfloat16)
tok = client.tokenizer
pad_id = tok.pad_token_id
print(f"loaded {a.model_name} on {device}; batch_size={a.batch_size}; max_len={a.max_len}; overlap={a.chunk_overlap}; pad_id={pad_id}", flush=True)

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

# Pre-chunk long sequences. Each input ORF maps to >=1 (chunk, weight)
# entries; weight is the non-overlapping AA span this chunk uniquely
# represents, used as the aggregator weight. Short ORFs pass through
# as a single chunk with weight=len.
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

flat = []  # (seq_idx, chunk_str, weight)
n_long = 0
for si, s in enumerate(seqs):
    parts = chunkify(s, a.max_len, a.chunk_overlap)
    if len(parts) > 1: n_long += 1
    for chunk, w in parts:
        flat.append((si, chunk, w))
print(f"  {n_long}/{len(seqs)} sequences exceeded max_len ({a.max_len}); total chunks: {len(flat)}", flush=True)

t0 = time.time()
# Per-ORF accumulator: weighted sum of chunk-mean embeddings, plus total weight
acc_vec = [None] * len(seqs)
acc_w   = [0.0]  * len(seqs)
with torch.no_grad():
    for i in range(0, len(flat), a.batch_size):
        batch_entries = flat[i:i+a.batch_size]
        batch = [c for _, c, _ in batch_entries]
        enc = tok(batch, return_tensors="pt", padding=True, add_special_tokens=True)
        ids_t = enc.input_ids.to(device)
        mask  = (ids_t != pad_id)
        out = client(sequence_tokens=ids_t, sequence_id=mask)
        for k, (si, _, w) in enumerate(batch_entries):
            valid = mask[k]
            idxs = valid.nonzero(as_tuple=True)[0]
            if len(idxs) <= 2:
                vec = out.embeddings[k, idxs].mean(dim=0)
            else:
                vec = out.embeddings[k, idxs[1:-1]].mean(dim=0)
            vec = vec.float().cpu().numpy() * w
            if acc_vec[si] is None: acc_vec[si] = vec
            else:                   acc_vec[si] = acc_vec[si] + vec
            acc_w[si] += w
        if (i // a.batch_size) % 10 == 0:
            elapsed = time.time() - t0
            done = i + len(batch)
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(flat) - done) / rate if rate > 0 else 0
            print(f"  {done}/{len(flat)} chunks ({rate:.1f} chunk/s, ETA {eta:.0f}s)", flush=True)

embeddings = [acc_vec[i] / acc_w[i] for i in range(len(seqs))]
emb = np.vstack(embeddings)
cols = [f"dim_{i}" for i in range(emb.shape[1])]
pd.DataFrame(emb, columns=cols).to_parquet(out_parquet, index=False)
pd.DataFrame({"sequence_id": ids, "index": list(range(len(ids)))}).to_csv(out_index, index=False)
print(f"done: {len(embeddings)} embeddings in {time.time()-t0:.1f}s", flush=True)
'''


def protocol(context: ExecutionContext):
    iorfs   = context.Input(orfs)
    iw      = context.Input(weights)
    iemb    = context.Output(out_emb)
    iidx    = context.Output(out_idx)

    # Constants baked at module top (BATCH_SIZE, MAX_LEN, CHUNK_OVERLAP);
    # the inference script auto-downgrades to CPU if torch doesn't see a GPU.
    device = "cuda"

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")

    script = Path("infer_esmc.py")
    with open(script, "w") as f:
        f.write(INFERENCE)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"weights", "/weights"),
            (context.external_cwd/script.name, f"/work/{script.name}"),
        ],
        # MIG cotenancy: the relay's bash subprocess loses SLURM's CUDA_VISIBLE_DEVICES,
        # so `$VAR` won't expand. Read it from os.environ (the metasmith container
        # has it set via the nextflow beforeScript APPTAINERENV_CUDA_VISIBLE_DEVICES)
        # and embed the literal MIG UUID so apptainer binds the right slice.
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
