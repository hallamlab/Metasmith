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
p.add_argument("--out-layers", required=True)
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
out_layers = os.path.abspath(a.out_layers)
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
# EZpred's own fasta2plm.py takes `hidden_states.mean(axis=2)` then indexes
# `[layer-1]` for each of layers 34/35/36 -- so layer L is block L-1 of the stack,
# and the tensor is the RAW per-block output, not the normalised `embeddings` the
# forward also returns. Accumulated with the same length weights as the mean vector
# so a chunked long sequence is treated identically in both products.
REPR_LAYERS = [34, 35, 36]
acc_lay = [None] * len(seqs)
with torch.no_grad():
    for i in range(0, len(flat), a.batch_size):
        batch_entries = flat[i:i+a.batch_size]
        batch = [c for _, c, _ in batch_entries]
        enc = tok(batch, return_tensors="pt", padding=True, add_special_tokens=True)
        ids_t = enc.input_ids.to(device)
        mask  = (ids_t != pad_id)
        out = client(sequence_tokens=ids_t, sequence_id=mask)
        if out.hidden_states is None:
            raise SystemExit(
                "[esm_c] the forward returned no hidden_states, so the per-layer "
                "means EZpred needs cannot be built. Its heads take layers 34/35/36 "
                "and there is no way to recover them from `embeddings` alone.")
        # (n_layers, batch, seq, dim) -> (n_layers, batch, dim), mean over the
        # sequence axis exactly as EZpred does before selecting its layers.
        hs_mean = out.hidden_states.float().mean(axis=2).cpu().numpy()
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
            lay = np.stack([hs_mean[L - 1, k] for L in REPR_LAYERS]) * w
            if acc_lay[si] is None: acc_lay[si] = lay
            else:                   acc_lay[si] = acc_lay[si] + lay
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
layers = np.stack([acc_lay[i] / acc_w[i] for i in range(len(seqs))]).astype(np.float32)
# Written through an open handle, not by path. `np.save(path, ...)` appends `.npy`
# to any name that does not already end in it, so passing the product path directly
# writes the array BESIDE the file the step declared -- the step then reports its
# product missing while the bytes sit next to it. A handle takes the name as given.
with open(out_layers, "wb") as _fh:
    np.save(_fh, layers)
print(f"layer means {layers.shape} (layers {REPR_LAYERS}) -> {out_layers}", flush=True)
print(f"done: {len(embeddings)} embeddings in {time.time()-t0:.1f}s", flush=True)
