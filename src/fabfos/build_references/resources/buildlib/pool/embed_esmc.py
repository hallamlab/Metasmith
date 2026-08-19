import argparse, os, time
import numpy as np
import pandas as pd
import torch

p = argparse.ArgumentParser()
p.add_argument("--weights", required=True, help="dir containing data/weights/<file>.pth")
p.add_argument("--model-name", default="esmc_600m")
p.add_argument("--fasta", required=True)
p.add_argument("--out-npy", required=True)
p.add_argument("--out-index", required=True)
p.add_argument("--batch-size", type=int, default=32)
p.add_argument("--max-len", type=int, default=2048)
p.add_argument("--chunk-overlap", type=int, default=128)
a = p.parse_args()

# The ESM SDK's from_pretrained takes registered model NAMES, not paths, and resolves
# data/weights/<file>.pth relative to the process cwd -- hence INFRA_PROVIDER=local
# plus a chdir into the unpacked bundle.
os.environ["INFRA_PROVIDER"] = "local"
weights_dir = os.path.abspath(a.weights)
fasta = os.path.abspath(a.fasta)
out_npy = os.path.abspath(a.out_npy)
out_index = os.path.abspath(a.out_index)
os.chdir(weights_dir)

from esm.models.esmc import ESMC

if not torch.cuda.is_available():
    raise SystemExit(
        "[pool-esmc] torch sees no GPU. 222k sequences through a 600M model on CPU is "
        "not a slower run, it is a run that does not finish inside any walltime -- and "
        "a pool that is a subset of what it claims misindexes nothing but answers "
        "fewer queries, silently. Resources(gpus=REQUIRED) is what allocates one.")
device = "cuda"
client = ESMC.from_pretrained(a.model_name).to(device).eval().to(dtype=torch.bfloat16)
tok = client.tokenizer
pad_id = tok.pad_token_id
print(f"[pool-esmc] loaded {a.model_name} on {device}; batch_size={a.batch_size}; "
      f"max_len={a.max_len}; overlap={a.chunk_overlap}", flush=True)

ids, seqs = [], []
with open(fasta) as f:
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
print(f"[pool-esmc] {len(seqs):,} sequences", flush=True)
if not seqs:
    raise SystemExit("[pool-esmc] the pool FASTA is empty")


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
    if len(parts) > 1:
        n_long += 1
    for chunk, w in parts:
        flat.append((si, chunk, w))
print(f"[pool-esmc] {n_long:,}/{len(seqs):,} sequences exceeded max_len; "
      f"{len(flat):,} chunks", flush=True)

t0 = time.time()
acc_vec = [None] * len(seqs)
acc_w = [0.0] * len(seqs)
with torch.no_grad():
    for i in range(0, len(flat), a.batch_size):
        batch_entries = flat[i:i+a.batch_size]
        batch = [c for _, c, _ in batch_entries]
        enc = tok(batch, return_tensors="pt", padding=True, add_special_tokens=True)
        ids_t = enc.input_ids.to(device)
        mask = (ids_t != pad_id)
        out = client(sequence_tokens=ids_t, sequence_id=mask)
        for k, (si, _, w) in enumerate(batch_entries):
            idxs = mask[k].nonzero(as_tuple=True)[0]
            if len(idxs) <= 2:
                vec = out.embeddings[k, idxs].mean(dim=0)
            else:
                vec = out.embeddings[k, idxs[1:-1]].mean(dim=0)
            vec = vec.float().cpu().numpy() * w
            if acc_vec[si] is None:
                acc_vec[si] = vec
            else:
                acc_vec[si] = acc_vec[si] + vec
            acc_w[si] += w
        if (i // a.batch_size) % 200 == 0:
            done = i + len(batch)
            elapsed = time.time() - t0
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(flat) - done) / rate if rate > 0 else 0
            print(f"[pool-esmc]   {done:,}/{len(flat):,} chunks "
                  f"({rate:.1f} chunk/s, ETA {eta/60:.1f} min)", flush=True)

emb = np.vstack([acc_vec[i] / acc_w[i] for i in range(len(seqs))]).astype(np.float32)
np.save(out_npy, emb)
pd.DataFrame({"sequence_id": ids, "index": list(range(len(ids)))}).to_csv(
    out_index, index=False)
print(f"[pool-esmc] {emb.shape} -> {out_npy} in {time.time()-t0:.1f}s", flush=True)
