"""Time ESM-1b mean embeddings on CPU, the step that makes CLEAN a GPU transform.

Runs inside the external_clean image. Each of --procs workers embeds its share of the sampled ORFs
with --threads torch threads, length-sorted into token-budget batches as fair-esm's extract.py does,
and writes the layer-33 mean embeddings CLEAN reads.
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import time

import numpy as np

ALPHA = set("ACDEFGHIKLMNPQRSTVWYXBUZO")
MODEL = "/opt/torch_cache/hub/checkpoints/esm1b_t33_650M_UR50S.pt"
TRUNC = 1022


def read_fasta(path):
    name, seq = None, []
    for line in open(path):
        if line.startswith(">"):
            if name is not None:
                yield name, "".join(seq)
            name, seq = line[1:].split()[0], []
        else:
            seq.append("".join(c if c in ALPHA else "X" for c in line.strip().upper() if c != "*"))
    if name is not None:
        yield name, "".join(seq)


def worker(rank, items, a, out):
    import torch
    import esm

    torch.set_num_threads(a.threads)
    model, alphabet = esm.pretrained.load_model_and_alphabet_local(MODEL)
    model.eval()
    if a.mode == "int8":
        model = torch.ao.quantization.quantize_dynamic(model, {torch.nn.Linear}, dtype=torch.qint8)
    convert = alphabet.get_batch_converter()

    items = sorted(items, key=lambda x: len(x[1]))
    batches, cur, longest = [], [], 0
    for it in items:
        L = min(len(it[1]), TRUNC) + 2
        if cur and max(longest, L) * (len(cur) + 1) > a.toks:
            batches.append(cur)
            cur, longest = [], 0
        cur.append(it)
        longest = max(longest, L)
    if cur:
        batches.append(cur)

    ctx = torch.autocast("cpu", dtype=torch.bfloat16) if a.mode == "bf16" else torch.autocast("cpu", enabled=False)
    ids, embs, residues = [], [], 0
    t0 = time.time()
    with torch.inference_mode(), ctx:
        for b in batches:
            _, _, toks = convert([(n, s[:TRUNC]) for n, s in b])
            rep = model(toks, repr_layers=[33])["representations"][33].float()
            for i, (n, s) in enumerate(b):
                L = min(len(s), TRUNC)
                ids.append(n)
                embs.append(rep[i, 1:L + 1].mean(0).numpy())
                residues += L
    wall = time.time() - t0
    np.save(f"{out}.r{rank}.npy", np.stack(embs).astype(np.float32))
    json.dump({"ids": ids, "wall": wall, "orfs": len(ids), "residues": residues}, open(f"{out}.r{rank}.json", "w"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fasta", required=True)
    p.add_argument("--n", type=int, default=0, help="sample this many ORFs; 0 takes them all")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--mode", choices=["fp32", "bf16", "int8"], default="fp32")
    p.add_argument("--procs", type=int, default=1)
    p.add_argument("--threads", type=int, default=48)
    p.add_argument("--toks", type=int, default=4096)
    p.add_argument("--out", required=True)
    a = p.parse_args()

    items = list(read_fasta(a.fasta))
    if a.n and a.n < len(items):
        items = random.Random(a.seed).sample(items, a.n)
    shards = [items[i::a.procs] for i in range(a.procs)]
    t0 = time.time()
    ps = [mp.get_context("spawn").Process(target=worker, args=(r, s, a, a.out)) for r, s in enumerate(shards)]
    for x in ps:
        x.start()
    for x in ps:
        x.join()
    total = time.time() - t0
    parts = [json.load(open(f"{a.out}.r{r}.json")) for r in range(a.procs)]
    ids = [i for q in parts for i in q["ids"]]
    emb = np.concatenate([np.load(f"{a.out}.r{r}.npy") for r in range(a.procs)])
    np.save(f"{a.out}.npy", emb)
    for r in range(a.procs):
        os.remove(f"{a.out}.r{r}.npy")
        os.remove(f"{a.out}.r{r}.json")
    orfs = sum(q["orfs"] for q in parts)
    compute = max(q["wall"] for q in parts)
    summary = {"mode": a.mode, "procs": a.procs, "threads": a.threads, "toks": a.toks, "orfs": orfs,
               "residues": sum(q["residues"] for q in parts), "embed_wall_s": round(compute, 1),
               "total_wall_s": round(total, 1), "orf_per_s": round(orfs / compute, 3),
               "orf_per_s_per_core": round(orfs / compute / (a.procs * a.threads), 4), "ids": ids}
    json.dump(summary, open(f"{a.out}.json", "w"))
    print(json.dumps({k: v for k, v in summary.items() if k != "ids"}), flush=True)


if __name__ == "__main__":
    main()
