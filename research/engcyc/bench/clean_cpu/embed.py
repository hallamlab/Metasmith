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
import sys
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


def parse_cpus(spec):
    cpus = []
    for part in spec.split(","):
        lo, _, hi = part.partition("-")
        cpus.extend(range(int(lo), int(hi or lo) + 1))
    return cpus


def worker(rank, items, calib_items, a, out):
    block = sorted(os.sched_getaffinity(0))
    if a.pin:
        block = block[rank * a.threads:(rank + 1) * a.threads]
        os.sched_setaffinity(0, block)
    import torch
    import esm

    torch.set_num_threads(a.threads)
    if rank == 0:
        cap = [l.strip() for l in torch.__config__.show().splitlines() if "CPU capability" in l]
        print(json.dumps({"torch": torch.__version__, "capability": cap,
                          "affinity_r0": sorted(os.sched_getaffinity(0))[:4] + ["..."]}), flush=True)
    if a.mode.startswith("ort_"):
        import contextlib
        import onnxruntime as ort
        alphabet = esm.data.Alphabet.from_architecture("ESM-1b")
        convert = alphabet.get_batch_converter()
        # OMP_PROC_BIND binds this (main) thread to one cpu when torch loads, and ORT's pool would
        # inherit that single cpu; give the thread its whole block back first.
        os.sched_setaffinity(0, block)
        so = ort.SessionOptions()
        so.intra_op_num_threads, so.inter_op_num_threads = a.threads, 1
        sess = ort.InferenceSession(f"{a.onnx_dir}/esm1b_{a.mode[4:]}.onnx", so, providers=["CPUExecutionProvider"])

        def model(toks, repr_layers):
            return {"representations": {33: torch.from_numpy(sess.run(["rep"], {"toks": toks.numpy()})[0])}}
        ctx = contextlib.nullcontext()
    else:
        model, alphabet = esm.pretrained.load_model_and_alphabet_local(a.model)
        model.eval()
        convert = alphabet.get_batch_converter()
    if a.mode.startswith("ort_"):
        pass
    elif a.device == "cuda":
        model = model.cuda()
        ctx = (torch.autocast("cuda", dtype=torch.bfloat16) if a.mode == "bf16"
               else torch.autocast("cuda", enabled=False))
        if rank == 0:
            print(json.dumps({"cuda_visible": os.environ.get("CUDA_VISIBLE_DEVICES"),
                              "device": torch.cuda.get_device_name(0),
                              "capability": torch.cuda.get_device_capability(0),
                              "arch_list": torch.cuda.get_arch_list()}), flush=True)
    else:
        import quant
        calib = [convert([(n, s[:TRUNC]) for n, s in calib_items[i:i + 8]])[2]
                 for i in range(0, len(calib_items), 8)]
        model, ctx = quant.prepare(model, a.mode, calib)
    if a.sdpa:
        # ESM asks attention for its averaged weights, which keeps torch off its fused
        # scaled_dot_product_attention kernel; CLEAN never reads them.
        from esm.multihead_attention import MultiheadAttention
        fwd = MultiheadAttention.forward

        def no_weights(self, *args, **kw):
            if not kw.get("need_head_weights"):
                kw["need_weights"] = False
            return fwd(self, *args, **kw)
        MultiheadAttention.forward = no_weights
    if a.ipex:
        import intel_extension_for_pytorch as ipex
        # ipex.optimize repacks Linear weights into a blocked layout that ESM's fused attention
        # call rejects; onnx_trace keeps attention on the per-projection path.
        for m in model.modules():
            if type(m).__name__ == "MultiheadAttention":
                m.onnx_trace = True
        model = ipex.optimize(model, dtype=torch.bfloat16 if a.mode == "bf16" else torch.float32)

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

    ids, embs, residues = [], [], 0
    t0 = time.time()
    with torch.inference_mode(), ctx:
        for b in batches:
            _, _, toks = convert([(n, s[:TRUNC]) for n, s in b])
            if a.device == "cuda":
                toks = toks.cuda(non_blocking=True)
            rep = model(toks, repr_layers=[33])["representations"][33].float().cpu()
            for i, (n, s) in enumerate(b):
                L = min(len(s), TRUNC)
                ids.append(n)
                embs.append(rep[i, 1:L + 1].mean(0).numpy())
                residues += L
    wall = time.time() - t0
    gpu_peak = round(torch.cuda.max_memory_allocated() / 1e9, 2) if a.device == "cuda" else None
    np.save(f"{out}.r{rank}.npy", np.stack(embs).astype(np.float32))
    json.dump({"ids": ids, "wall": wall, "orfs": len(ids), "residues": residues, "batches": len(batches), "gpu_peak_gb": gpu_peak}, open(f"{out}.r{rank}.json", "w"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fasta", required=True)
    p.add_argument("--n", type=int, default=0, help="sample this many ORFs; 0 takes them all")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--mode", default="fp32", help="see quant.py; on cuda only fp32 and bf16")
    p.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    p.add_argument("--onnx-dir", default="", help="ort_* modes: the directory ort_export.py wrote")
    p.add_argument("--procs", type=int, default=1)
    p.add_argument("--threads", type=int, default=48)
    p.add_argument("--toks", type=int, default=4096)
    p.add_argument("--out", required=True)
    p.add_argument("--cpuset", default="", help="restrict the run to these cpus, e.g. 0-47")
    p.add_argument("--pin", action="store_true", help="give each worker its own disjoint block of --threads cpus")
    p.add_argument("--no-save", action="store_true", help="keep only the timing json")
    p.add_argument("--model", default=MODEL, help="ESM-1b checkpoint, e.g. one written by slim_ckpt.py")
    p.add_argument("--sdpa", action="store_true", help="drop ESM's unused attention weights")
    p.add_argument("--ipex", action="store_true", help="ipex.optimize the model (needs an IPEX overlay)")
    a = p.parse_args()
    if a.cpuset:
        os.sched_setaffinity(0, parse_cpus(a.cpuset))
    if a.pin:
        assert a.procs * a.threads <= len(os.sched_getaffinity(0)), "not enough cpus to pin"

    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    items = list(read_fasta(a.fasta))
    if a.n and a.n < len(items):
        items = random.Random(a.seed).sample(items, a.n)
    calib_items = random.Random(a.seed + 1000).sample(items, 32) if a.mode == "sq_int8" else []
    shards = [items[i::a.procs] for i in range(a.procs)]
    t0 = time.time()
    ps = [mp.get_context("spawn").Process(target=worker, args=(r, s, calib_items, a, a.out)) for r, s in enumerate(shards)]
    for x in ps:
        x.start()
    for x in ps:
        x.join()
    total = time.time() - t0
    parts = [json.load(open(f"{a.out}.r{r}.json")) for r in range(a.procs)]
    ids = [i for q in parts for i in q["ids"]]
    emb = np.concatenate([np.load(f"{a.out}.r{r}.npy") for r in range(a.procs)])
    if not a.no_save:
        np.save(f"{a.out}.npy", emb)
    for r in range(a.procs):
        os.remove(f"{a.out}.r{r}.npy")
        os.remove(f"{a.out}.r{r}.json")
    orfs = sum(q["orfs"] for q in parts)
    compute = max(q["wall"] for q in parts)
    summary = {"mode": a.mode, "procs": a.procs, "threads": a.threads, "toks": a.toks, "orfs": orfs,
               "pin": a.pin, "cpuset": a.cpuset, "sdpa": a.sdpa, "ipex": a.ipex, "n_batches_r0": parts[0].get("batches"),
               "per_proc_wall": [round(q["wall"], 1) for q in parts], "device": a.device,
               "gpu_peak_gb": parts[0].get("gpu_peak_gb"),
               "residues": sum(q["residues"] for q in parts), "embed_wall_s": round(compute, 1),
               "total_wall_s": round(total, 1), "orf_per_s": round(orfs / compute, 3),
               "orf_per_s_per_core": round(orfs / compute / (a.procs * a.threads), 4), "ids": ids}
    json.dump(summary, open(f"{a.out}.json", "w"))
    print(json.dumps({k: v for k, v in summary.items() if k != "ids"}), flush=True)


if __name__ == "__main__":
    main()
