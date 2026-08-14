import os
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::esmfold.env"))
weights   = model.AddRequirement(lib.GetType("ref::esmfold_weights"))
orfs      = model.AddRequirement(lib.GetType("sequences::orfs_shard"))
out_struct = model.AddProduct(lib.GetType("sequences::predicted_structures"))


# Predicts a PDB per input ORF using HuggingFace EsmForProteinFolding.
# Sequences > MAX_LEN AA are skipped (ESMFold's pairwise attention is O(L^2)).
# A 3g.40gb MIG slice with chunk_size=64 fits up to ~2048 AA in bf16.
#
# Per metasmith/dev guidance (msg #153): runtime knobs baked at module top.
# Unlike the encoder PLMs, ESMFold has *native* chunked attention via
# trunk.set_chunk_size(...) — no sliding-window aggregation needed; longer
# seqs above MAX_LEN are simply skipped (~380/1.44M at cap=2048).
MAX_LEN    = 2048
CHUNK_SIZE = 64

INFERENCE = r'''
import argparse, os, sys
from pathlib import Path
import torch
from transformers import EsmForProteinFolding, AutoTokenizer

p = argparse.ArgumentParser()
p.add_argument("--weights", required=True)
p.add_argument("--fasta", required=True)
p.add_argument("--out-dir", required=True)
p.add_argument("--device", default="cuda")
p.add_argument("--max-len", type=int, default=2048,
               help="skip sequences longer than this (OOM-protection)")
p.add_argument("--chunk-size", type=int, default=64,
               help="ESMFold trunk chunk size; lower=less memory, slower")
a = p.parse_args()

# Note: PYTORCH_CUDA_ALLOC_CONF must be set before torch initializes CUDA,
# which happens at the first CUDA call below. We set it via the apptainer
# `--env` flag on the container arm (see protocol() below), not here, because
# `os.environ.setdefault` after `import torch` is too late to influence the
# allocator's startup configuration.
device = torch.device(a.device if (a.device == "cpu" or torch.cuda.is_available()) else "cpu")
tok = AutoTokenizer.from_pretrained(a.weights)
mdl = EsmForProteinFolding.from_pretrained(a.weights, low_cpu_mem_usage=True).to(device).eval()
if device.type == "cuda":
    mdl.esm = mdl.esm.to(dtype=torch.bfloat16)
mdl.trunk.set_chunk_size(a.chunk_size)

out_dir = Path(a.out_dir)
out_dir.mkdir(parents=True, exist_ok=True)

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

skipped = 0
total = len(ids)
with torch.no_grad():
    for i, (sid, seq) in enumerate(zip(ids, seqs)):
        if len(seq) > a.max_len:
            sys.stderr.write(f"skip {sid}: len {len(seq)} > {a.max_len}\n")
            skipped += 1
            continue
        inputs = tok([seq], return_tensors="pt", add_special_tokens=False).to(device)
        output = mdl(**inputs)
        cif = mdl.output_to_pdb(output)[0]
        (out_dir / f"{sid}.pdb").write_text(cif)
        if (i + 1) % 50 == 0:
            print(f"hb {i+1}/{total}", flush=True)
        # ESMFold per-sequence intermediates can leave the caching allocator
        # holding many GB across iterations on metag-scale workloads; without
        # this flush we OOM mid-shard despite per-call peak < slice capacity.
        del inputs, output, cif
        if device.type == "cuda":
            torch.cuda.empty_cache()
print(f"esmfold done: folded {len(ids)-skipped}, skipped {skipped} (>{a.max_len} AA)", flush=True)
'''


def protocol(context: ExecutionContext):
    iorfs    = context.Input(orfs)
    iw       = context.Input(weights)
    istruct  = context.Output(out_struct)

    device = "cuda"   # inference script falls back to cpu via torch.cuda.is_available()

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")
    context.LocalShell("mkdir -p structures")

    script = Path("infer_esmfold.py")
    with open(script, "w") as f:
        f.write(INFERENCE)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"weights", "/weights"),
            (context.external_cwd/"structures", "/out"),
            (context.external_cwd/script.name, f"/work/{script.name}"),
        ],
        args=[
            "--nv",
            "--env", f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES','')}",
            "--env", "PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True",
        ],
        cmd=f"""
            python /work/{script.name} \
                --weights /weights \
                --fasta {iorfs.container} \
                --out-dir /out \
                --device {device} \
                --max-len {MAX_LEN} \
                --chunk-size {CHUNK_SIZE}
        """,
    )

    context.LocalShell(f"tar -czf {istruct.local} -C structures .")

    return ExecutionResult(
        manifest=[{out_struct: istruct.local}],
        success=istruct.local.exists(),
    )


# NOTE: GPU via context.params["gpus"]; Resources has no gpus= field yet.
# ESMFold weights ~10 GB; needs ~16 GB VRAM for moderate sequences.
TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=3),
    ),
)
