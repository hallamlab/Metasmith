"""COMEBin binning — GPU-only.

CPU comebin is wasteful (7-19h per Spanish-Lakes sample). The container
referenced by `env::comebin.env` is now the CUDA-built variant
(`quay.io/hallamlab/external_comebin:gpu-1.0.4`); apptainer `--nv` exposes
the host driver/libcuda so COMEBin's Phase-2 contrastive training step
auto-detects `torch.cuda.is_available()` and runs on GPU.

Slurm wiring: the launching runner is responsible for setting per-process
clusterOptions that allocate a GPU MIG slice and the GPU SLURM account
(e.g. `--account=def-shallam_gpu --gpus=nvidia_h100_80gb_hbm3_3g.40gb:1`).
See `spanish-lakes/w3-binning/scripts/run_w3_binning.py` for the
make_fir_slurm_config-style override (pattern lifted from
`metasmith-libraries/deep-learning/main/launch_dl_embeddings.py`).

If a SLURM job lands without a GPU allocation, apptainer `--nv` emits a
warning and the CUDA-pytorch falls back to CPU — functional but slow;
don't do that intentionally.
"""
import os
import glob
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::comebin.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
bam         = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
bin_fasta   = model.AddProduct(lib.GetType("sequences::comebin_bin_fasta"))
table       = model.AddProduct(lib.GetType("binning::comebin_contig_to_bin_table"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)

    threads = context.params.get('cpus', 8)
    workdir = "comebin_out"
    bam_dir = "bam_input"

    context.LocalShell(f"grep -c '^>' {iasm.local} > contig_count.txt")
    contig_count = int(Path("contig_count.txt").read_text().strip())
    batch_size = max(32, min(contig_count, 1024))

    context.ExecWithEnv().ifContainerDo(
        env = image,
        args=[
            "--nv",
            "--env", f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES','')}",
        ],
        cmd = f"""
            mkdir -p {bam_dir}
            cp -L {ibam.container} {bam_dir}/
            mkdir -p {workdir}
            run_comebin.sh -a {iasm.container} -o {workdir} -p {bam_dir} -t {threads} -b {batch_size}
        """
    )

    outputs = []
    bin_dir = f"{workdir}/comebin_res/comebin_res_bins"
    bin_files = sorted(glob.glob(f"{bin_dir}/*.fa"))
    for i, bin_path in enumerate(bin_files):
        out_bin = context.Output(bin_fasta, i=i)
        context.LocalShell(f"cp {bin_path} {out_bin.local}")
        outputs.append({bin_fasta: out_bin.local})

    otable = context.Output(table)
    context.LocalShell(f"cp {workdir}/comebin_res/comebin_res.tsv {otable.local}")

    return ExecutionResult(
        manifest=outputs + [{table: otable.local}],
        success=len(outputs) > 0 and otable.local.exists(),
    )


# Wall time: COMEBin's Phase-2 contrastive training drops ~10-20× on a
# 3g.40gb MIG slice vs CPU — 4h cushion. Memory pinned modestly (host RSS
# was ~3 GB on CPU per nxf_trace.peak_vmem); VRAM lives off-RAM.
TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    )
)
