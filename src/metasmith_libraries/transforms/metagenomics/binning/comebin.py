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

    context.LocalShell(
        "awk '/^>/{if(l>=1000)n++; l=0; next}{l+=length($0)}"
        f"END{{if(l>=1000)n++; print n+0}}' {iasm.local} > usable_contig_count.txt"
    )
    usable_contigs = int(Path("usable_contig_count.txt").read_text().strip())
    if usable_contigs < 2:
        return ExecutionResult(manifest=[], success=False)
    batch_size = min(usable_contigs, 1024)

    _cmd = f"""
            mkdir -p {bam_dir}
            cp -L {ibam.container} {bam_dir}/
            mkdir -p {workdir}
            run_comebin.sh -a {iasm.container} -o {workdir} -p {bam_dir} -t {threads} -b {batch_size}
        """
    context.ExecWithEnv() \
        .ifContainerDo(
            env=image,
            args=[
                "--nv",
                "--env", f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES','')}",
            ],
            cmd=_cmd,
        ) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

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
