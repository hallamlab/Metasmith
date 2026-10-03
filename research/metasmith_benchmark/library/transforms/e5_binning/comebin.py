# The standard COMEBin, except that an assembly COMEBin cannot bin yields an empty table instead of a
# failed step. DAS Tool requires all three binners' tables, so one failed binner would end the whole
# MAG lane for that sample. COMEBin crashes outright on small assemblies: korem2015's 0.5 GB
# single-end runs give ~130 contigs of at least 1 kb, its training loader yields no batch, and it dies
# on `UnboundLocalError: local variable 'logits'`. An OOM or a walltime kill still fails the step,
# because it takes this protocol down with the container.
#
# It makes no bin FASTAs. DAS Tool reads only the table, and a nextflow output cannot be empty, so a
# bin product would fail the very step this exists to keep.
import os
from pathlib import Path
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::comebin.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
bam         = model.AddRequirement(lib.GetType("alignment::bam"), parents={asm})
table       = model.AddProduct(lib.GetType("binning::comebin_contig_to_bin_table"))


def _no_bins(context, why):
    Log.Warn(f"COMEBin made no bins: {why}. Recording an empty table, which das_tool leaves out")
    otable = context.Output(table)
    otable.local.write_text("")
    return ExecutionResult(manifest=[{table: otable.local}], success=True)


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
        return _no_bins(context, f"{usable_contigs} contigs of at least 1 kb")
    batch_size = min(usable_contigs, 1024)

    _cmd = f"""
            mkdir -p {bam_dir}
            cp -L {ibam.container} {bam_dir}/
            mkdir -p {workdir}
            run_comebin.sh -a {iasm.container} -o {workdir} -p {bam_dir} -t {threads} -b {batch_size} \
                || echo $? > comebin_exit
        """
    context.ExecWithEnv(
            env=image,
            args=[
                "--nv",
                "--env", f"CUDA_VISIBLE_DEVICES={os.environ.get('CUDA_VISIBLE_DEVICES','')}",
            ],
            cmd=_cmd,
        )

    res = Path(f"{workdir}/comebin_res/comebin_res.tsv")
    if Path("comebin_exit").exists() or not res.exists():
        code = Path("comebin_exit").read_text().strip() if Path("comebin_exit").exists() else "0"
        return _no_bins(context, f"exit {code} on {usable_contigs} contigs of at least 1 kb")

    otable = context.Output(table)
    context.LocalShell(f"cp {res} {otable.local}")
    return ExecutionResult(manifest=[{table: otable.local}], success=otable.local.exists())


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
