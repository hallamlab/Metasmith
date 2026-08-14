import os
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image    = model.AddRequirement(lib.GetType("env::eggnog-mapper.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
data_dir = model.AddRequirement(lib.GetType("annotation::eggnog_data"))
out      = model.AddProduct(lib.GetType("annotation::eggnog_results_chunk"))

def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idata = context.Input(data_dir)
    iout  = context.Output(out)

    cpus = context.params.get("cpus")
    cpus = 16 if cpus is None else cpus

    # Copy eggNOG DB to node-local storage to avoid Lustre I/O contention.
    # Use ./ (CWD) which metasmith sets to the Nextflow work dir on localscratch
    # (SLURM_TMPDIR). Do NOT use TMPDIR or /tmp — those resolve to a RAM disk
    # (tmpfs) on Compute Canada nodes, and 100 concurrent 48GB copies OOM-kill.
    local_data = Path("./eggnog_data")
    context.LocalShell(f"cp -r {idata.external} {local_data}")

    # Strip stop codon asterisks from protein sequences (same as interproscan)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(local_data, "/eggnog_data")],
        cmd=f"""
            sed '/^[^>]/s/\\*//g' {iorfs.container} > ./proteins.clean.faa
            emapper.py \
                --data_dir /eggnog_data \
                -i ./proteins.clean.faa \
                -o results \
                -m diamond \
                --cpu {cpus}
        """,
    )

    context.LocalShell(f"cp results.emapper.annotations {iout.local}")

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=24),
    ),
)
