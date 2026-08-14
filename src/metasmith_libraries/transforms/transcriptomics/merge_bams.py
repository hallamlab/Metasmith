from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
bam     = model.AddRequirement(lib.GetType("transcriptomics::star_bam"), parents={exp})
image   = model.AddRequirement(lib.GetType("env::samtools.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::merged_bam"))

def protocol(context: ExecutionContext):
    bam_paths = context.InputGroup(bam)
    iout = context.Output(out)
    threads = context.params.get('cpus')
    threads = 8 if threads is None else threads

    # Write BAM list for samtools merge
    bam_list = Path("bam_list.txt")
    with open(bam_list, "w") as f:
        for p in bam_paths:
            f.write(f"{p.container}\n")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            samtools merge -@ {threads} -b {bam_list} merged.bam
            samtools sort -@ {threads} -o sorted.bam merged.bam
            samtools index sorted.bam
        """,
    )
    context.LocalShell(f"mv sorted.bam {iout.local}")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    )
)
