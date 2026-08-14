from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
gtf     = model.AddRequirement(lib.GetType("transcriptomics::stringtie_gtf"), parents={exp})
gff     = model.AddRequirement(lib.GetType("transcriptomics::braker3_gff"), parents={exp})
image   = model.AddRequirement(lib.GetType("env::stringtie.env"))
out     = model.AddProduct(lib.GetType("transcriptomics::merged_gtf"))

def protocol(context: ExecutionContext):
    gtf_paths=context.InputGroup(gtf)
    igff=context.Input(gff)
    iout=context.Output(out)
    threads = context.params.get('cpus')
    threads = 2 if threads is None else threads

    # Write a file listing all per-sample GTF paths
    gtf_list = Path("gtf_list.txt")
    with open(gtf_list, "w") as f:
        for p in gtf_paths:
            f.write(f"{p.container}\n")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            stringtie --merge \
                -G {igff.container} \
                -o merged.gtf \
                -p {threads} \
                {gtf_list}
        """,
    )
    context.LocalShell(f"mv merged.gtf {iout.local}")
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    )
)
