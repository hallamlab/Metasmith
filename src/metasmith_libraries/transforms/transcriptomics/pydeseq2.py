from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
qgtf    = model.AddRequirement(lib.GetType("transcriptomics::stringtie_quant_gtf"), parents={exp})
image   = model.AddRequirement(lib.GetType("env::pydeseq2.env"))
helpers = model.AddRequirement(lib.GetType("lib::transcriptomics"))
out     = model.AddProduct(lib.GetType("transcriptomics::diff_count_table"))

def protocol(context: ExecutionContext):
    qgtf_paths=context.InputGroup(qgtf)
    ihelp = context.Input(helpers)
    iout=context.Output(out)

    manifest = Path("sample_manifest.tsv")
    with open(manifest, "w") as f:
        for i, p in enumerate(qgtf_paths):
            sample_name = p.local.parent.name if p.local.parent.name != "." else f"sample_{i}"
            f.write(f"{sample_name}\t{p.container}\n")

    _cmd = f"python {ihelp.container}/pydeseq2.py {manifest} {iout.container}"
    context.ExecWithEnv(env=image, cmd=_cmd)
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
        memory=Size.GB(8),
        duration=Duration(hours=1),
    )
)
