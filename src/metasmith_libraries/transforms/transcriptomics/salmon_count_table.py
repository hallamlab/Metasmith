from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
quant   = model.AddRequirement(lib.GetType("transcriptomics::salmon_quant"), parents={exp})
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
helpers = model.AddRequirement(lib.GetType("lib::transcriptomics"))
out     = model.AddProduct(lib.GetType("transcriptomics::count_table"))

def protocol(context: ExecutionContext):
    quant_paths=context.InputGroup(quant)
    ihelp = context.Input(helpers)
    iout=context.Output(out)

    manifest = Path("quant_manifest.tsv")
    with open(manifest, "w") as f:
        for i, p in enumerate(quant_paths):
            sample_name = p.local.parent.name if p.local.parent.name != "." else f"sample_{i}"
            f.write(f"{sample_name}\t{p.container}\n")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"python {ihelp.container}/salmon_count_table.py {manifest} {iout.container}",
    )
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
