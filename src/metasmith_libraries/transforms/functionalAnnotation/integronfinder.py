import glob
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::integronfinder.env"))
asm = model.AddRequirement(lib.GetType("sequences::contig_batch"))
out_summary = model.AddProduct(lib.GetType("annotation::integronfinder_summary"))
out_integrons = model.AddProduct(lib.GetType("annotation::integronfinder_integrons"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    isummary = context.Output(out_summary)
    iintegrons = context.Output(out_integrons)

    threads = context.params.get("cpus", 8)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            integron_finder \
                --local-max \
                --func-annot \
                --cpu {threads} \
                --outdir if_out \
                {iasm.container} \
                || true
        """,
    )

    summ = sorted(glob.glob("if_out/Results_Integron_Finder_*/*.summary"))
    integ = sorted(glob.glob("if_out/Results_Integron_Finder_*/*.integrons"))

    if summ:
        context.LocalShell(f"cp {summ[0]} {isummary.local}")
    else:
        Path(isummary.local).write_text("ID_replicon\tCALIN\tcomplete\tIn0\ttopology\tsize\n")
    if integ:
        context.LocalShell(f"cp {integ[0]} {iintegrons.local}")
    else:
        Path(iintegrons.local).write_text("# No integrons detected\n")

    return ExecutionResult(
        manifest=[{out_summary: isummary.local, out_integrons: iintegrons.local}],
        success=isummary.local.exists() and iintegrons.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=6),
    ),
)
