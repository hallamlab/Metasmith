from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::busco.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orfs"))
lineage  = model.AddRequirement(lib.GetType("annotation::busco_lineage"))
out      = model.AddProduct(lib.GetType("annotation::busco_results"))

def protocol(context: ExecutionContext):
    iorfs    = context.Input(orfs)
    ilineage = context.Input(lineage)
    iout     = context.Output(out)
    cpus     = context.params.get("cpus")
    cpus     = 8 if cpus is None else cpus

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(ilineage.external, "/busco_lineage/lineages/eukaryota_odb10")],
        cmd=f"""\
            busco \
                -i {iorfs.container} \
                -o busco_out \
                -m proteins \
                -l eukaryota_odb10 \
                -c {cpus} \
                -f \
                --offline \
                --download_path /busco_lineage
        """,
    )

    json_files = list(Path("busco_out").rglob("short_summary*.json"))
    if json_files:
        context.LocalShell(f"cp {json_files[0]} {iout.local}")
    else:
        txt_files = list(Path("busco_out").rglob("short_summary*.txt"))
        if txt_files:
            context.LocalShell(f"cp {txt_files[0]} {iout.local}")

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
