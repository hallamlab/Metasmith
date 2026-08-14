from pathlib import Path
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::busco.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
lineage  = model.AddRequirement(lib.GetType("annotation::busco_lineage"))
out      = model.AddProduct(lib.GetType("annotation::busco_full_table_chunk"))

def protocol(context: ExecutionContext):
    iorfs    = context.Input(orfs)
    ilineage = context.Input(lineage)
    iout     = context.Output(out)
    cpus     = context.params.get("cpus")
    cpus     = 8 if cpus is None else cpus

    lineage_name = Path(ilineage.external).name

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(ilineage.external, f"/busco_lineage/lineages/{lineage_name}")],
        cmd=f"""\
            busco \
                -i {iorfs.container} \
                -o busco_out \
                -m proteins \
                -l {lineage_name} \
                -c {cpus} \
                -f \
                --offline \
                --download_path /busco_lineage
        """,
    )

    tsv_files = list(Path("busco_out").rglob("full_table.tsv"))
    if tsv_files:
        context.LocalShell(f"cp {tsv_files[0]} {iout.local}")

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
