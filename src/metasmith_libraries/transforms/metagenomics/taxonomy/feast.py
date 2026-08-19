from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::feast.env"))
sources = model.AddRequirement(lib.GetType("annotation::feast_sources"))
out_props = model.AddProduct(lib.GetType("annotation::feast_proportions"))


def protocol(context: ExecutionContext):
    isources = context.Input(sources)
    iout = context.Output(out_props)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(isources.external, "/feast_sources")],
        cmd=f"""
            FEAST \
                --otus /feast_sources/FEAST_otus.csv \
                --metadata /feast_sources/FEAST_metadata_final.csv \
                --outdir feast_out
            cp feast_out/FEAST_results_source_contributions_matrix.txt {iout.container} \
                || cp feast_out/*source_contributions* {iout.container}
        """,
    )

    if not iout.local.exists():
        Path(iout.local).write_text("source\tproportion\n")

    return ExecutionResult(
        manifest=[{out_props: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
