from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::busco.env"))
src     = model.AddRequirement(lib.GetType("annotation::busco_source"))
out     = model.AddProduct(lib.GetType("annotation::busco_lineage"))

LINEAGE = "eukaryota_odb10"

def protocol(context: ExecutionContext):
    iout = context.Output(out)

    # busco --download ignores --download_path and always writes to
    # ./busco_downloads/ relative to cwd. Download there then move.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            busco \
                --download {LINEAGE} \
                --download_path ./busco_downloads
            mv ./busco_downloads/lineages/{LINEAGE} {iout.container}
        """,
    )

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=src,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
