from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::bbtools.env"))
reads = model.AddRequirement(lib.GetType("sequences::clean_short_reads"))
ref = model.AddRequirement(lib.GetType("annotation::crassphage_ref"))
out_cov = model.AddProduct(lib.GetType("annotation::crassphage_coverage"))


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iref = context.Input(ref)
    iout = context.Output(out_cov)

    threads = context.params.get("cpus", 4)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            bbmap.sh -Xmx6g \
                ref={iref.container} \
                in={ireads.container} \
                nodisk=t \
                threads={threads} \
                covstats={iout.container} \
                ambiguous=random
        """,
    )

    return ExecutionResult(
        manifest=[{out_cov: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
