from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::antismash.env"))
out   = model.AddProduct(lib.GetType("ref::antismash"))


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    # antiSMASH bundles a download command that fetches Pfam, ClusterBlast,
    # MIBiG, Resfams, NRPS/PKS substrate prediction models, etc.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            mkdir -p {iout.container}
            download-antismash-databases --database-dir {iout.container}
        """,
    )

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=12),
    ),
)
