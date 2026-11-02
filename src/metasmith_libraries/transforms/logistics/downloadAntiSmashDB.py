from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::antismash.env"))
out   = model.AddProduct(lib.GetType("ref::antismash"))

# The downloader's sources and checksums belong to the antiSMASH release in
# antismash.env, so its own fetches stay rather than a FetchCommand. The step is
# local for the bandwidth; the indexing that follows (hmmpress, diamond makedb)
# fits the login node.


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    context.ExecWithEnv(env=image, cmd=f"""
        mkdir -p {iout.container}
        download-antismash-databases --database-dir {iout.container}
        antismash --databases {iout.container} --check-prereqs
    """)

    whole = all(
        (iout.local/d).is_dir() and any((iout.local/d).iterdir())
        for d in ("pfam", "resfam", "tigrfam", "clusterblast", "knownclusterblast", "nrps_pks")
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=12),
    ),
)
