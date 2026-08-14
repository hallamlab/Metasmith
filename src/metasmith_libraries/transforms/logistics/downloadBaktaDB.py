from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::bakta.env"))
db    = model.AddProduct(lib.GetType("annotation::bakta_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    # bakta ships a `bakta_db` helper that fetches its packaged database from
    # Zenodo. The light build lands in <out>/db-light/, which is exactly the
    # subdirectory bakta_noncoding.py mounts at /db and references as
    # `--db /db/db-light`. The non-coding-only consumer (--skip-cds) does not
    # touch the AMRFinderPlus DB, so no `amrfinder_update` step is needed here.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            mkdir -p {idb.container}
            bakta_db download --output {idb.container} --type light
        """,
    )

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=(idb.local / "db-light").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=4),
    ),
)
