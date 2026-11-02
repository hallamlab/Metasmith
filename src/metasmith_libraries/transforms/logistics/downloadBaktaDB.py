from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::bakta.env"))
db    = model.AddProduct(lib.GetType("annotation::bakta_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv(
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
