from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
ref   = model.AddProduct(lib.GetType("annotation::feast_sources"))

LIB_FEAST_SOURCES = "/home/phyberos/project-rpp/lib/feast_sources"
REQUIRED = ["FEAST_otus.csv", "FEAST_metadata_final.csv"]


def protocol(context: ExecutionContext):
    iref = context.Output(ref)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            mkdir -p {iref.container}
            cp {LIB_FEAST_SOURCES}/FEAST_otus.csv {iref.container}/
            cp {LIB_FEAST_SOURCES}/FEAST_metadata_final.csv {iref.container}/
        """,
    )

    ok = all((iref.local / f).exists() for f in REQUIRED)
    return ExecutionResult(
        manifest=[{ref: iref.local}],
        success=ok,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(minutes=10),
    ),
)
