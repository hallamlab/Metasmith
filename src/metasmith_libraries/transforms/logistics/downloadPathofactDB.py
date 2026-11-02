from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
ref   = model.AddProduct(lib.GetType("annotation::pathofact_db"))

ZENODO_URL = ("https://zenodo.org/api/records/14192463/files/"
              "DATABASES.tar.gz/content")


def protocol(context: ExecutionContext):
    iref = context.Output(ref)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            mkdir -p {iref.container}
            wget -q --no-check-certificate "{ZENODO_URL}" -O pathofact_db.tar.gz
            tar xzf pathofact_db.tar.gz -C {iref.container}
            rm -f pathofact_db.tar.gz
        """,
    )

    return ExecutionResult(
        manifest=[{ref: iref.local}],
        success=iref.local.exists() and any(iref.local.iterdir()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
