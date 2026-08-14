from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))  # has wget + tar
ref   = model.AddProduct(lib.GetType("annotation::pathofact_db"))

# PathoFact 2.0 core database (HMM profiles, DIAMOND DBs, MGE / toxin signatures),
# ~1.14 GB, published on Zenodo (record 14192463, "PathoFact 2.0 core database").
# The archive is NOT baked into the container image; it is staged here and bound
# into the PathoFact container at /pathofact_db at runtime. The container's
# `pathofact` wrapper points the pipeline's config `db` at this directory.
ZENODO_URL = ("https://zenodo.org/api/records/14192463/files/"
              "DATABASES.tar.gz/content")


def protocol(context: ExecutionContext):
    iref = context.Output(ref)

    # fetch + extract into the product dir, preserving the archive's layout
    # (top-level DATABASES/ tree). The pathofact wrapper resolves the DB root
    # (this dir or its nested DATABASES/).
    context.ExecWithEnv().ifContainerDo(
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
