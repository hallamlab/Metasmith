from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::sylph_db"))

SYLPH_DB_URL = "https://faust.compbio.cs.cmu.edu/sylph-stuff/gtdb-r220-c200-dbv1.syldb"
# From checksums.md5 beside it. A run that points sylph_url at another database
# passes that database's md5 as sylph_md5.
SYLPH_DB_MD5 = "23ba2901b6f96357c1e63b6458f7e6ad"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    url  = context.params.get("sylph_url", SYLPH_DB_URL)
    md5  = context.params.get("sylph_md5", SYLPH_DB_MD5)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        FetchCommand(url, iout.local, md5=md5, label="sylph GTDB sketches"),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.is_file() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(2),
        duration=Duration(hours=6),
    ),
)
