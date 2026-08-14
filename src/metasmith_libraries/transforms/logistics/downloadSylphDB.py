# REVIEW: URL points at the Sylph GTDB-r220 prebuilt sketch
# (c200, dbv1 = standard pre-cluster + ANI param set). The
# ref::sylph_db dtype expects a single .syldb file, so we save the
# download directly as that file rather than into a subdirectory.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::sylph_db"))

SYLPH_DB_URL = "http://faust.compbio.cs.cmu.edu/sylph-stuff/gtdb-r220-c200-dbv1.syldb"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q {SYLPH_DB_URL} -O {iout.container}
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
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
