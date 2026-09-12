from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::kraken2_db"))

KRAKEN2_DB_URL = "https://genome-idx.s3.amazonaws.com/kraken/k2_standard_16gb_20240605.tar.gz"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    context.ExecWithEnv(
        env=image,
        cmd=f"""
            mkdir -p {iout.container}
            wget -q {KRAKEN2_DB_URL} -O k2.tar.gz
            tar xzf k2.tar.gz -C {iout.container}
            rm k2.tar.gz
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
        memory=Size.GB(8),
        duration=Duration(hours=6),
    ),
)
