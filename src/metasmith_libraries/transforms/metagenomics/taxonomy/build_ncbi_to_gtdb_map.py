from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
taxdump  = model.AddRequirement(lib.GetType("ref::ncbi_taxdump"))
metadata = model.AddRequirement(lib.GetType("ref::gtdb_metadata"))
lcastar  = model.AddRequirement(lib.GetType("lib::lcastar"))
out      = model.AddProduct(lib.GetType("ref::ncbi_to_gtdb_map"))


def protocol(context: ExecutionContext):
    itax = context.Input(taxdump)
    imeta = context.Input(metadata)
    ilib = context.Input(lcastar)
    iout = context.Output(out)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            python3 {ilib.container}/ncbi_to_gtdb.py \
                --taxdump {itax.container} \
                --metadata {imeta.container}/bac120_metadata_r232.tsv.gz {imeta.container}/ar53_metadata_r232.tsv.gz \
                --out {iout.container}
        """,
    )

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=taxdump,
    resources=Resources(
        cpus=1,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)
