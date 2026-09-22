from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::chopper.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
reads   = model.AddRequirement(lib.GetType("e2::adapter_trimmed_long_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::filtered_long_reads"))

# nf-core/mag 5.5.0: longreads_min_length 1000, longreads_min_quality unset, no lambda reference.
MIN_LENGTH = 1000


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iout = context.Output(out)
    cpus = context.params.get("cpus") or 1

    context.ExecWithEnv(env=image, cmd=f"""
        zcat {ireads.container} | chopper --threads {cpus} --minlength {MIN_LENGTH} | gzip > {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(cpus=4, memory=Size.GB(8), duration=Duration(hours=6)),
)
