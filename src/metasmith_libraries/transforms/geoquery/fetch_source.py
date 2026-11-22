"""Query one source over every AOI; a source that finds nothing still writes its status rows."""
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::geoquery.env"))
aois   = model.AddRequirement(lib.GetType("geoquery::aois"))
source = model.AddRequirement(lib.GetType("geoquery::source"))
table  = model.AddProduct(lib.GetType("geoquery::source_table"))


def protocol(context: ExecutionContext):
    a, s, o = context.Input(aois), context.Input(source), context.Output(table)
    context.ExecWithEnv(
        env=image,
        cmd=f"python -m geoquery.fetch --aois {a.container} --source {s.container} --out {o.container}",
    )
    return ExecutionResult(manifest=[{table: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol, model=model, group_by=source,
    # network-bound: a fetch uses ~20% of a core, so one core each lets twice as many run
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=30)),
)
