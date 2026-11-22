"""Normalise the AOI table: ids, names, country and area for every AOI."""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::geoquery.env"))
table = model.AddRequirement(lib.GetType("geoquery::aoi_table"))
aois  = model.AddProduct(lib.GetType("geoquery::aois"))


def protocol(context: ExecutionContext):
    i, o = context.Input(table), context.Output(aois)
    context.ExecWithEnv(
        env=image,
        cmd=f"python -m geoquery.normalise --in {i.container} --out {o.container}",
    )
    return ExecutionResult(manifest=[{aois: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol, model=model, group_by=table,
    resources=Resources(cpus=1, memory=Size.GB(1), duration=Duration(minutes=10)),
)
