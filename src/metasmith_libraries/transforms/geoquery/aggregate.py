"""Gather every source table into the integrated long, wide, coverage and gaps tables."""
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::geoquery.env"))
aois     = model.AddRequirement(lib.GetType("geoquery::aois"))
tables   = model.AddRequirement(lib.GetType("geoquery::source_table"), parents={aois})
long_    = model.AddProduct(lib.GetType("geoquery::observations_long"))
wide     = model.AddProduct(lib.GetType("geoquery::features_wide"))
coverage = model.AddProduct(lib.GetType("geoquery::coverage"))
gaps     = model.AddProduct(lib.GetType("geoquery::gaps"))
PRODUCTS = {long_: "observations_long.parquet", wide: "features_wide.parquet",
            coverage: "coverage.csv", gaps: "gaps.csv"}


def protocol(context: ExecutionContext):
    ins = " ".join(str(t.container) for t in context.InputGroup(tables))
    context.ExecWithEnv(
        env=image,
        cmd=f"python -m geoquery.aggregate --tables {ins} --out-dir integrated",
    )
    outs = {p: context.Output(p) for p in PRODUCTS}
    for p, name in PRODUCTS.items():
        context.LocalShell(f"mv integrated/{name} {outs[p].local}")
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=all(o.local.exists() for o in outs.values()))


TransformInstance(
    protocol=protocol, model=model, group_by=aois,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(minutes=30)),
)
