from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
diamond = model.AddRequirement(lib.GetType("bench::bigg_hits_diamond"), parents={orfs})
kofam   = model.AddRequirement(lib.GetType("bench::bigg_hits_kofam"), parents={orfs})
out     = model.AddProduct(lib.GetType("bench::bigg_hits_both"))


# Both tables' rows. carve keeps each BiGG gene's best-scoring row, so a gene takes whichever
# source scores it higher.
def protocol(context: ExecutionContext):
    idiamond = context.Input(diamond)
    ikofam   = context.Input(kofam)
    iout     = context.Output(out)
    with open(iout.local, "w") as fout:
        for part in (idiamond.local, ikofam.local):
            fout.write(open(part).read())
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=10)),
)
