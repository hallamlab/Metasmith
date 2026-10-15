from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4::diamond_0930.env"))
faa     = model.AddRequirement(lib.GetType("e4::carveme_bigg_proteins"))
db      = model.AddProduct(lib.GetType("e4::bigg_diamond_db_0930"))


def protocol(context: ExecutionContext):
    ifaa = context.Input(faa)
    idb  = context.Output(db)

    # carveme_init's last step. The .dmnd CarveMe 1.2.2 ships predates DIAMOND 0.9.30 and does not open.
    context.ExecWithEnv(env=image, cmd=f"""
        diamond makedb --in {ifaa.container} -d bigg_proteins
        mv bigg_proteins.dmnd {idb.container}
    """)

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=faa,
    resources=Resources(cpus=2, memory=Size.GB(4), duration=Duration(minutes=30)),
)
