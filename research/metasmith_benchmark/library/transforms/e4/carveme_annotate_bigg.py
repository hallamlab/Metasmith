from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4::diamond_0930.env"))
db      = model.AddRequirement(lib.GetType("e4::bigg_diamond_db_0930"))
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
hits    = model.AddProduct(lib.GetType("bench::bigg_diamond_hits"))


def protocol(context: ExecutionContext):
    idb   = context.Input(db)
    iorfs = context.Input(orfs)
    ihits = context.Output(hits)

    # The call CarveMe 1.2.2's `carve` makes for a protein FASTA (reconstruction/diamond.py::run_blast),
    # with its default arguments and no --threads.
    context.ExecWithEnv(env=image, cmd=f"""
        diamond blastp -d {idb.container} -q {iorfs.container} -o {ihits.container} --more-sensitive --top 10
    """)

    return ExecutionResult(
        manifest=[{hits: ihits.local}],
        success=ihits.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=4, memory=Size.GB(8), duration=Duration(hours=1)),
)
