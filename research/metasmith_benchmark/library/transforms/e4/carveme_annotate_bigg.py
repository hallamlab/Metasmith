from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e4::carveme_122.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
hits    = model.AddProduct(lib.GetType("bench::bigg_diamond_hits"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    ihits = context.Output(hits)

    # The call CarveMe 1.2.2's `carve` makes for a protein FASTA (reconstruction/diamond.py::run_blast),
    # against the database it ships, with its default arguments. find_spec, because importing
    # carveme demands CPLEX, which this step does not bind.
    context.ExecWithEnv(env=image, cmd=f"""
        db=$(python -c 'import importlib.util, os; print(os.path.dirname(importlib.util.find_spec("carveme").origin))')/data/input/bigg_proteins.dmnd
        diamond blastp -d "$db" -q {iorfs.container} -o {ihits.container} --more-sensitive --top 10
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
