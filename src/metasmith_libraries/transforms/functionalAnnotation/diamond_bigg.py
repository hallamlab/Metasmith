from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::carveme.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
hits  = model.AddProduct(lib.GetType("annotation::bigg_diamond_hits"))

CPUS = 8


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    ihits = context.Output(hits)

    # The search CarveMe 1.6.6's `carve` runs for a protein FASTA
    # (reconstruction/diamond.py::run_blast, default arguments) against the index the image
    # ships, plus --threads, which carve leaves unset. Its output is what `carve --diamond`
    # reads back, so a reconstruction can take this lane instead of searching again.
    context.ExecWithEnv(env=image, cmd=f"""
        db=$(python -c 'import carveme; print(carveme.project_dir + carveme.config.get("generated", "diamond_db"))')
        diamond blastp -d $db -q {iorfs.container} -o {ihits.container} --more-sensitive --top 10 --quiet --threads {CPUS}
    """)

    return ExecutionResult(
        manifest=[{hits: ihits.local}],
        success=ihits.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=CPUS, memory=Size.GB(16), duration=Duration(hours=6)),
)
