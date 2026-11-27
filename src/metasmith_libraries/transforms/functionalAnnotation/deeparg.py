from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::deeparg.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db    = model.AddRequirement(lib.GetType("ref::deeparg_db"))
out   = model.AddProduct(lib.GetType("annotation::deeparg_results_chunk"))

# PathoFact 2.0's rule deeparg: the LS (gene) model on proteins at --min-prob 0.7
# (Config.yaml), every other setting deeparg's own default. DIAMOND's E-value is against
# the database, so a chunk's calls do not depend on its neighbours.
MIN_PROB = 0.7


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idb   = context.Input(db)
    iout  = context.Output(out)

    # Theano compiles its kernels under HOME, which the image leaves unwritable.
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        export HOME=$PWD
        sed '/^[^>]/s/\\*//g' {iorfs.container} > query.faa
        deeparg predict --model LS --type prot --min-prob {MIN_PROB} \\
            -d {idb.container} --input query.faa --out chunk
        cp chunk.mapping.ARG {iout.container}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
