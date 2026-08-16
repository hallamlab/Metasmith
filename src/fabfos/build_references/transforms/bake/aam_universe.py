"""One submission table, three classes, and the only thing a mapper member consumes.

THE INVARIANT THIS BUYS. `worklist.mappable` was "the one reader every member uses",
which made "the three members saw the same list" a convention enforced by a shared
function -- a member deriving its own universe could drift from its siblings by one filter
and the ensemble's disagreement rate would stop measuring disagreement between mappers.
With this type it stops being a convention and becomes a fact about the graph: there is
one table, and it is the only thing a member requires.

THREE CLASSES UNDER ONE KEY, AND THEY CANNOT COLLIDE. `whole` and `completed` are disjoint
on mnxr because the rescue completes exactly the reactions the worklist refused; `reduced`
keys on `MNXR#X`, which is not a valid MNXR. Uniqueness is asserted in the method rather
than trusted here -- a member reads this into a dict, so a duplicate key does not raise,
it drops one of two different molecules submitted under one name.

WHAT `submission_class` IS FOR. Layer identity used to be "which pass produced this",
which made layer membership depend on scheduling. It becomes "which class of submission
this row answers", which every row carries.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist  = model.AddRequirement(lib.GetType("interm::aam_worklist"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
partial   = model.AddRequirement(lib.GetType("interm::aam_partial"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_uni   = model.AddProduct(lib.GetType("interm::aam_universe"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irs  = context.Input(rescue)
    ipa  = context.Input(partial)
    ilib = context.Input(bakelib)
    iout = context.Output(out_uni)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p uni

        {py} -m ecspr.bake.aam.universe build \
            --worklist {iwl.container} \
            --rescued {irs.container}/rescued.parquet \
            --partial {ipa.container}/partial_universe.parquet \
            --out uni/universe.parquet \
            --out-summary uni/summary.tsv

        cp uni/universe.parquet {iout.container}

        {py} -m ecspr.bake.evidence collect --root _ev --tool universe \
            --file uni/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_uni: iout.local}, {ev: iev.local}],
        # An empty universe is every member mapping nothing, which is the one state that
        # makes every lane downstream green and empty at once.
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "universe").is_dir()
                 and any((iev.local / "universe").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # Three parquets concatenated and a uniqueness check.
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(minutes=30)),
)
