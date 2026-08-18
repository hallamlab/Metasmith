"""The redox invariance repair -- the one correction the old graph could not express.

NOTHING IN THE PREVIOUS GRAPH RAN OVER A FINISHED STACK. `aam_ensemble` fused, stacked,
closed the ledger and minted the bake in one protocol, so there was no artifact between
"the layers agree" and "this is the reference" for a correction to be applied to. This step
is that seam, and the seam is why the split was worth doing.

WHAT IT CORRECTS. NADH and NAD(+) differ by one hydrogen, and hydrogen is not in this
graph's element vocabulary -- so to a maximum-common-substructure mapper the two 21-carbon
skeletons are indistinguishable and it will route a substrate's carbon into the cofactor.
A hydride transfer does no such thing. Every C/N/P correspondence running between a
NAD(P)/FAD/FMN couple and a substrate is therefore impossible, and the measured population
is 5,457 reactions -- concentrated in `disagree_diluted` and `<member>_only`, with
`curated` and `consensus` clean.

IT IS A REPAIR AND NOT A FILTER, which is the plan's own correction to the proposal it
comes from and the reason coverage may not fall here. A refused arm's weight is not
deleted: the source atom's surviving arms are rescaled back to the total it started with,
so the refusal CONCENTRATES the atom's claim on the destination that survives the
invariant. Where no arm survives, the couple is removed and `atom_pairs.forced_pairs` is
asked whether conservation settles the remainder on its own. What is left after that is a
real loss, and it gets its own ledger outcome rather than being folded into
`mapped_nothing`.

SULFUR IS THE CANARY AND THE RUN ASSERTS IT. NAD, NADP, FAD and FMN carry no sulfur, so an
S row cannot be a crossing arm; the module exits non-zero if the S count moves, because
that would mean the scope predicate reached something it has no invariant for.

THE REFUSALS SHIP. One row per refused correspondence under a named predicate, beside the
table of which ids were treated as which cofactor family and state -- and which carry a
family's exact C/N/P signature under a name the map does not recognise, which is the check
that says the name map has gone stale against a new MNXref rather than that the chemistry
changed.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
stacked     = model.AddRequirement(lib.GetType("interm::aam_stack"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_pairs   = model.AddProduct(lib.GetType("interm::aam_pairs"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    ist  = context.Input(stacked)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    ilib = context.Input(bakelib)
    iout = context.Output(out_pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p _lookups redox
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {iar.container} _lookups/atom_ranks.parquet

        {py} -m ecspr.bake.aam.redox repair \
            --pairs {ist.container} --lookups _lookups \
            --out redox/aam_pairs.parquet \
            --out-refusals redox/refusals.parquet \
            --out-cofactors redox/cofactors.tsv \
            --out-emptied redox/emptied.txt \
            --out-summary redox/summary.tsv

        # THE FIVE FILES TRAVEL TOGETHER. A corrected table with no record of what was
        # corrected is a table nobody can check, and the emptied list is what stops a
        # reaction the repair took reading as one no mapper answered.
        mkdir -p {iout.container}
        cp redox/aam_pairs.parquet redox/refusals.parquet redox/cofactors.tsv \
           redox/emptied.txt redox/summary.tsv {iout.container}/

        {py} -m ecspr.bake.evidence collect --root _ev --tool redox \
            --file redox/refusals.parquet redox/cofactors.tsv redox/emptied.txt \
                   redox/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    # `emptied.txt` is checked for EXISTENCE and not for content: an empty one is the good
    # outcome, and the whole design goal is that it stays short.
    want = ["aam_pairs.parquet", "refusals.parquet", "cofactors.tsv", "emptied.txt",
            "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_pairs: iout.local}, {ev: iev.local}],
        success=(all((iout.local / f).exists() for f in want)
                 and (iout.local / "aam_pairs.parquet").stat().st_size > 0
                 and (iev.local / "redox").is_dir()
                 and any((iev.local / "redox").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # The stacked table plus the metabolite table in memory, one equation parse per
    # reaction that has pairs, and no mapper. The re-derivation touches a handful of keys.
    resources=Resources(cpus=2, memory=Size.GB(48), duration=Duration(hours=2)),
)
