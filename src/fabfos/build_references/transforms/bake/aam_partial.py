"""The partial lane: build element-reduced submissions for what no full map will reach.

BEFORE ANY MAPPER RUNS, which is a reversal and the whole point of this task. This lane
used to sit after both mapper passes because its target set was "reactions that ended with
nothing" -- a fact about a RUN, computed by subtracting six finished member tables -- and
its own docstring argued that a lane guessing from reaction length would be aiming at a
proxy for the thing it could simply be told. Being told cost three sequential mapper
passes and nine lanes.

`interm::aam_forecast` is what changes the trade. It predicts silence from NAMED
MECHANISMS rather than from a proxy -- our two size caps, RXNMapper's context window as a
property of the string, and the previous bake's own records of what hung, timed out and
came back empty -- and it may only ever ADD submissions. Over-offering costs mapper time
and nothing else, because the layer stack is additive and its gates refuse rather than
warn, so a reduction built for a reaction that maps fine is never claimed by anything.

WHAT IT PRODUCES, and neither half needs a mapper to be useful:

  * `partial_forced.parquet` -- pairs conservation leaves no choice about. One substrate
    and one product carrying element X in equal counts is a unique bijection; n > 1 is
    the doubly-stochastic completion, spread rather than picked. Already in the
    extractor's shape, so the layer stack reads it with no special case. NOT restricted to
    the forecast's offers: it is exact and free once the equation is parsed, and making an
    exact pairing depend on a prediction is the one direction this lane must not be wrong
    in.
  * `partial_universe.parquet` -- element-reduced submissions for everything conservation
    does not settle, one row per (reaction, element), which `aam_universe` concatenates
    with the whole and completed classes into the one table the members read.

WHY REDUCING IS NOT STRIPPING. An atom of X cannot come from a participant carrying no
X, so dropping the X-free participants removes no possible source and no possible
destination for an X atom -- and the balance of X across the KEPT set is checked before
anything is written, so an unaccounted X is a refusal rather than a submission. Only X's
pairs are read back out; every other element's map in a reduced submission is discarded
UNREAD, because for those elements the reduction really is a strip.

THE LAYER GOES DOWN LAST. `aam_layers.stack` restricts each layer to what nothing below
it claimed, so a partial map can never overwrite a real one -- that is machinery, not a
promise. And every row it contributes carries `source=partial` with its own method, so
`aam_worklist close` can tell a reaction that banked PARTIAL pairs from one that banked a
full map and from one that banked nothing. A partial pair that reads as banked is the
stop line this lane is built around.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))

# THE TARGET SET IS ONE TABLE NOW, and this requirement is the reversal: the lane depends
# on a PREDICTION rather than on six finished member products, so the planner can schedule
# it before a single mapper starts.
forecast    = model.AddRequirement(lib.GetType("interm::aam_forecast"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))

bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_partial = model.AddProduct(lib.GetType("interm::aam_partial"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    ifc  = context.Input(forecast)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    ilib = context.Input(bakelib)
    iout = context.Output(out_partial)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    # Same re-presentation the rescue does: the lookups are read as a DIRECTORY, and
    # their content-addressed staging paths are not siblings.
    stage_lookups = f"""
        mkdir -p _lookups
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {iar.container} _lookups/atom_ranks.parquet
    """

    cmd = f"""
        set -e
        {stage_lookups}
        mkdir -p partial

        {py} -m ecspr.bake.aam.partial build --lookups _lookups \
            --forecast {ifc.container} \
            --out partial/partial_universe.parquet \
            --out-forced partial/partial_forced.parquet \
            --out-summary partial/summary.tsv

        # THE THREE FILES TRAVEL TOGETHER. The universe carries the reduced participant
        # lists the extractor must read instead of re-parsing the equation, and the
        # summary is the only record of what the reduction could NOT reach -- which is
        # the half a coverage claim is made of.
        mkdir -p {iout.container}
        cp partial/partial_universe.parquet partial/partial_forced.parquet \
           partial/summary.tsv {iout.container}/

        {py} -m ecspr.bake.evidence collect --root _ev --tool partial \
            --file partial/partial_universe.parquet partial/partial_forced.parquet \
                   partial/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    want = ["partial_universe.parquet", "partial_forced.parquet", "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_partial: iout.local}, {ev: iev.local}],
        # An EMPTY universe is a legitimate outcome here, unlike the rescue's: it means
        # nothing the forecast offered survived the balance test, which is the state this
        # lane exists to make visible. So the check is that all three files exist, not
        # that any is non-empty.
        success=(all((iout.local / f).exists() for f in want)
                 and (iev.local / "partial").is_dir()
                 and any((iev.local / "partial").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # Formula arithmetic and one rdkit parse per submission -- no mapper, so no long
    # tail. The memory is the metabolite table and the rank index, the same two the
    # rescue holds.
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=2)),
)
