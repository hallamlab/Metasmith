"""The partial lane: build element-reduced submissions for what no full map reached.

AFTER BOTH MAPPER PASSES, and that position is the point. This lane's target set is
"reactions that ended with nothing", and there is no way to know which those are without
having run the members -- so it is computed by SUBTRACTING every member's pairs table
from what the worklist and the rescue admitted, and the worklist's own size refusals are
added to it. A lane that guessed from reaction length would be aiming at a proxy for the
thing it can simply be told.

WHAT IT PRODUCES, and neither half needs a mapper to be useful:

  * `partial_forced.parquet` -- pairs conservation leaves no choice about. One substrate
    and one product carrying element X in equal counts is a unique bijection; n > 1 is
    the doubly-stochastic completion, spread rather than picked. Already in the
    extractor's shape, so the layer stack reads it with no special case.
  * `partial_universe.parquet` -- element-reduced submissions for everything conservation
    does not settle, one row per (reaction, element), in the schema the mapper lanes
    read. Pass 3 is the same three members over this.

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

worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))

# THE TARGET SET IS A FACT ABOUT A RUN, and it is computed by SUBTRACTION. Every member
# of both passes is required -- not for the pairs themselves but for which reactions have
# one: a reaction that was admitted and appears in no member's table is one that ended
# with nothing, whether the mapper returned empty, the extractor called it `stripped`, or
# it named no pair. An absent member would make the gap the whole universe, so the lane
# refuses one rather than treating it as covering nothing.
m_rxn       = model.AddRequirement(lib.GetType("interm::aam_member_rxnmapper"))
m_local     = model.AddRequirement(lib.GetType("interm::aam_member_localmapper"))
m_indigo    = model.AddRequirement(lib.GetType("interm::aam_member_indigo"))
m_rxn_r     = model.AddRequirement(lib.GetType("interm::aam_member_rxnmapper_rescue"))
m_local_r   = model.AddRequirement(lib.GetType("interm::aam_member_localmapper_rescue"))
m_indigo_r  = model.AddRequirement(lib.GetType("interm::aam_member_indigo_rescue"))
rescue      = model.AddRequirement(lib.GetType("interm::aam_rescue"))

bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_partial = model.AddProduct(lib.GetType("interm::aam_partial"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    members = [context.Input(m) for m in
               (m_rxn, m_local, m_indigo, m_rxn_r, m_local_r, m_indigo_r)]
    ires = context.Input(rescue)
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
    covered = " ".join(str(m.container) for m in members)

    cmd = f"""
        set -e
        {stage_lookups}
        mkdir -p partial

        {py} -m ecspr.bake.aam.partial build --lookups _lookups \
            --worklist {iwl.container} \
            --rescued {ires.container}/rescued.parquet \
            --covered {covered} \
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
        # the members reached everything, which is the state this lane exists to make
        # visible. So the check is that all three files exist, not that any is non-empty.
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
