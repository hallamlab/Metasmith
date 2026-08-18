"""Copy MNXref's own curated twin onto its structureless role twin, where it is neutral.

WHAT THE ARGUMENT IS. `Acceptor` (MNXM8975) has no structure and blocks 607 reactions.
MNXref itself holds `A` (MNXM35) with the SMILES `[H][*]([H])([H])[H]` and files both
under the same cross-reference description across kegg, seed and sabiork. Supplying the
record MetaNetX already wrote is a smaller claim than inventing a stand-in, and this
step makes exactly that claim and no other.

WHAT KEEPS IT SAFE IS ONE PREDICATE. The twin must be ELEMENT-NEUTRAL -- zero C, N, S
and P, all four known from `lookup::element_counts`. Such a body can neither absorb nor
emit a mapped atom, so admitting it unblocks the reaction for its concrete partners
while asserting nothing about them. The acyl-carrier family fails that predicate on its
own numbers (the ACP twin carries C14 N3 S1 P1 through a thioester), so the refusal that
`curation.REFUSE` spells out by name is here enforced by arithmetic, and a new carrier
nobody has heard of is refused on the same terms.

WHY IT NEEDS THE RECOUNT. `H4*` has no countable FORMULA -- `count_element` refuses a
`*` -- so without the recount every element-neutral twin in MNXref reads as unknown and
this lane refuses all of them. The one input that makes the lane possible is the one
`aam_recount` produces.

IT DOES NOT BYPASS THE ARBITER. The output is a crosswalk in the rescue's own shape;
`admit` checks it like any other row, the `*` bodies still have to cancel across the
equation, and the per-element balance still decides. The overlap with the existing
`lane_acceptor` is reported rather than assumed away: that lane already draws six
spellings of the same claim, so what this one buys is generality, and the number is in
`summary.tsv`.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
xrefs       = model.AddRequirement(lib.GetType("lookup::xrefs"))
synonyms    = model.AddRequirement(lib.GetType("lookup::synonyms"))
counts      = model.AddRequirement(lib.GetType("lookup::element_counts"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_blk     = model.AddProduct(lib.GetType("interm::aam_blockers"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    ixr  = context.Input(xrefs)
    isy  = context.Input(synonyms)
    iec  = context.Input(counts)
    ilib = context.Input(bakelib)
    iout = context.Output(out_blk)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    # The lookups are read as a DIRECTORY, the same re-presentation the rescue makes:
    # content-addressed staging leaves them in unrelated places, and four separate path
    # arguments are four chances for one to come from a different build. `atom_ranks` is
    # absent because this lane never indexes an atom -- it decides which structure a
    # metabolite gets, and the ranks are minted downstream from that decision.
    cmd = f"""
        set -e
        mkdir -p _lookups
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {ixr.container} _lookups/xrefs.parquet
        ln -sfn {isy.container} _lookups/synonyms.parquet

        {py} -m ecspr.bake.aam.twins blockers --lookups _lookups \
            --element-counts {iec.container} \
            --worklist {iwl.container} \
            --out blockers

        mkdir -p {iout.container}
        cp blockers/crosswalk.tsv blockers/decisions.tsv blockers/summary.tsv \
           {iout.container}/

        # THE REFUSALS ARE THE DELIVERABLE THE NEXT LANE READS. `decisions.tsv` names
        # why each blocker was not resolved, under a closed set of predicates; a run
        # that shipped only the accepts would leave the next argument with nowhere to
        # start.
        {py} -m ecspr.bake.evidence collect --root _ev --tool blockers \
            --file blockers/crosswalk.tsv blockers/decisions.tsv blockers/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    want = ["crosswalk.tsv", "decisions.tsv", "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_blk: iout.local}, {ev: iev.local}],
        # `decisions.tsv` by name, not "the directory is non-empty": a lane that
        # resolved nothing writes a legitimately EMPTY crosswalk, and the file that
        # distinguishes that from a crash is the one listing what it refused.
        success=(all((iout.local / f).exists() and (iout.local / f).stat().st_size > 0
                     for f in want)
                 and (iev.local / "blockers").is_dir()
                 and any((iev.local / "blockers").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # The alias index over 3.9 M xref rows and 4.0 M synonym rows is the whole cost;
    # everything after it is dictionary lookups over a few thousand blockers. The
    # explode-and-normalise pass is the peak, and it is sized for that rather than for
    # the steady state.
    resources=Resources(cpus=2, memory=Size.GB(32), duration=Duration(hours=2)),
)
