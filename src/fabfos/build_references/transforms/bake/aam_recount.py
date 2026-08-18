"""Recount every metabolite's C/N/S/P from its structure, once, for the whole graph.

THE LEVER THIS IS. The largest single population of unmappable reactions is not blocked
by missing chemistry -- it is blocked by a formula that declines to state a count.
`C70H131N3O9PS*2` carries a residue of unspecified composition, so `count_element`
returns None and every gate downstream refuses; meanwhile the SMILES in the same MetaNetX
record says C70 N3 P1 S1 in explicit atoms. This step reads the structure instead of the
formula wherever there is one, and publishes the answer as a table.

A LOOKUP, NOT AN INTERMEDIATE, for the reason `lookup.yml` gives: five lanes across two
images need it, and the alternative is each of them re-parsing 1.5 M structures per run to
answer the same question with five chances to answer it differently. It is derived from
`lookup::metabolites` alone, so it is cheap to rebuild without redoing the 810 MB parse
that produced its input -- which is why it is a step of its own rather than a sixth
product of `mnx_lookups`.

`check` IS PART OF THE STEP, not a test. `atom_ranks` already holds one canonical rank per
atom of element X for every structured participant, so `len(ranks)` is this count for
every row the two tables share. Two readings of one structure that disagree would give a
pair row an index into a rank list of the wrong length -- silently, and only for the
metabolites where it matters most. The assertion runs here, before anything joins it.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_counts  = model.AddProduct(lib.GetType("lookup::element_counts"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    ilib = context.Input(bakelib)
    iout = context.Output(out_counts)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p rc
        {py} -m ecspr.bake.aam.recount build \
            --metabolites {imt.container} \
            --out rc/element_counts.parquet --out-summary rc/summary.tsv

        # Before it leaves this step. A recount that disagrees with the node identity
        # contract is not a table to publish and inspect later.
        {py} -m ecspr.bake.aam.recount check \
            --counts rc/element_counts.parquet --atom-ranks {iar.container}

        cp rc/element_counts.parquet {iout.container}

        {py} -m ecspr.bake.evidence collect --root _ev --tool recount \
            --file rc/element_counts.parquet rc/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_counts: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "recount").is_dir()
                 and any((iev.local / "recount").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # One rdkit parse per metabolite over the WHOLE compound table -- 1.5 M of them, not
    # the 32 k that appear in a reaction, because the curation lanes hunt structured twins
    # by name and a twin need not itself be a participant.
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=2)),
)
