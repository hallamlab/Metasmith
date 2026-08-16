"""Pairs conservation forces, for the reactions no member will ever be given.

NO MAPPER RUNS AND NONE COULD. Its target set is the complement of the widest admission
any member makes -- what is left after `INDIGO_ADMITS` and after the rescue's completions
-- so this is not a cheaper route to an answer a mapper would also reach. It is the only
route to those reactions at all.

TWO GRAINS, TWO FILES, AND THE SPLIT IS THE DELIVERABLE. The conjugate arm is exact: a
participant standing on both sides with equal multiplicity contributes the same unknown
amount to each, so it cancels on IDENTITY, and what remains may leave conservation exactly
one possibility. Those are atom-grain pairs and they are banked. The single-unknown arm
and the generic-carrier class settle a number about a MOLECULE -- which is not an atom
correspondence, and a structureless species has no canonical rank to hang one on. They go
in their own table, are reported in the ledger as claims, and are never banked. The
measured 99.7% agreement between this algebra and a mapper is an argument for the first
half and no argument at all for the second.

IT NEEDS THE RECOUNT TO REACH ANYTHING. `count_element` refuses a `*` formula, correctly,
and every arm here is arithmetic over counts -- so without `lookup::element_counts` the
lane abstains on precisely the residue species that make these reactions unreadable in the
first place. The price rides along: the unspecified residue slots have to cancel across
the reaction before any count is treated as a conservation claim.

AND IT NEEDS `lookup::atom_ranks`, because a forced pair is an index into the rank list of
a (metabolite, element). A count that disagrees with the length of that list is refused
rather than emitted -- it would index a real pair row into a list of the wrong length,
silently, for exactly the metabolites where it matters.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
rescue      = model.AddRequirement(lib.GetType("interm::aam_rescue"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))
counts      = model.AddRequirement(lib.GetType("lookup::element_counts"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_alg     = model.AddProduct(lib.GetType("interm::aam_algebra"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irs  = context.Input(rescue)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    iec  = context.Input(counts)
    ilib = context.Input(bakelib)
    iout = context.Output(out_alg)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p _lookups alg
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {iar.container} _lookups/atom_ranks.parquet

        {py} -m ecspr.bake.aam.algebra build --lookups _lookups \
            --element-counts {iec.container} \
            --worklist {iwl.container} \
            --rescued {irs.container}/rescued.parquet \
            --out-forced alg/forced_pairs.parquet \
            --out-claims alg/claims.tsv \
            --out-summary alg/summary.tsv

        mkdir -p {iout.container}
        cp alg/forced_pairs.parquet alg/claims.tsv alg/summary.tsv {iout.container}/

        {py} -m ecspr.bake.evidence collect --root _ev --tool algebra \
            --file alg/claims.tsv alg/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    want = ["forced_pairs.parquet", "claims.tsv", "summary.tsv"]
    return ExecutionResult(
        manifest=[{out_alg: iout.local}, {ev: iev.local}],
        # ALL THREE BY NAME. A run that wrote the pairs and lost the claims table would
        # publish the banked half of a two-grain product with nothing to read it against,
        # which is the one way this lane can mislead.
        success=(all((iout.local / f).exists() and (iout.local / f).stat().st_size > 0
                     for f in want)
                 and (iev.local / "algebra").is_dir()
                 and any((iev.local / "algebra").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # Arithmetic over the reaction table plus the rank index in memory. No mapper, no
    # rdkit parse per reaction -- the counts are read, not derived.
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=1)),
)
