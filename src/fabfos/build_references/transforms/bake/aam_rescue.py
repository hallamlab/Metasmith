"""The rescue: propose a structure for every blocker, then complete the reaction.

BETWEEN THE TWO MAPPER PASSES, and that position is the whole change. This work used to
run inside `aam_ensemble`, after every member had finished, with Indigo mapping the
completed reactions on the spot. The consequence is visible in the deployed table: all
9,089 of its rescue-derived reactions are `mcs_only` -- Indigo alone, at half weight --
because the crosswalk did not exist when the neural members ran and nothing ever showed
them a completed reaction. 14.5% of the table, uncorroborated, because of an ordering.

Here the completion is its own step and produces a UNIVERSE: `rescued.parquet`, in the
same schema the mapper lanes read. Pass 2 runs all three members over it, and what they
agree on becomes full-weight consensus instead of a single member's assertion. This is
the one place the build is meant to BEAT the table it reproduces rather than match it.

NO MAPPER RUNS HERE. Everything this step decides is arithmetic:

  * WHICH STRUCTURE each blocker gets, from nine proposer lanes, merged by a fixed
    priority so one metabolite is claimed by exactly one argument.
  * WHETHER THE `*` BODIES CANCEL across the equation. A curated carrier draws its body
    as `*` and counts it as zero for every element; that is only safe when the same body
    stands on both sides.
  * WHETHER THE CONCRETE ATOMS BALANCE, per element. This is where the conservation claim
    is actually tested, and it needs no atom map -- only formulas and the curated
    structures. A reaction where no element balances is not passed on at all, because the
    extractor would drop every pair it produced.

`propose` DRY-RUNS THE ADMISSION CHECK before `complete` starts, because `admit` aborts
on the first bad row and the previous generation learned that by losing a mapping pass to
a stale id in row 4,000.

WHAT IT REFUSES TO PRODUCE. A rescue that completes zero reactions exits non-zero rather
than writing an empty universe. Pass 2 over nothing is a green build that has silently
lost layer 3.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
chebi       = model.AddRequirement(lib.GetType("fabfos_data::chebi"))
modelseed   = model.AddRequirement(lib.GetType("fabfos_data::modelseed"))

worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
metabolites = model.AddRequirement(lib.GetType("lookup::metabolites"))
atom_ranks  = model.AddRequirement(lib.GetType("lookup::atom_ranks"))
xrefs       = model.AddRequirement(lib.GetType("lookup::xrefs"))
synonyms    = model.AddRequirement(lib.GetType("lookup::synonyms"))

curation    = model.AddRequirement(lib.GetType("buildlib::aam_curation.py"))
worklib     = model.AddRequirement(lib.GetType("buildlib::aam_worklist.py"))
evidence    = model.AddRequirement(lib.GetType("buildlib::build_evidence.py"))

out_rescue  = model.AddProduct(lib.GetType("interm::aam_rescue"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    ich  = context.Input(chebi)
    ims  = context.Input(modelseed)
    iwl  = context.Input(worklist)
    irx  = context.Input(reactions)
    imt  = context.Input(metabolites)
    iar  = context.Input(atom_ranks)
    ixr  = context.Input(xrefs)
    isy  = context.Input(synonyms)
    ilib = context.Input(curation)
    iout = context.Output(out_rescue)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    # The lanes read the five lookups as a DIRECTORY. Their content-addressed staging
    # paths are not siblings, so they are re-presented under one name here; five separate
    # arguments would be five chances for one of them to come from a different build.
    stage_lookups = f"""
        mkdir -p _lookups
        ln -sfn {irx.container} _lookups/reactions.parquet
        ln -sfn {imt.container} _lookups/metabolites.parquet
        ln -sfn {iar.container} _lookups/atom_ranks.parquet
        ln -sfn {ixr.container} _lookups/xrefs.parquet
        ln -sfn {isy.container} _lookups/synonyms.parquet
    """

    cmd = f"""
        set -e
        {stage_lookups}
        mkdir -p rescue

        {py} {libdir}/aam_curation.py propose --lookups _lookups \
            --worklist {iwl.container} \
            --chebi {ich.container} --modelseed {ims.container} \
            --out rescue/crosswalk.tsv

        {py} {libdir}/aam_curation.py complete --lookups _lookups \
            --worklist {iwl.container} \
            --crosswalk rescue/crosswalk.tsv \
            --out rescue/rescued.parquet \
            --out-balance rescue/balance.tsv \
            --out-placeholders rescue/placeholders.tsv

        # THE FOUR FILES TRAVEL TOGETHER as one product. The extractor needs the
        # placeholder list to suppress scaffolding atoms and the balance table to drop
        # unbalanced elements; handed one build's crosswalk and another's placeholders it
        # would suppress the wrong atoms and say nothing about it.
        mkdir -p {iout.container}
        cp rescue/crosswalk.tsv rescue/placeholders.tsv rescue/balance.tsv \
           rescue/rescued.parquet {iout.container}/

        # The CROSSWALK is the curation: the only place the assertions are written down,
        # each with the basis that warrants it and the lane that made it. Nothing else in
        # the build records what was CLAIMED, as against what survived the gates.
        {py} {libdir}/build_evidence.py collect --root _ev --tool rescue \
            --file rescue/crosswalk.tsv rescue/placeholders.tsv rescue/balance.tsv \
                   rescue/rescued.parquet
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    want = ["crosswalk.tsv", "placeholders.tsv", "balance.tsv", "rescued.parquet"]
    return ExecutionResult(
        manifest=[{out_rescue: iout.local}, {ev: iev.local}],
        # ALL FOUR BY NAME. "The directory is non-empty" would pass a rescue that wrote a
        # crosswalk and no balance table, which is the case that silently un-gates every
        # rescued element downstream.
        success=(all((iout.local / f).exists() and (iout.local / f).stat().st_size > 0
                     for f in want)
                 and (iev.local / "rescue").is_dir()
                 and any((iev.local / "rescue").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # The synonym index and the ChEBI/ModelSeed structure tables are the memory here;
    # the lanes themselves are name parsing and arithmetic. No mapper, so no long tail.
    # MEASURED over the full 24,098-reaction blocked set: propose 52 s at 3.4 GB, complete
    # 33 s at 2.5 GB. The request is sized to that rather than to caution, because this
    # step sits between the two mapper passes -- every minute it spends queueing is a
    # minute pass 2 has not started, and a 48 GB ask queues behind a 24 GB one for nothing.
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=2)),
)
