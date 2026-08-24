# The direction assembly: calibrate the curated bins, fuse the members, encode the ratios.
#
# WHAT IS HERE AND WHAT IS NOT. The two thermodynamic members are lanes of their own
# (`equilibrator.py`, `dgbyg.py`); the curated member is read here, from the drop-in, the
# same way `aam_ensemble` reads the other .dat.
#
# IT INSTANTIATES NO MEMBER. `dir_calibrate` imports `dir_thermo_eq` at module level, but
# that module imports `equilibrator_api` inside `EquilibratorMember.__init__`, and the
# calibration reads the member table rather than instantiating it. `dir_quotient annotate`
# does need rdkit, because the substitution table it restages through admits its models by
# parsing SMILES -- rdkit is in this image, which is the other reason the lane is grouped by
# it.
#
# WHAT THAT BUYS IS AN OFFLINE RE-FIT. `curated`, `calibrate` and `combine` are pure pandas
# over artifacts already on disk, so `benchmarks/direction_rescue/reassemble.py` re-runs them
# in seconds and reproduces the deployed annotation frame-for-frame, taking the correction
# table as an input rather than recomputing it. That is what makes the arms of a re-bake --
# balance gate, prior width, quotient, prior scale, constants -- separable offline instead of
# confounded in one run.
#
# THE CURATED CALL IS THE LOAD-BEARING STEP, not a formality. REACTION-DIRECTION is stated
# in MetaCyc's equation orientation and MNXref re-canonicalises orientation on import, so a
# naive metacyc->MNXR join INVERTS the curated call on ~60% of reactions. `dir_curated`
# re-expresses every call by comparing compound sets, and records the undecidable ones
# rather than guessing.
#
# CALIBRATION READS THE MEMBER TABLE rather than re-instantiating the member and re-scoring
# every curated reaction. That was about half this lane's wall clock, spent recomputing
# numbers the member had already written down.
#
# IT CALIBRATES ON THE MEASURED ARM ONLY. eQuilibrator's group-contribution arm returns
# identically zero for group-conserving chemistry, which is exactly what dominates the
# REVERSIBLE bin -- including it manufactures a fictitiously tight zero-centred bin and
# then reports high confidence in it.
#
# AN ABSENT MEMBER IS A MISSING VOTE HERE, and this is the one place in the lane where that
# is true. Each member lane refuses if its own tool is unavailable, because a lane's only
# product is its member table. The combiner is different: it is defined over whatever
# members spoke, and `dir_combine` already treats an empty table as silence. No evidence
# shrinks toward dG'=0, giving ratio 1.0 -- reversible as a LIMIT rather than as an
# if-branch -- so a reaction the ensemble is silent on is a provable no-op.
#
# THE TWO THERMODYNAMIC MEMBERS ARE CORRELATED, both fitted on TECRDB, so their agreement
# is discounted by a shared-error floor rather than counted twice. MetaCyc is the only
# independent member, which is why losing the licensed drop-in does not shrink this
# ensemble evenly -- it removes the only thing that can break a tie between two members
# that were always going to agree.
#
# IT STOPS AT THE UNCODED TABLE. This step used to encode its own output, which bought one
# fewer step at the price of requiring `ref::metabolism_vocab` -- and so of waiting on the
# whole atom-mapping branch. The vocabulary was never an input to the science: the encoder
# touches exactly one of its five spaces, `rxn`, and that space is `sorted(universe)` read
# off MetaNetX `reac_prop`, not anything a mapper produces. So the edge was real but sat in
# the wrong place, wrapping a few minutes of encoding around a multi-hour assembly. It moves
# to `direction_bake.py`, and this lane runs the moment the two members are in. The identity
# guarantee is unaffected: it was always enforced inside `bake_metabolism`, which reads the
# block off the vocabulary and has no path that could mint a second one.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::equilibrator.env"))
metanetx   = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))

metacyc    = model.AddRequirement(lib.GetType("fabfos_data::metacyc"))
ecmdb      = model.AddRequirement(lib.GetType("fabfos_data::ecmdb"))
bionumbers = model.AddRequirement(lib.GetType("fabfos_data::bionumbers"))
member_eq  = model.AddRequirement(lib.GetType("interm::direction_member_eq"))
member_db  = model.AddRequirement(lib.GetType("interm::direction_member_dgbyg"))

bakelib    = model.AddRequirement(lib.GetType("buildlib::ecspr"))

annot      = model.AddProduct(lib.GetType("interm::direction_annotation"))
ev         = model.AddProduct(lib.GetType("evidence::tool_output"))

CURATED_DAT = "reactions.dat"

RESOLVE = f"""
    set -e
    N=$(find {{metanetx}} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[direction] expected exactly one release under {{metanetx}}, found $N." \\
             'The orientation alignment compares compound sets across reac_xref and' \\
             'chem_xref, so two snapshots would be compared against each other' >&2
        exit 1
    fi
    MNX=$(find {{metanetx}} -mindepth 1 -maxdepth 1 -type d)

    N=$(find {{metacyc}} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[direction] expected exactly one MetaCyc release under {{metacyc}}, found $N." \\
             'Which release the references are built from is a provenance decision, and' \\
             'this is the ONE input with no external record of that decision' >&2
        exit 1
    fi
    MCREL=$(find {{metacyc}} -mindepth 1 -maxdepth 1 -type d)
    MCVER=$(basename $MCREL)
    # Both drop-in shapes are legitimate: a distribution unpacked as downloaded keeps
    # `<release>/data/`, a hand-flattened one puts the .dat files at the top.
    if [ -s "$MCREL/data/{CURATED_DAT}" ]; then
        MC=$MCREL/data
    elif [ -s "$MCREL/{CURATED_DAT}" ]; then
        MC=$MCREL
    else
        echo "[direction] no {CURATED_DAT} under $MCREL (looked in ./ and ./data/)." \\
             'MetaCyc flat-files are LICENSED and not redistributable, so nothing' \\
             'fetches this -- place the distribution under' \\
             'data/fabfos/originals/metacyc/<release>/. Without it this ensemble keeps only' \\
             'its two CORRELATED members and has nothing that can break a tie' >&2
        exit 1
    fi
    for pair in "ecmdb {ecmdb}" "bionumbers {bionumbers}"; do
        set -- $pair
        N=$(find $2 -mindepth 1 -maxdepth 1 -type d | wc -l)
        if [ "$N" -ne 1 ]; then
            echo "[direction] expected exactly one $1 release under $2, found $N." \\
                 'A concentration is only interpretable against the release it was' \\
                 'measured in, and two would be aggregated against each other' >&2
            exit 1
        fi
    done
    ECMDB=$(find {ecmdb} -mindepth 1 -maxdepth 1 -type d)
    BIONUMBERS=$(find {bionumbers} -mindepth 1 -maxdepth 1 -type d)

    echo "[direction] metanetx $(basename $MNX) · metacyc $MCVER ·" \\
         "ecmdb $(basename $ECMDB) · bionumbers $(basename $BIONUMBERS)"
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    imc  = context.Input(metacyc)
    iec  = context.Input(ecmdb)
    ibn  = context.Input(bionumbers)
    ieq  = context.Input(member_eq)
    idb  = context.Input(member_db)
    ilib = context.Input(bakelib)
    iout = context.Output(annot)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, metacyc=imc.container,
                             ecmdb=iec.container, bionumbers=ibn.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        # The calibration and the combiner ARE the direction subpackage, and neither the
        # fallback hash (`bake/*.py`) nor any pinned package moves when that subpackage
        # does -- so the version is computed from it. The curated member keeps $MCVER
        # instead: what identifies that table is which MetaCyc release it read.
        DIRVER=$({py} -m ecspr.bake.evidence fingerprint --package direction)
        echo "[direction] method $DIRVER"

        # The base list, recomputed from the same release the members were asked about.
        {py} -m ecspr.bake.direction.drive universe --reac-prop $MNX/reac_prop.tsv \
            --out _universe.json

        # The curated member. Per-reaction AND per-MNXR are both kept, because the
        # orientation alignment between them IS the claim: a per-MNXR table alone cannot
        # be checked against what MetaCyc actually said.
        #
        # `--supplementary-crosswalk` reaches the 545 directed MetaCyc reactions
        # reac_xref never joined -- overwhelmingly generic-polymer chemistry, which is
        # the same MetaNetX weakness the substitution lane exists for. It is additive
        # only, so it cannot move a call the primary join already made; the OFF
        # configuration remains what reproduces r8.
        {py} -m ecspr.bake.direction.curated \
            --metacyc-reactions $MC/{CURATED_DAT} \
            --reac-xref $MNX/reac_xref.tsv \
            --reac-prop $MNX/reac_prop.tsv \
            --chem-xref $MNX/chem_xref.tsv \
            --supplementary-crosswalk \
            --out _curated_per_mnxr.parquet \
            --out-per-reaction _curated_per_reaction.parquet
        {py} -m ecspr.bake.evidence collect --root _ev --tool metacyc_direction \
            --version $MCVER \
            --file _curated_per_mnxr.parquet _curated_per_reaction.parquet

        # THE REACTION QUOTIENT. One concentration and one width per metabolite from the
        # pinned sources, then one correction per (MNXR, member) -- per member, because the
        # substitution lane restages polymer and carrier chemistry and a row refused for one
        # member and kept for the other does not have one equation between them.
        {py} -m ecspr.bake.direction.quotient table \
            --source ecmdb bionumbers \
            --chunk ecmdb=$ECMDB bionumbers=$BIONUMBERS \
            --chem-xref $MNX/chem_xref.tsv \
            --chem-prop $MNX/chem_prop.tsv \
            --out _concentrations.tsv
        {py} -m ecspr.bake.direction.quotient annotate \
            --table _concentrations.tsv \
            --reac-prop $MNX/reac_prop.tsv \
            --chem-prop $MNX/chem_prop.tsv \
            --substitutions {libdir}/ecspr/bake/direction \
            --out _correction.parquet
        {py} -m ecspr.bake.evidence collect --root _ev --tool direction_quotient \
            --version $DIRVER \
            --file _concentrations.tsv _correction.parquet

        # --quotient HERE TOO, not only in the combiner. The prior is averaged with the
        # thermo vote, so it has to be fitted on the number the thermo vote carries.
        {py} -m ecspr.bake.direction.calibrate \
            --curated _curated_per_mnxr.parquet \
            --reac-prop $MNX/reac_prop.tsv \
            --eq-member {ieq.container} \
            --quotient _correction.parquet \
            --out-calibration _calibration.parquet \
            --out-points _calibration_points.parquet

        # Written under its own NAME first, then copied to the product path. Both are
        # needed and for different reasons: the product path is content-addressed, and an
        # evidence directory holding a content-addressed filename is one nobody can read,
        # while `check_references.py` looks for this table by this literal name.
        {py} -m ecspr.bake.direction.combine \
            --base-mnxrs _universe.json \
            --eq {ieq.container} \
            --dgbyg {idb.container} \
            --curated _curated_per_mnxr.parquet \
            --calibration _calibration.parquet \
            --quotient _correction.parquet \
            --out direction_annotation.parquet
        cp direction_annotation.parquet {iout.container}

        # The calibration POINTS, not just the fitted bins: the fit is a claim about the
        # curated bins, and a claim whose points are gone cannot be re-examined when a
        # bin looks wrong.
        {py} -m ecspr.bake.evidence collect --root _ev --tool direction_calibration \
            --version $DIRVER \
            --file _calibration.parquet _calibration_points.parquet \
                   direction_annotation.parquet
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    kept = [iev.local / t for t in ("metacyc_direction", "direction_quotient",
                                    "direction_calibration")]
    return ExecutionResult(
        manifest=[{annot: iout.local}, {ev: iev.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0
                and all(d.is_dir() and any(d.iterdir()) for d in kept),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # MEASURED on the pinned inputs, command by command: curated 9.4 s / 224 MB,
    # `quotient table` 48.5 s / 3.8 GB, `quotient annotate` 14.2 s / 2.2 GB,
    # calibrate 3.6 s / 252 MB, combine 2.3 s / 354 MB. Under a minute and a half,
    # single-threaded because the lane sets OMP_NUM_THREADS=1.
    #
    # THE MEMORY IS THE QUOTIENT'S CROSSWALK. `quotient table` reads all 678 MB of
    # chem_xref and builds a name index over it, which is the whole peak -- everything
    # else in the lane runs in a third of a gigabyte. 8 GB leaves headroom over the
    # measured 3.8; the previous 4 would not have.
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(minutes=30)),
)
