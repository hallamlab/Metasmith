"""The direction assembly: calibrate the curated bins, fuse the members, encode the ratios.

WHAT IS HERE AND WHAT IS NOT. The two thermodynamic members are lanes of their own
(`equilibrator.py`, `dgbyg.py`); the curated member is read here, from the drop-in, the
same way `aam_ensemble` reads the other .dat. It runs in the eQuilibrator image for a
reason that is not obvious: `dir_calibrate` imports `dir_thermo_eq` at module level, so
the calibration needs that env even though it no longer instantiates the member.

THE CURATED CALL IS THE LOAD-BEARING STEP, not a formality. REACTION-DIRECTION is stated
in MetaCyc's equation orientation and MNXref re-canonicalises orientation on import, so a
naive metacyc->MNXR join INVERTS the curated call on ~60% of reactions. `dir_curated`
re-expresses every call by comparing compound sets, and records the undecidable ones
rather than guessing.

CALIBRATION READS THE MEMBER TABLE rather than re-instantiating the member and re-scoring
every curated reaction. That was about half this lane's wall clock, spent recomputing
numbers the member had already written down.

IT CALIBRATES ON THE MEASURED ARM ONLY. eQuilibrator's group-contribution arm returns
identically zero for group-conserving chemistry, which is exactly what dominates the
REVERSIBLE bin -- including it manufactures a fictitiously tight zero-centred bin and
then reports high confidence in it.

AN ABSENT MEMBER IS A MISSING VOTE HERE, and this is the one place in the lane where that
is true. Each member lane refuses if its own tool is unavailable, because a lane's only
product is its member table. The combiner is different: it is defined over whatever
members spoke, and `dir_combine` already treats an empty table as silence. No evidence
shrinks toward dG'=0, giving ratio 1.0 -- reversible as a LIMIT rather than as an
if-branch -- so a reaction the ensemble is silent on is a provable no-op.

THE TWO THERMODYNAMIC MEMBERS ARE CORRELATED, both fitted on TECRDB, so their agreement
is discounted by a shared-error floor rather than counted twice. MetaCyc is the only
independent member, which is why losing the licensed drop-in does not shrink this
ensemble evenly -- it removes the only thing that can break a tie between two members
that were always going to agree.

IT STOPS AT THE UNCODED TABLE. This step used to encode its own output, which bought one
fewer step at the price of requiring `ref::metabolism_vocab` -- and so of waiting on the
whole atom-mapping branch. The vocabulary was never an input to the science: the encoder
touches exactly one of its five spaces, `rxn`, and that space is `sorted(universe)` read
off MetaNetX `reac_prop`, not anything a mapper produces. So the edge was real but sat in
the wrong place, wrapping a few minutes of encoding around a multi-hour assembly. It moves
to `direction_bake.py`, and this lane runs the moment the two members are in. The identity
guarantee is unaffected: it was always enforced inside `bake_metabolism`, which reads the
block off the vocabulary and has no path that could mint a second one.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::equilibrator.env"))
metanetx   = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))

metacyc    = model.AddRequirement(lib.GetType("fabfos_data::metacyc"))
member_eq  = model.AddRequirement(lib.GetType("interm::direction_member_eq"))
member_db  = model.AddRequirement(lib.GetType("interm::direction_member_dgbyg"))

driver     = model.AddRequirement(lib.GetType("buildlib::dir_drive.py"))
curated_m  = model.AddRequirement(lib.GetType("buildlib::dir_curated.py"))
flatfile   = model.AddRequirement(lib.GetType("buildlib::dir_metacyc_flatfile.py"))
calibrate  = model.AddRequirement(lib.GetType("buildlib::dir_calibrate.py"))
combiner   = model.AddRequirement(lib.GetType("buildlib::dir_combine.py"))
thermo_eq  = model.AddRequirement(lib.GetType("buildlib::dir_thermo_eq.py"))
refdata    = model.AddRequirement(lib.GetType("buildlib::dir_refdata.py"))
canon_m    = model.AddRequirement(lib.GetType("buildlib::dir_canon.py"))
# Not used by this lane, imported by dir_drive at module scope for the dGbyG lane's
# fan-out. Staged here because a driver that cannot import is a lane that cannot start.
sharder    = model.AddRequirement(lib.GetType("buildlib::aam_shard.py"))
evidence   = model.AddRequirement(lib.GetType("buildlib::build_evidence.py"))

annot      = model.AddProduct(lib.GetType("interm::direction_annotation"))
# ONE evidence product, holding `<tool>/<version>/` for each tool this step ran --
# `metacyc_direction/` and `direction_calibration/`. The annotation goes in the second of
# those AS WELL AS being a product: the product path is content-addressed, and
# `check_references.py` looks for the string table its equivalence check needs by the
# literal name `direction_annotation.parquet`.
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
    echo "[direction] metanetx $(basename $MNX) · metacyc $MCVER"
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    imc  = context.Input(metacyc)
    ieq  = context.Input(member_eq)
    idb  = context.Input(member_db)
    ilib = context.Input(combiner)
    iout = context.Output(annot)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, metacyc=imc.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        # The base list, recomputed from the same release the members were asked about.
        {py} {libdir}/dir_drive.py universe --reac-prop $MNX/reac_prop.tsv \
            --out _universe.json

        # The curated member. Per-reaction AND per-MNXR are both kept, because the
        # orientation alignment between them IS the claim: a per-MNXR table alone cannot
        # be checked against what MetaCyc actually said.
        {py} {libdir}/dir_curated.py \
            --metacyc-reactions $MC/{CURATED_DAT} \
            --reac-xref $MNX/reac_xref.tsv \
            --reac-prop $MNX/reac_prop.tsv \
            --chem-xref $MNX/chem_xref.tsv \
            --out _curated_per_mnxr.parquet \
            --out-per-reaction _curated_per_reaction.parquet
        {py} {libdir}/build_evidence.py collect --root _ev --tool metacyc_direction \
            --version $MCVER \
            --file _curated_per_mnxr.parquet _curated_per_reaction.parquet

        {py} {libdir}/dir_calibrate.py \
            --curated _curated_per_mnxr.parquet \
            --reac-prop $MNX/reac_prop.tsv \
            --eq-member {ieq.container} \
            --out-calibration _calibration.parquet \
            --out-points _calibration_points.parquet

        # Written under its own NAME first, then copied to the product path. Both are
        # needed and for different reasons: the product path is content-addressed, and an
        # evidence directory holding a content-addressed filename is one nobody can read,
        # while `check_references.py` looks for this table by this literal name.
        {py} {libdir}/dir_combine.py \
            --base-mnxrs _universe.json \
            --eq {ieq.container} \
            --dgbyg {idb.container} \
            --curated _curated_per_mnxr.parquet \
            --calibration _calibration.parquet \
            --out direction_annotation.parquet
        cp direction_annotation.parquet {iout.container}

        # The calibration POINTS, not just the fitted bins: the fit is a claim about the
        # curated bins, and a claim whose points are gone cannot be re-examined when a
        # bin looks wrong.
        {py} {libdir}/build_evidence.py collect --root _ev --tool direction_calibration \
            --file _calibration.parquet _calibration_points.parquet \
                   direction_annotation.parquet
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    # Evidence is a HARD condition here as in every lane, and both tools are named rather
    # than counted: this step runs two, and one silently failing to collect is exactly the
    # case a count would pass.
    kept = [iev.local / t for t in ("metacyc_direction", "direction_calibration")]
    return ExecutionResult(
        manifest=[{annot: iout.local}, {ev: iev.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0
                and all(d.is_dir() and any(d.iterdir()) for d in kept),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # Table arithmetic over the universe, plus one parse of reac_prop. Minutes.
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=2)),
)
