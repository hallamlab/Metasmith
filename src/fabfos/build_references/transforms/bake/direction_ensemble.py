from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image      = model.AddRequirement(lib.GetType("env::equilibrator.env"))
metanetx   = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))

metacyc    = model.AddRequirement(lib.GetType("fabfos_data::metacyc"))
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
    echo "[direction] metanetx $(basename $MNX) · metacyc $MCVER"
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    imc  = context.Input(metacyc)
    ieq  = context.Input(member_eq)
    idb  = context.Input(member_db)
    ilib = context.Input(bakelib)
    iout = context.Output(annot)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, metacyc=imc.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        # The base list, recomputed from the same release the members were asked about.
        {py} -m ecspr.bake.direction.drive universe --reac-prop $MNX/reac_prop.tsv \
            --out _universe.json

        # The curated member. Per-reaction AND per-MNXR are both kept, because the
        # orientation alignment between them IS the claim: a per-MNXR table alone cannot
        # be checked against what MetaCyc actually said.
        {py} -m ecspr.bake.direction.curated \
            --metacyc-reactions $MC/{CURATED_DAT} \
            --reac-xref $MNX/reac_xref.tsv \
            --reac-prop $MNX/reac_prop.tsv \
            --chem-xref $MNX/chem_xref.tsv \
            --out _curated_per_mnxr.parquet \
            --out-per-reaction _curated_per_reaction.parquet
        {py} -m ecspr.bake.evidence collect --root _ev --tool metacyc_direction \
            --version $MCVER \
            --file _curated_per_mnxr.parquet _curated_per_reaction.parquet

        {py} -m ecspr.bake.direction.calibrate \
            --curated _curated_per_mnxr.parquet \
            --reac-prop $MNX/reac_prop.tsv \
            --eq-member {ieq.container} \
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
            --out direction_annotation.parquet
        cp direction_annotation.parquet {iout.container}

        # The calibration POINTS, not just the fitted bins: the fit is a claim about the
        # curated bins, and a claim whose points are gone cannot be re-examined when a
        # bin looks wrong.
        {py} -m ecspr.bake.evidence collect --root _ev --tool direction_calibration \
            --file _calibration.parquet _calibration_points.parquet \
                   direction_annotation.parquet
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

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
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=2)),
)
