"""Direction member lane: eQuilibrator -- component-contribution over the pinned cache.

One of the two thermodynamic members, and the expensive one: loading the 1.3 GB compound
cache and running component-contribution over the reaction universe is most of this
ensemble's wall clock. It is its own step for the same reason LocalMapper is -- a change
to the calibration or the combiner must not re-pay it.

`XDG_CACHE_HOME` IS THE ONLY LEVER ON THE CACHE, and this is the fact the lane exists
around. `equilibrator_cache.zenodo.get_cached_filepath` resolves through
`pooch.os_cache("equilibrator")` -- i.e. `$XDG_CACHE_HOME/equilibrator/<file>` -- and
reads nothing else. `EQUILIBRATOR_CACHE_DIR` is read by no part of pooch,
equilibrator_cache or component_contribution: it was exported for a generation and was a
no-op the whole time, and what actually made that build work was that the acquisition
happened to leave the package's own nested layout in place. Now that
`acquire/equilibrator.py` lifts the two artifacts into `equilibrator/<version>/`, the
layout pooch expects is rebuilt here BY SYMLINK -- compounds.sqlite is 1.3 GB and a copy
per member is 1.3 GB of nothing.

Pooch re-checks its embedded md5 for each file before using it, which is the same claim
`zenodo.md5` records in the acquired product. So a wrong or truncated staged cache is
caught here rather than showing up as strange free energies.

THE UNIVERSE IS RECOMPUTED, NOT PASSED. `dir_drive universe` is a parse of one MetaNetX
release's reac_prop, and the release is asserted single at the top of every lane -- so
the two members and the combiner derive the same list from the same bytes with the same
code. Making it a product instead would put a shared node between two members that have
nothing else to say to each other, and would serialise them behind it.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::equilibrator.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
equilib   = model.AddRequirement(lib.GetType("fabfos_data::equilibrator"))

driver    = model.AddRequirement(lib.GetType("buildlib::dir_drive.py"))
member_eq = model.AddRequirement(lib.GetType("buildlib::dir_thermo_eq.py"))
refdata   = model.AddRequirement(lib.GetType("buildlib::dir_refdata.py"))
canon_m   = model.AddRequirement(lib.GetType("buildlib::dir_canon.py"))
# Not used by this lane, imported by dir_drive at module scope for the dGbyG lane's
# fan-out. Staged here because a driver that cannot import is a lane that cannot start.
sharder   = model.AddRequirement(lib.GetType("buildlib::aam_shard.py"))
evidence  = model.AddRequirement(lib.GetType("buildlib::build_evidence.py"))

out_eq    = model.AddProduct(lib.GetType("interm::direction_member_eq"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

CACHE_LINK = "_eqcache"

RESOLVE = f"""
    set -e
    N=$(find {{metanetx}} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[eq] expected exactly one release under {{metanetx}}, found $N" >&2
        exit 1
    fi
    MNX=$(find {{metanetx}} -mindepth 1 -maxdepth 1 -type d)

    N=$(find {{equilibrator}} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[eq] expected exactly one cache version under {{equilibrator}}, found $N." \\
             'compounds.sqlite and cc_params.npz are FITTED TOGETHER; mixing versions' \\
             'silently changes what dG the measured arm reports' >&2
        exit 1
    fi
    EQ=$(find {{equilibrator}} -mindepth 1 -maxdepth 1 -type d)

    mkdir -p {CACHE_LINK}/equilibrator
    for f in compounds.sqlite cc_params.npz; do
        if [ ! -s "$EQ/$f" ]; then
            echo "[eq] the pinned cache at $EQ is missing $f" >&2
            exit 1
        fi
        ln -sfn $EQ/$f {CACHE_LINK}/equilibrator/$f
    done
    export XDG_CACHE_HOME=$(pwd -P)/{CACHE_LINK}
    echo "[eq] metanetx $(basename $MNX) · cache $(basename $EQ)"
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ieq  = context.Input(equilib)
    ilib = context.Input(driver)
    iout = context.Output(out_eq)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, equilibrator=ieq.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        {py} {libdir}/dir_drive.py universe --reac-prop $MNX/reac_prop.tsv \
            --out _universe.json
        # --require: this lane's ONLY product is the member table, so an unavailable
        # member is a failed step, not a missing vote. The tolerant path belongs to the
        # combiner, which is where "one member is absent" is a legitimate state.
        {py} {libdir}/dir_drive.py eval --member eq --require \
            --universe _universe.json \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container}

        {py} {libdir}/build_evidence.py collect --root _ev --tool equilibrator \
            --file _universe.json {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_eq: iout.local}, {ev: iev.local}],
        # THE RAW OUTPUT IS PART OF THE RESULT, not a diagnostic nicety. This used to
        # pass on the table alone, arguing that an evidence directory lost after a
        # twelve-hour run was not worth failing over. It is: the copy happens seconds
        # after the tool finished, in the same command, so an absence is not the lane
        # being busy -- it is something going wrong that a green lane would hide, and the
        # tool's own output is the only record of what it actually said.
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "equilibrator").is_dir()
                 and any((iev.local / "equilibrator").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # The cache load dominates the memory; the scan over the universe dominates the time.
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=12)),
)
