from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::equilibrator.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
equilib   = model.AddRequirement(lib.GetType("fabfos_data::equilibrator"))

bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

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
    ilib = context.Input(bakelib)
    iout = context.Output(out_eq)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, equilibrator=ieq.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        {py} -m ecspr.bake.direction.drive universe --reac-prop $MNX/reac_prop.tsv \
            --out _universe.json
        # --require: this lane's ONLY product is the member table, so an unavailable
        # member is a failed step, not a missing vote. The tolerant path belongs to the
        # combiner, which is where "one member is absent" is a legitimate state.
        {py} -m ecspr.bake.direction.drive eval --member eq --require \
            --universe _universe.json \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container}

        {py} -m ecspr.bake.evidence collect --root _ev --tool equilibrator \
            --file _universe.json {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_eq: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "equilibrator").is_dir()
                 and any((iev.local / "equilibrator").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=12)),
)
