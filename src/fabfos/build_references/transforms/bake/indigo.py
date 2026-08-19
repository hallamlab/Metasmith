from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
universe  = model.AddRequirement(lib.GetType("interm::aam_universe"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
cache     = model.AddRequirement(lib.GetType("fabfos_data::aam_cache"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_indigo"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

SHARDS = 28
TIMEOUT_S = 20

RESOLVE = """
    set -e
    N=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[indigo] expected exactly one release under {metanetx}, found $N" >&2
        exit 1
    fi
    MNX=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d)
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    iun  = context.Input(universe)
    irs  = context.Input(rescue)
    ica  = context.Input(cache)
    ilib = context.Input(bakelib)
    R = irs.container
    iout = context.Output(pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        mkdir -p members

        pids=""
        for i in $(seq 0 {SHARDS - 1}); do
            {py} -m ecspr.bake.aam.indigo_member map \
                --universe {iun.container} \
                --cache-dir {ica.container}/indigo \
                --shard $i/{SHARDS} --timeout {TIMEOUT_S} \
                --sidecar members/indigo_$i.attempted \
                --out members/indigo_$i.tsv &
            pids="$pids $!"
        done
        rc=0
        for p in $pids; do wait $p || rc=1; done
        if [ $rc -ne 0 ]; then
            echo "[indigo] a shard exited non-zero -- most likely the OOM killer." >&2
            echo "   NOT ignored: a bare 'wait' returns 0 whatever the children did, and" >&2
            echo "   that is how a short member reached the fusion once already. The" >&2
            echo "   caches are resumable, so re-running this lane costs only what the" >&2
            echo "   dead shard had not reached." >&2
            exit 1
        fi

        {py} -m ecspr.bake.aam.indigo_member merge \
            --shard-file members/indigo_*.tsv --expect {SHARDS} \
            --out members/indigo.tsv
        # ONE EXTRACTION OVER THREE SUBMISSION CLASSES -- see bake/rxnmapper.py for
        # why the rescue's three tables ride along and why they are no-ops for a whole
        # reaction.
        {py} -m ecspr.bake.atom_pairs extract \
            --aam members/indigo.tsv --align strict --fallback-forced \
            --universe {iun.container} \
            --resolved {R}/crosswalk.tsv --placeholders {R}/placeholders.tsv \
            --balance {R}/balance.tsv \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container} --out-status members/indigo_status.tsv

        # The SIDECARS are evidence in their own right: they say what was ATTEMPTED, and
        # subtracting them from the caches names the reactions Indigo hung on -- which
        # nothing else records, because a hang writes no row.
        {py} -m ecspr.bake.evidence collect --root _ev --tool indigo \
            --file members/indigo.tsv members/indigo_*.tsv \
                   members/indigo_*.attempted members/indigo_status.tsv \
                   {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{pairs: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "indigo").is_dir()
                 and any((iev.local / "indigo").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=SHARDS, memory=Size.GB(24), duration=Duration(hours=12)),
)
