from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::dgbyg.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))

bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_db    = model.AddProduct(lib.GetType("interm::direction_member_dgbyg"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

SHARDS = 20

RESOLVE = """
    set -e
    N=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[dgbyg] expected exactly one release under {metanetx}, found $N" >&2
        exit 1
    fi
    MNX=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d)
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ilib = context.Input(bakelib)
    iout = context.Output(out_db)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container)
    py = (f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 "
          f"LD_LIBRARY_PATH=$CONDA_PREFIX/lib:$LD_LIBRARY_PATH python3")

    cmd = f"""
        {resolve}
        # Recomputed rather than passed -- see equilibrator.py. Same release, same code,
        # same list; a shared node between two independent members would only serialise
        # them.
        {py} -m ecspr.bake.direction.drive universe --reac-prop $MNX/reac_prop.tsv \
            --out _universe.json
        mkdir -p members

        # --require, for the same reason as the eQuilibrator lane: the member table is
        # this step's only product, so an unimportable member is a failure here. It
        # remains a missing vote at the combiner, which is the right place for that.
        # Written without `${{...}}`: this is an f-string, and a brace that survives into
        # the shell fails at the end of the lane rather than at import.
        pids=""
        for i in $(seq 0 {SHARDS - 1}); do
            {py} -m ecspr.bake.direction.drive eval --member dgbyg --require \
                --universe _universe.json --shard $i/{SHARDS} \
                --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
                --out members/dgbyg_$i.parquet &
            pids="$pids $!"
        done
        rc=0
        for p in $pids; do wait $p || rc=1; done
        if [ $rc -ne 0 ]; then
            echo "[dgbyg] a shard exited non-zero." >&2
            echo "   NOT ignored: a bare 'wait' returns 0 whatever the children did, and" >&2
            echo "   that is how a short member reached the fusion once already." >&2
            exit 1
        fi

        # The merge is this lane's completeness proof, which is why it is handed the
        # universe rather than just the tables: eight files that parse is not the same
        # claim as eight files that add up to what was asked about.
        {py} -m ecspr.bake.direction.drive merge --member dgbyg \
            --shard-file members/dgbyg_*.parquet --expect {SHARDS} \
            --universe _universe.json --out {iout.container}

        # The REASON column is the diagnostic value of this member: an abstention on
        # `wildcard` is the guard working, one on `unbalanced` is a statement about the
        # equation rather than about the model, and the two must stay distinguishable.
        # PER-SHARD tables as well as the merged one -- the reasons are what separate the
        # guard firing from the model failing, and a shard that abstained on everything is
        # only visible before the concatenation.
        {py} -m ecspr.bake.evidence collect --root _ev --tool dgbyg \
            --file _universe.json members/dgbyg_*.parquet {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_db: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "dgbyg").is_dir()
                 and any((iev.local / "dgbyg").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=SHARDS, memory=Size.GB(56), duration=Duration(hours=2)),
)
