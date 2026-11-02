from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
universe  = model.AddRequirement(lib.GetType("interm::aam_universe"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
cache     = model.AddRequirement(lib.GetType("fabfos_data::aam_cache"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_rxnmapper"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

MEMBER = "rxnmapper"
SHARDS = 4

RESOLVE = """
    set -e
    N=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[{member}] expected exactly one release under {metanetx}, found $N" >&2
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
    iout = context.Output(pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent
    R = irs.container

    resolve = RESOLVE.format(metanetx=imnx.container, member=MEMBER)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        mkdir -p members

        pids=""
        for i in $(seq 0 {SHARDS - 1}); do
            {py} -m ecspr.bake.aam.neural_members --member {MEMBER} \
                --universe {iun.container} \
                --cache-dir {ica.container}/{MEMBER} \
                --shard $i/{SHARDS} \
                --sidecar members/{MEMBER}_$i.attempted \
                --out members/{MEMBER}_$i.tsv &
            pids="$pids $!"
        done
        rc=0
        for p in $pids; do wait $p || rc=1; done
        if [ $rc -ne 0 ]; then
            echo "[{MEMBER}] a shard exited non-zero -- most likely the OOM killer." >&2
            echo "   NOT ignored: a bare 'wait' returns 0 whatever the children did, and" >&2
            echo "   that is how a short member reached the fusion once already. The" >&2
            echo "   caches and sidecars are resumable, so re-running this lane costs" >&2
            echo "   only what the dead shard had not reached." >&2
            exit 1
        fi

        {py} -m ecspr.bake.aam.neural_members --member {MEMBER} \
            --merge-from members/{MEMBER}_*.tsv \
            --out members/{MEMBER}.tsv
        # ONE EXTRACTION OVER THREE SUBMISSION CLASSES, which is what `--universe` buys:
        # a row carrying an `element` gets the reduced participant lists the reduction
        # actually made, and a row without one gets the equation-derived template as
        # always. The rescue's three tables come along for the completed class -- its
        # curated structures are real and kept, its placeholders are scaffolding and
        # suppressed, and its per-element balance is where the conservation claim is
        # tested. All three are no-ops for a whole reaction, whose participants are
        # structured by construction.
        {py} -m ecspr.bake.atom_pairs extract \
            --aam members/{MEMBER}.tsv --align strict --fallback-forced \
            --universe {iun.container} \
            --resolved {R}/crosswalk.tsv --placeholders {R}/placeholders.tsv \
            --balance {R}/balance.tsv \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container} --out-status members/{MEMBER}_status.tsv

        # The VERSION comes from the installed distribution, not the module: localmapper
        # 0.1.5 reports `__version__ == "0.1.1"`, and filing 0.1.5's output under 0.1.1
        # would collide with a real 0.1.1 run at one path with nothing to tell them apart.
        # `collect` resolves that itself; the lane just names the tool.
        # The SIDECARS are evidence in their own right: subtracting them from the caches
        # names the reactions a shard died inside, which nothing else records.
        {py} -m ecspr.bake.evidence collect --root _ev --tool {MEMBER} \
            --file members/{MEMBER}.tsv members/{MEMBER}_*.tsv \
                   members/{MEMBER}_*.attempted members/{MEMBER}_status.tsv \
                   {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{pairs: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "rxnmapper").is_dir()
                 and any((iev.local / "rxnmapper").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=SHARDS, memory=Size.GB(32), duration=Duration(hours=12)),
)
