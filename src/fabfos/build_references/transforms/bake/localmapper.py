from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
universe  = model.AddRequirement(lib.GetType("interm::aam_universe"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
cache     = model.AddRequirement(lib.GetType("fabfos_data::aam_cache"))
m_indigo  = model.AddRequirement(lib.GetType("interm::aam_member_indigo"))
m_rxn     = model.AddRequirement(lib.GetType("interm::aam_member_rxnmapper"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_localmapper"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

MEMBER = "localmapper"
MEM_BUDGET_GB = 12
TIMEOUT_S = 240
SHARDS = 6

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
    iind = context.Input(m_indigo)
    irxn = context.Input(m_rxn)
    ilib = context.Input(bakelib)
    iout = context.Output(pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent
    R = irs.container

    resolve = RESOLVE.format(metanetx=imnx.container, member=MEMBER)
    py = (f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 "
          f"HOME=$PWD DGLBACKEND=pytorch "
          f"MPLCONFIGDIR=$PWD/_cache/mpl XDG_CACHE_HOME=$PWD/_cache/xdg python3")

    cmd = f"""
        {resolve}
        mkdir -p members

        pids=""
        for i in $(seq 0 {SHARDS - 1}); do
            {py} -m ecspr.bake.aam.neural_members --member {MEMBER} \
                --universe {iun.container} \
                --cache-dir {ica.container}/{MEMBER} \
                --covered {iind.container} {irxn.container} \
                --shard $i/{SHARDS} --timeout {TIMEOUT_S} \
                --sidecar members/{MEMBER}_$i.attempted \
                --timeout-log members/{MEMBER}_$i.timeouts \
                --mem-budget-gb {MEM_BUDGET_GB} \
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

        # ENUMERATED, NOT GLOBBED. `{MEMBER}_*.tsv` also matches `{MEMBER}_status.tsv`,
        # which the extract step below writes -- absent on a first run, PRESENT on a task
        # retry, where it would be concatenated into the member as if it were a shard.
        # Naming the shards also makes a missing one a refusal, which is the whole point
        # of the shard discipline.
        shards=""
        for i in $(seq 0 {SHARDS - 1}); do
            shards="$shards members/{MEMBER}_$i.tsv"
        done
        {py} -m ecspr.bake.aam.neural_members --member {MEMBER} \
            --merge-from $shards \
            --out members/{MEMBER}.tsv
        # ONE EXTRACTION OVER THREE SUBMISSION CLASSES -- see bake/rxnmapper.py for
        # why the rescue's three tables ride along and why they are no-ops for a whole
        # reaction.
        {py} -m ecspr.bake.atom_pairs extract \
            --aam members/{MEMBER}.tsv --align strict --fallback-forced \
            --universe {iun.container} \
            --resolved {R}/crosswalk.tsv --placeholders {R}/placeholders.tsv \
            --balance {R}/balance.tsv \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container} --out-status members/{MEMBER}_status.tsv

        # The per-shard caches, sidecars AND timeout logs are all evidence. The timeout
        # log is the only thing separating "LocalMapper declined" from "LocalMapper ran
        # out of budget", because unlike a hang a timeout writes a row.
        {py} -m ecspr.bake.evidence collect --root _ev --tool {MEMBER} \
            --file members/{MEMBER}.tsv members/{MEMBER}_*.tsv \
                   members/{MEMBER}_*.attempted members/{MEMBER}_*.timeouts \
                   members/{MEMBER}_status.tsv {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{pairs: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "localmapper").is_dir()
                 and any((iev.local / "localmapper").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=SHARDS, memory=Size.GB(84), duration=Duration(hours=12)),
)
