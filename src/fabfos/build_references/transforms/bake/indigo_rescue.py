"""AAM member lane: Indigo over the RESCUED universe. Pass 2.

The same tool, the same sidecar discipline and the same shard count as the pass-1 lane --
read `indigo.py` for why Indigo needs all three. What differs is the universe and the
extraction.

THE UNIVERSE is `interm::aam_rescue`'s `rescued.parquet`: reactions MetaNetX could not
build, completed with curated structures and `*` bodies that cancel. The same MNXR has a
DIFFERENT SMILES here than in the worklist, which is why this is a separate product
rather than more rows in the pass-1 cache -- merging the two on `mnxr` would overwrite a
map of one molecule with a map of another.

THE EXTRACTION carries three extra arguments and each one is load-bearing.
`--placeholders` names the scaffolding whose atoms must be SUPPRESSED: a placeholder
stands in for a generic carrier so the mapper sees a sane reaction, and its atoms are not
that molecule's atoms. `--resolved` names the curated structures whose atoms must be
KEPT: those ARE the metabolite, and suppressing them would delete the very edges the
curation exists to license. `--balance` drops, per reaction and per element, the elements
whose concrete atoms did not balance -- which is where the conservation claim is tested.

IN THE DEPLOYED CHAIN THIS WAS THE ONLY RESCUE MAPPER, which is why every rescue-derived
reaction in that table is `mcs_only` at half weight. Here it is one vote of three.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_indigo_rescue"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

# This universe is a sixth of pass 1's, but the two lanes barely overlap in practice --
# this one cannot start until the curation sweep has run, by which point pass 1 is
# finishing -- so it takes the width rather than a proportional share. Single pass here
# too; see indigo.py for why the retry went.
SHARDS = 16
TIMEOUT_S = 20
TOOL = "indigo_rescue"

RESOLVE = """
    set -e
    N=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[indigo_rescue] expected exactly one release under {metanetx}, found $N" >&2
        exit 1
    fi
    MNX=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d)
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ires = context.Input(rescue)
    ilib = context.Input(bakelib)
    iout = context.Output(pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent
    R = ires.container

    resolve = RESOLVE.format(metanetx=imnx.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        mkdir -p members

        pids=""
        for i in $(seq 0 {SHARDS - 1}); do
            {py} -m ecspr.bake.aam.indigo_member map \
                --worklist {R}/rescued.parquet \
                --shard $i/{SHARDS} --timeout {TIMEOUT_S} \
                --sidecar members/{TOOL}_$i.attempted \
                --out members/{TOOL}_$i.tsv &
            pids="$pids $!"
        done
        rc=0
        for p in $pids; do wait $p || rc=1; done
        if [ $rc -ne 0 ]; then
            echo "[{TOOL}] a shard exited non-zero; the caches are resumable" >&2
            exit 1
        fi

        {py} -m ecspr.bake.aam.indigo_member merge \
            --shard-file members/{TOOL}_*.tsv --expect {SHARDS} \
            --out members/{TOOL}.tsv
        {py} -m ecspr.bake.atom_pairs extract \
            --aam members/{TOOL}.tsv --align strict \
            --placeholders {R}/placeholders.tsv --resolved {R}/crosswalk.tsv \
            --balance {R}/balance.tsv \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container} --out-status members/{TOOL}_status.tsv

        {py} -m ecspr.bake.evidence collect --root _ev --tool {TOOL} \
            --file members/{TOOL}.tsv members/{TOOL}_*.tsv \
                   members/{TOOL}_*.attempted members/{TOOL}_status.tsv \
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
                 and (iev.local / TOOL).is_dir()
                 and any((iev.local / TOOL).iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # A sixth of pass 1's universe, and the completed reactions are small -- the vehicles
    # are drawn budgets, not real macromolecules. Same shape, a fraction of the wall time.
    # Memory is the measured 650 MB per process with room, not a guess: what a lane
    # DECLARES is what the executor reserves, so an inflated figure idles the host.
    resources=Resources(cpus=SHARDS, memory=Size.GB(16), duration=Duration(hours=6)),
)
