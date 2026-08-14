"""AAM member lane: RXNMapper over the RESCUED universe. Pass 2.

The second vote on the reactions the deployed chain gave only one. Read `rxnmapper.py`
for the lane's shape and `indigo_rescue.py` for what the three extra extraction arguments
mean; what is worth saying here is why a transformer is asked about a vehicle at all.

A COMPLETED REACTION IS NOT AN ODD REACTION. Once the `*` bodies cancel, what remains is
the ordinary chemistry the carrier was hiding -- an acyl chain transferred, a serine
phosphorylated, a monomer added to a chain. That is exactly what an attention mapper is
good at, and it never got the chance because the crosswalk that completes the reaction
was built after the mapper had run. The dummies it does see are inert: their atoms are
suppressed at extraction, so a mapper that routes a `*` badly costs nothing, and one that
routes the CARGO badly disagrees with Indigo, which is the disagreement the ensemble is
built to record.

FEWER SHARDS than pass 1, because the universe is a sixth of the size and the completed
reactions are small -- a vehicle is a drawn budget, not a real macromolecule.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
neural    = model.AddRequirement(lib.GetType("buildlib::aam_neural_members.py"))
sharder   = model.AddRequirement(lib.GetType("buildlib::aam_shard.py"))
extractor = model.AddRequirement(lib.GetType("buildlib::ecspr_atom_pairs.py"))
evidence  = model.AddRequirement(lib.GetType("buildlib::build_evidence.py"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_rxnmapper_rescue"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

MEMBER = "rxnmapper"
TOOL = "rxnmapper_rescue"
SHARDS = 2

RESOLVE = """
    set -e
    N=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[rxnmapper_rescue] expected one release under {metanetx}, found $N" >&2
        exit 1
    fi
    MNX=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d)
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ires = context.Input(rescue)
    ilib = context.Input(neural)
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
            {py} {libdir}/aam_neural_members.py --member {MEMBER} \
                --worklist {R}/rescued.parquet \
                --shard $i/{SHARDS} \
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

        {py} {libdir}/aam_neural_members.py --member {MEMBER} \
            --merge-from members/{TOOL}_*.tsv --out members/{TOOL}.tsv
        {py} {libdir}/ecspr_atom_pairs.py extract \
            --aam members/{TOOL}.tsv --align strict \
            --placeholders {R}/placeholders.tsv --resolved {R}/crosswalk.tsv \
            --balance {R}/balance.tsv \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container} --out-status members/{TOOL}_status.tsv

        {py} {libdir}/build_evidence.py collect --root _ev --tool {TOOL} \
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
    resources=Resources(cpus=SHARDS, memory=Size.GB(24), duration=Duration(hours=6)),
)
