"""AAM member lane: Indigo over the PARTIAL universe. Pass 3.

Read `indigo_rescue.py` for the lane's shape. What differs here is only what it is
pointed at, and one extraction argument.

THE SUBMISSIONS ARE ELEMENT REDUCTIONS. Each row of the partial universe is one
(reaction, element): the participants that carry no X have been removed, the balance of X
across what remains has been checked, and the reaction that results is small -- which is
the whole reason this pass exists. Indigo is here for the reason it is in every other
pass: it is the third member and the NON-neural one, so its agreement with the two
transformers is independent confirmation rather than shared architectural bias. See
`ecspr.bake.aam.partial`.

`--partial` IS NOT OPTIONAL HERE. The extractor would otherwise re-derive the participant
list from `reac_prop`, compare it to a template count that is deliberately smaller, and
refuse every submission as `stripped`. It also tells the extractor WHICH element to read
back: every other element's map in a reduced submission is discarded unread, because for
those the reduction really is a strip.

SMALL SUBMISSIONS, so one shard and a short wall clock: the point of the reduction is
that what is left fits.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
partial   = model.AddRequirement(lib.GetType("interm::aam_partial"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_indigo_partial"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

MEMBER = "indigo"
TOOL = "indigo_partial"
SHARDS = 1
# Indigo is a compiled automapper and can hang uninterruptibly on one reaction; the
# timeout is what makes that cost one submission rather than the lane.
TIMEOUT_S = 20

RESOLVE = """
    set -e
    N=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[indigo_partial] expected one release under {metanetx}, found $N" >&2
        exit 1
    fi
    MNX=$(find {metanetx} -mindepth 1 -maxdepth 1 -type d)
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ipar = context.Input(partial)
    ilib = context.Input(bakelib)
    iout = context.Output(pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent
    P = ipar.container

    resolve = RESOLVE.format(metanetx=imnx.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        mkdir -p members

        {py} -m ecspr.bake.aam.indigo_member map \
            --worklist {P}/partial_universe.parquet \
            --shard 0/1 --timeout {TIMEOUT_S} \
            --sidecar members/{TOOL}_0.attempted \
            --out members/{TOOL}_0.tsv
        # One shard, and `merge` still runs: it is what checks the shard count matches
        # what the caches were written under, and skipping it because there is only one
        # is how a two-shard resume later reads one shard's output as the whole member.
        {py} -m ecspr.bake.aam.indigo_member merge \
            --shard-file members/{TOOL}_0.tsv --expect 1 \
            --out members/{TOOL}.tsv

        {py} -m ecspr.bake.atom_pairs extract \
            --aam members/{TOOL}.tsv --align strict \
            --partial {P}/partial_universe.parquet \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container} --out-status members/{TOOL}_status.tsv

        {py} -m ecspr.bake.evidence collect --root _ev --tool {TOOL} \
            --file members/{TOOL}.tsv members/{TOOL}_*.tsv \
                   members/{TOOL}_status.tsv {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{pairs: iout.local}, {ev: iev.local}],
        # An EMPTY partial universe is a legitimate state -- it means the members reached
        # everything -- so this lane must not treat an empty product as a failure the way
        # the sweeping passes do. The file has to EXIST; it does not have to have rows.
        success=(iout.local.exists()
                 and (iev.local / TOOL).is_dir()
                 and any((iev.local / TOOL).iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(24), duration=Duration(hours=4)),
)
