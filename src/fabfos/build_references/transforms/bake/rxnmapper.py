"""AAM member lane: RXNMapper -- the BERT attention mapper. Pass 1, over the worklist.

One tool, one step, one product. The lane maps and then EXTRACTS, so what leaves it is
the shared pairs shape rather than the tool's private cache; the cache goes to
evidence::, where the claim `source=rxnmapper+indigo` on a fused row can be checked
against what the member actually said.

IT READS `interm::aam_worklist`, NOT `lookup::reactions`. The worklist is the adjudicated
universe -- one row per MNXR with a verdict -- and every member takes `verdict ==
mappable` from it. Two things follow. The reaction SMILES is one string built once, so
the disagreement this ensemble measures is between mappers and not between two SMILES
builders. And the reactions this member does NOT attempt have a row saying why, which is
the difference between a member that abstained and a member that never looked.

IT MAPS THE WHOLE MAPPABLE UNIVERSE, including the ~13k the curated map already answers.
That looks wasteful and is a deliberate trade. The member used to be handed an
`--exclude` list built from layer 1, which meant every member lane depended on the
curated lane and could not start until it finished. `aam_layers.stack` restricts each
layer to what nothing below it claimed ANYWAY, so the exclude never changed a single row
of the result -- it only saved ~22.6% of the mapping. Paying that back buys a graph where
the members depend on nothing but the worklist.

SHARDED, for cost rather than for hangs. With LocalMapper demoted to the gap it was built
for, this is the longest mapper lane, and four concurrent single-threaded processes turn
most of a shift into a couple of hours. The sidecar discipline comes along with the
shards and earns its keep for a different failure than Indigo's: RXNMapper does not hang,
it gets OOM-killed, and a resume cannot tell the difference -- either way the run died
inside a reaction having written nothing about it, and without the attempted-ids log the
next run picks the same reaction and dies again.

`--align strict` at extraction, unlike layer 1: we built these reaction SMILES from the
equation, one fragment per participant, so a template-count mismatch really does mean a
molecule went missing and the mapper re-routed its atoms onto what remained.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
worklist  = model.AddRequirement(lib.GetType("interm::aam_worklist"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_rxnmapper"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

MEMBER = "rxnmapper"
# One core each, transformer inference. Four is the point where the lane stops being the
# build's long pole; more would need the memory of another model copy per shard for
# less and less.
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
    iwl  = context.Input(worklist)
    ilib = context.Input(bakelib)
    iout = context.Output(pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, member=MEMBER)
    # OMP pinned to 1 and re-exported HERE rather than inherited: the mapper forks, and
    # an unpinned BLAS inside each fork oversubscribes the node into swap. With four
    # shards on four cores that is the difference between four processes and four times
    # the node's cores.
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        mkdir -p members

        pids=""
        for i in $(seq 0 {SHARDS - 1}); do
            {py} -m ecspr.bake.aam.neural_members --member {MEMBER} \
                --worklist {iwl.container} \
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
        {py} -m ecspr.bake.atom_pairs extract \
            --aam members/{MEMBER}.tsv --align strict --fallback-forced \
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
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{pairs: iout.local}, {ev: iev.local}],
        # THE RAW OUTPUT IS PART OF THE RESULT, not a diagnostic nicety. This used to
        # pass on the table alone, arguing that an evidence directory lost after a
        # twelve-hour run was not worth failing over. It is: the copy happens seconds
        # after the tool finished, in the same command, so an absence is not the lane
        # being busy -- it is something going wrong that a green lane would hide, and the
        # tool's own output is the only record of what it actually said.
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "rxnmapper").is_dir()
                 and any((iev.local / "rxnmapper").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # cpus == SHARDS: each shard is one single-threaded inference process. Memory covers
    # four model copies with room for the largest reaction the worklist admits.
    resources=Resources(cpus=SHARDS, memory=Size.GB(32), duration=Duration(hours=12)),
)
