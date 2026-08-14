"""AAM member lane: Indigo -- the deterministic structural mapper.

The third L2 member, and the only one that is not a model. It is also the ARBITER of
the curation sweep, which is why it is a hard dependency of two layers rather than one.

SHARDED, BECAUSE INDIGO HANGS. It can stall inside its compiled search where no signal
and no timeout option reaches it -- a hang, not a slow reaction, and it writes no row on
its way in. So the lane fans out over shards, each shard records the reaction it is
about to attempt in a SIDECAR before attempting it, and a shard that dies is restarted
past the hang having lost exactly one reaction. The difference between a sidecar and its
cache is the only record that a hang happened at all.

A TIMEOUT IS A RECORDED OUTCOME, not a crash: the row is written with an empty map and
`status=timeout`, so the difference between "Indigo could not" and "Indigo never got
there" survives into the evidence. That distinction is the whole reason the sidecar and
the status column are separate things.

A DEAD SHARD FAILS THE LANE, and that is a change from the shell driver this replaces.
There, the shards were backgrounded and collected with a bare `wait`, which in bash
returns 0 whatever the children did: on 2026-07-26 one of eight shards was OOM-killed
mid-run and the stage carried on, merged, and would have handed a silently short member
to the fusion. Waiting on each PID turns that into a named failure. The caches are
resumable, so re-running the lane costs only what the dead shard had not reached.

ONE PASS, NO RETRY. There used to be a second pass that re-attempted every recorded
timeout at six times the budget, on the argument that a timeout is a statement about a
budget rather than about the chemistry. That argument is still true and the pass was
still wrong, for three reasons the first full run made visible:

  * ITS WARRANT IS INHERITED, NOT MEASURED HERE. The claim was that a previous
    generation's second pass recovered a measurable number of reactions. It prints
    `recovered N of M`, and that line appears in no output this project has kept, so the
    number cannot be checked. What can be observed runs the other way: the rescue lane's
    retry ground for twenty-two minutes over about thirty reactions a shard with cpu
    time equal to wall time throughout, meaning every attempt it had made was timing out
    again at the longer budget too.
  * IT WAS THE LANE. Indigo's timeouts are a size effect and size effects on a
    combinatorial search do not yield to a constant factor. At four shards the pass was
    a hundred reactions each at up to 120 s -- one to three hours to re-ask 0.74% of the
    universe, while most of the host sat idle.
  * IT CHECKPOINTED NOTHING. The map pass flushes a sidecar before every reaction; the
    retry wrote its cache once, after the loop. A crash anywhere in those hours lost the
    whole pass.

What happens to a reaction that times out is unchanged and was always the real answer:
`merge` drops rows with an empty map, so it is simply absent from this member, and it
lands in the gap set LocalMapper exists to cover -- with RXNMapper independently holding
about a third of them already. Widening the fan-out costs the same reactions far less.

A TIGHTER ATOM CAP IS NOT THE ANSWER EITHER, and it was the first thing tried. Timeout
probability by reaction size is 0.04% under 200 atoms, 2.2% to 300, 12.8% to 400 and
51.5% from 400 to the worklist's 600-atom ceiling. Refusing above 400 would avoid 169 of
307 timeouts and throw away 156 reactions Indigo did map -- from a band the worklist's
own yield curve banks at 28%. Twenty seconds is the cheaper way to find out.

TWENTY-EIGHT SHARDS, and the four this used to declare were sized by an incident rather
than by a measurement. The recorded reason was memory: eight concurrent processes
exhausted a 64 GB node, with one shard's cache 26x its siblings', so the headroom was
made to cover the worst shard. The first full run measures the footprint at 650 MB per
process -- twenty-eight of them fit in 20 GB, and that outlier shard would have to be
thirty times worse again to threaten this box. What actually blew up on that node is not
recoverable from here; what is measurable is that it was not the mean.

The count is a SCHEDULING decision and nothing else: `shard_of` is `crc32(mnxr) % n`, so
changing n re-partitions the universe but not the work -- every reaction gets the same
budget, the same single pass and the same merge, and the member is identical whatever n
is. Twenty-eight leaves four cores for the RXNMapper lane it overlaps on a
thirty-two-core host, which is what the number is actually for.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
worklist  = model.AddRequirement(lib.GetType("interm::aam_worklist"))
indigo_m  = model.AddRequirement(lib.GetType("buildlib::aam_indigo_member.py"))
sharder   = model.AddRequirement(lib.GetType("buildlib::aam_shard.py"))
extractor = model.AddRequirement(lib.GetType("buildlib::ecspr_atom_pairs.py"))
evidence  = model.AddRequirement(lib.GetType("buildlib::build_evidence.py"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_indigo"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

SHARDS = 28
# Seconds per reaction before a map is recorded as a timeout rather than waited on. The
# retry pass raises it; this is the budget that keeps the first pass moving.
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
    irx  = context.Input(worklist)
    ilib = context.Input(indigo_m)
    iout = context.Output(pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    # Written without `${{...}}` on purpose: this is an f-string, and every brace that
    # survives into the shell is a brace that fails at hour three instead of at import.
    cmd = f"""
        {resolve}
        mkdir -p members

        pids=""
        for i in $(seq 0 {SHARDS - 1}); do
            {py} {libdir}/aam_indigo_member.py map \
                --worklist {irx.container} \
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

        {py} {libdir}/aam_indigo_member.py merge \
            --shard-file members/indigo_*.tsv --expect {SHARDS} \
            --out members/indigo.tsv
        {py} {libdir}/ecspr_atom_pairs.py extract \
            --aam members/indigo.tsv --align strict --fallback-forced \
            --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
            --out {iout.container} --out-status members/indigo_status.tsv

        # The SIDECARS are evidence in their own right: they say what was ATTEMPTED, and
        # subtracting them from the caches names the reactions Indigo hung on -- which
        # nothing else records, because a hang writes no row.
        {py} {libdir}/build_evidence.py collect --root _ev --tool indigo \
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
        # THE RAW OUTPUT IS PART OF THE RESULT, not a diagnostic nicety. This used to
        # pass on the table alone, arguing that an evidence directory lost after a
        # twelve-hour run was not worth failing over. It is: the copy happens seconds
        # after the tool finished, in the same command, so an absence is not the lane
        # being busy -- it is something going wrong that a green lane would hide, and the
        # tool's own output is the only record of what it actually said.
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "indigo").is_dir()
                 and any((iev.local / "indigo").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # cpus == SHARDS: each shard is a single-threaded compiled search, so asking for more
    # buys nothing and asking for fewer oversubscribes. Memory is MEASURED now -- 650 MB
    # per process at four shards, and a shard of a twenty-four-way split holds a sixth of
    # the reactions a quarter-way split did, so 24 GB is generous rather than tight. The
    # sixty-four this used to ask for was thirty times the truth, and a declaration that
    # wrong does not protect the lane, it idles the host: the executor admits by what is
    # DECLARED, so one lane reserved most of the pool against 2 GB of real use and left a
    # ready step queued behind it.
    resources=Resources(cpus=SHARDS, memory=Size.GB(24), duration=Duration(hours=12)),
)
