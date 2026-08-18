"""AAM member lane: LocalMapper -- the graph-neural mapper, run over the GAP.

THE ROLE IS THE POINT, and it was wrong here until now. In the deployed chain that
produced the reference this build reproduces, LocalMapper was never a universe sweep: it
ran over 487 reactions the rest of the pipeline could reach and had no mapping for, of
which it mapped 474 -- and every one of its 401 contributions to the deployed table comes
from that set. Zero fall outside it.

Ported as a sweep, it became a 57,522-reaction lane: 24 declared hours, roughly 20
measured, and two OOM kills at 48 GB, both of them frozen on a single very large
reaction. It bought a third vote on reactions the other two members had already agreed
about, and it changed the ensemble's arithmetic on all of them -- more members voting is
not free, it moves every consensus, every `*_only` and every dilution weight in the
table.

So the lane now takes the Indigo and RXNMapper products as REQUIREMENTS and maps only
what they left behind. That is the deployed "reach AND no rxn" gap set, expressed as
something the graph computes rather than something a person assembled by hand, and it
puts LocalMapper where a third opinion is worth paying for: the reactions where the first
two produced nothing.

THE MEMORY GUARD is what remains of the OOM. `--mem-budget-gb` caps the process's address
space AFTER the model loads (capping before it refuses the torch/DGL import, which
reserves a large virtual arena), so a reaction that surprises the worklist's size
threshold raises MemoryError inside the mapper call and is recorded as an abstention --
an outcome the ensemble can read -- instead of taking the node down with it.

THE REAL BOUND IS ON HOW LONG A PROCESS LIVES, and that took two runs to see. Single
process, 2026-07-28: 818 of 1,140 mapped, then 81 minutes on THREE reactions at 99.9% CPU
and a flat 13 GB, 323 still to go, so it was killed. The obvious reading was a search that
does not converge on certain reactions. THAT READING WAS WRONG, and the re-run refutes it
directly: MNXR198828, one of the two it was grinding on when it died, maps in shard 0 of
the sharded lane, which averages 4.3 s per reaction over 197 of them.

What actually degrades is the PROCESS. Across that single run the rate fell monotonically
-- 0.19, then 0.86, then 0.094, then 0.061 reactions/s -- which is the signature of
something accumulating in-process, not of a few expensive reactions arriving late. Six
shards cap that accumulation at ~190 reactions each, and the same universe that did not
finish in three hours finishes in forty-nine minutes with every reaction inside the
budget. The shard count is bounded by MEMORY, not cores: ~13 GB resident apiece, most of
it the loaded model, which every shard pays for separately.

THE TIMEOUT IS A BOUND, NOT THE FIX, and it is worth keeping precisely because it costs
nothing: zero reactions hit it on the full gap set, all 1,140 of which carry a map. It
exists so the worst case is computable at all -- before it, one reaction could hold the
lane forever and the run had no upper bound of any kind.

A TIGHTER SIZE CAP WOULD HAVE BEEN THE WRONG FIX TWICE OVER: the reaction it appeared to
stall on carries 280 atoms while reactions at the worklist's 600 ceiling had already gone
through, and the stall was not about the reaction at all.

THE CACHE IS STAGED IN NOW, which is what it never was. It used to live only in the task
work directory -- node-local scratch on the cluster, discarded on retry -- so its
per-reaction resume protected a run against nothing that actually happens on Sockeye.
`fabfos_data::aam_cache` is the same file handed in as a given, keyed on the SUBMISSION
STRING rather than on the reaction id, because one MNXR now has up to four possible
submissions and serving the wrong one is a map of a different molecule.

THE GAP IS COMPUTED AT (reaction, element) GRAIN, and with one universe that stops being a
nicety. When the three passes were separate, a reduced submission could only ever be
compared against the other members of ITS OWN pass. In one pass a reaction whose carbon
mapped would mark its own nitrogen reduction as covered, and this member would skip exactly
the submission the forecast built for it.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::rdkit.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
universe  = model.AddRequirement(lib.GetType("interm::aam_universe"))
rescue    = model.AddRequirement(lib.GetType("interm::aam_rescue"))
cache     = model.AddRequirement(lib.GetType("fabfos_data::aam_cache"))
# THE GAP IS DEFINED BY THESE TWO, so they are requirements rather than an ordering
# convention. The planner cannot schedule this lane before them, which is exactly the
# constraint the role implies: a gap-filler that runs first fills the whole universe --
# and with one pass instead of three that ordering is the ONLY thing keeping this member
# a gap-filler rather than a third redundant vote.
m_indigo  = model.AddRequirement(lib.GetType("interm::aam_member_indigo"))
m_rxn     = model.AddRequirement(lib.GetType("interm::aam_member_rxnmapper"))
bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

pairs     = model.AddProduct(lib.GetType("interm::aam_member_localmapper"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

MEMBER = "localmapper"
# Measured in the real :aam image on one core: ~0.65 GB at 100 atoms, 1.0 GB at 800,
# 3.3 GB at 1,000, 8.1 GB at 2,751. The worklist admits nothing over 600 atoms, so this
# budget is headroom for a surprise rather than a working limit.
MEM_BUDGET_GB = 12
# Seconds per reaction before the map abstains. See `aam_neural_members.run_localmapper`
# for the run that made this necessary; in short, the memory guard bounded the reactions
# that GROW and nothing bounded the ones that never converge.
TIMEOUT_S = 240
# SIX SHARDS, and the count is set by memory rather than by cores. A LocalMapper process
# sat at a flat 13 GB resident through the run that motivated the timeout -- mostly the
# loaded model, which every shard pays for separately -- so six is what fits the 110 GB
# executor pool with real headroom, not the 32 the box has cores for. The gap is ~1,100
# reactions, so six ways is already the difference between forty minutes and a day.
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
    # HOME IS NOT WRITABLE IN THIS CONTAINER AND DGL DIES ON THAT. The agent execs with
    # `--no-home --cleanenv`, so $HOME resolves to a path that is not mounted and the
    # config block's MPLCONFIGDIR/XDG_CACHE_HOME never reach the process. DGL writes its
    # backend selection to ~/.dgl on IMPORT and raises PermissionError when it cannot --
    # which killed this lane 45 s in, before a single reaction, and took the two steps
    # downstream of it with it. Matplotlib hits the same wall one line earlier and only
    # warns, which is why this reads as a warning followed by an unrelated traceback.
    # DGLBACKEND set means it never consults the file at all; HOME is redirected anyway
    # so the next library with the same habit is not a second incident.
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
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{pairs: iout.local}, {ev: iev.local}],
        # THE RAW OUTPUT IS PART OF THE RESULT, not a diagnostic nicety. The copy happens
        # seconds after the tool finished, in the same command, so an absence is not the
        # lane being busy -- it is something going wrong that a green lane would hide.
        # THE PAIRS TABLE MAY LEGITIMATELY BE SMALL and must still be non-empty: a gap of
        # a few hundred reactions is the expected size, but a gap of ZERO would mean the
        # covered-set read went wrong rather than that the other members were perfect.
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "localmapper").is_dir()
                 and any((iev.local / "localmapper").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # cpus == SHARDS: each shard is one single-threaded inference process. Memory is
    # 14 GB a shard, from a MEASURED 13 GB resident -- the loaded model dominates it and
    # every shard pays for it separately, which is why the shard count is bounded by
    # memory and not by the box's 32 cores.
    #
    # The 4 h this used to declare was not a measurement and did not hold: the lane ran
    # 3 h 15 m single-process and was 323 reactions short when it was killed. With every
    # reaction bounded at TIMEOUT_S the worst case is finally computable -- a shard of
    # ~190 reactions that ALL time out is 12.7 h -- and 12 h is that bound rounded to
    # what the box can actually be asked for, given 818 of 1,140 had already gone
    # through fast enough to make the all-timeout case impossible.
    resources=Resources(cpus=SHARDS, memory=Size.GB(84), duration=Duration(hours=12)),
)
