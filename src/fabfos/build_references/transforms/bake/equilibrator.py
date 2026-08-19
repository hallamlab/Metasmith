# Direction member lane: eQuilibrator -- component-contribution over the pinned cache.
#
# One of the two thermodynamic members, and the expensive one: loading the 1.3 GB compound
# cache and running component-contribution over the reaction universe is most of this
# ensemble's wall clock. It is its own step for the same reason LocalMapper is -- a change
# to the calibration or the combiner must not re-pay it.
#
# `XDG_CACHE_HOME` IS THE ONLY LEVER ON THE CACHE, and this is the fact the lane exists
# around. `equilibrator_cache.zenodo.get_cached_filepath` resolves through
# `pooch.os_cache("equilibrator")` -- i.e. `$XDG_CACHE_HOME/equilibrator/<file>` -- and
# reads nothing else. `EQUILIBRATOR_CACHE_DIR` is read by no part of pooch,
# equilibrator_cache or component_contribution: it was exported for a generation and was a
# no-op the whole time, and what actually made that build work was that the acquisition
# happened to leave the package's own nested layout in place. Now that
# `acquire/equilibrator.py` lifts the two artifacts into `equilibrator/<version>/`, the
# layout pooch expects is rebuilt here BY SYMLINK -- compounds.sqlite is 1.3 GB and a copy
# per member is 1.3 GB of nothing.
#
# Pooch re-checks its embedded md5 for each file before using it, which is the same claim
# `zenodo.md5` records in the acquired product. So a wrong or truncated staged cache is
# caught here rather than showing up as strange free energies.
#
# THE UNIVERSE IS RECOMPUTED, NOT PASSED. `dir_drive universe` is a parse of one MetaNetX
# release's reac_prop, and the release is asserted single at the top of every lane -- so
# the two members and the combiner derive the same list from the same bytes with the same
# code. Making it a product instead would put a shared node between two members that have
# nothing else to say to each other, and would serialise them behind it.
#
# SHARDED, AND IT WAS THE CRITICAL PATH UNTIL IT WAS. This lane ran at `cpus=1` while the
# dGbyG lane ran twenty-wide, so a members run cost whatever eQuilibrator cost serially --
# ~38 minutes of a ~38 minute run, with the other member idle for most of it. Nothing about
# the member required that: `drive eval --shard i/n` and `drive merge --expect n` are
# member-generic, partitioned by the same crc32 of the MNXR the mapper lanes use, and the
# merge refuses unless the shards reconstitute the universe exactly.
#
# THE MEMORY MULTIPLIES AND DOES NOT AMORTISE. The 2.372 GB peak is fixed per process --
# parsed chem_prop plus component-contribution's preprocessor matrices, ~0 marginal per
# reaction -- so it does not fall as the partition narrows. Sixteen shards is 38 GB against
# a 190 GB node, which is why sixteen and not four.
#
# The 1.3 GB compound cache is shared BY SYMLINK across the shards rather than copied, and
# is opened read-only, so the fan-out costs one cache rather than sixteen.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::equilibrator.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
equilib   = model.AddRequirement(lib.GetType("fabfos_data::equilibrator"))

bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_eq    = model.AddProduct(lib.GetType("interm::direction_member_eq"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

CACHE_LINK = "_eqcache"

# A PROPERTY OF THE HOST, not of the method -- the same rule the dGbyG lane states. The
# partition is content-addressed, so this changes what runs concurrently and never what
# any reaction is asked. Sixteen fits a 32-core Sockeye node beside its own memory (16 x
# 2.372 GB = 38 GB of 190) and leaves the node's other half for the merge.
SHARDS = 16

RESOLVE = f"""
    set -e
    N=$(find {{metanetx}} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[eq] expected exactly one release under {{metanetx}}, found $N" >&2
        exit 1
    fi
    MNX=$(find {{metanetx}} -mindepth 1 -maxdepth 1 -type d)

    N=$(find {{equilibrator}} -mindepth 1 -maxdepth 1 -type d | wc -l)
    if [ "$N" -ne 1 ]; then
        echo "[eq] expected exactly one cache version under {{equilibrator}}, found $N." \\
             'compounds.sqlite and cc_params.npz are FITTED TOGETHER; mixing versions' \\
             'silently changes what dG the measured arm reports' >&2
        exit 1
    fi
    EQ=$(find {{equilibrator}} -mindepth 1 -maxdepth 1 -type d)

    mkdir -p {CACHE_LINK}/equilibrator
    for f in compounds.sqlite cc_params.npz; do
        if [ ! -s "$EQ/$f" ]; then
            echo "[eq] the pinned cache at $EQ is missing $f" >&2
            exit 1
        fi
        ln -sfn $EQ/$f {CACHE_LINK}/equilibrator/$f
    done
    export XDG_CACHE_HOME=$(pwd -P)/{CACHE_LINK}
    echo "[eq] metanetx $(basename $MNX) · cache $(basename $EQ)"
"""


def protocol(context: ExecutionContext):
    imnx = context.Input(metanetx)
    ieq  = context.Input(equilib)
    ilib = context.Input(bakelib)
    iout = context.Output(out_eq)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    resolve = RESOLVE.format(metanetx=imnx.container, equilibrator=ieq.container)
    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        {resolve}
        # WHAT VERSIONS THIS LANE'S EVIDENCE. Left to itself `evidence collect` would name
        # the directory after component_contribution -- a package this image pins, so it
        # does not move when the ensemble's own code does, and two runs of materially
        # different direction code would land in one directory. The fallback is no better:
        # it hashes `bake/*.py` and the whole ensemble lives in `bake/direction/`.
        DIRVER=$({py} -m ecspr.bake.evidence fingerprint --package direction)
        echo "[eq] direction method $DIRVER"

        {py} -m ecspr.bake.direction.drive universe --reac-prop $MNX/reac_prop.tsv \
            --out _universe.json
        # --require: this lane's ONLY product is the member table, so an unavailable
        # member is a failed step, not a missing vote. The tolerant path belongs to the
        # combiner, which is where "one member is absent" is a legitimate state.
        # --substitutions: the carrier and polymer tables ship INSIDE the vendored
        # code, so this path is the same tree `evidence fingerprint` just hashed and a
        # table cannot come apart from the DIRVER that describes it. It must match what
        # `forecast build` priced, or the accounting describes a bake nobody built.
        # Omitting it is what reproduces r8.
        mkdir -p members
        pids=""
        for i in $(seq 0 {SHARDS - 1}); do
            {py} -m ecspr.bake.direction.drive eval --member eq --require \
                --universe _universe.json --shard $i/{SHARDS} \
                --reac-prop $MNX/reac_prop.tsv --chem-prop $MNX/chem_prop.tsv \
                --substitutions {libdir}/ecspr/bake/direction \
                --out members/eq_$i.parquet &
            pids="$pids $!"
        done
        rc=0
        for p in $pids; do wait $p || rc=1; done
        if [ $rc -ne 0 ]; then
            echo "[eq] a shard exited non-zero." >&2
            echo "   NOT ignored: a bare 'wait' returns 0 whatever the children did, and" >&2
            echo "   that is how a short member reached the fusion once already." >&2
            exit 1
        fi

        # The merge is this lane's completeness proof, which is why it is handed the
        # universe rather than just the tables: sixteen files that parse is not the same
        # claim as sixteen files that add up to what was asked about.
        {py} -m ecspr.bake.direction.drive merge --member eq \
            --shard-file members/eq_*.parquet --expect {SHARDS} \
            --universe _universe.json --out {iout.container}

        # PER-SHARD tables as well as the merged one. `unresolved` against `no_props` is
        # what separates a compound eQuilibrator cannot look up from one MetaNetX never
        # described, and a shard that abstained on everything -- a cold or half-linked
        # cache -- is only visible before the concatenation.
        {py} -m ecspr.bake.evidence collect --root _ev --tool equilibrator \
            --version $DIRVER \
            --file _universe.json members/eq_*.parquet {iout.container}
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_eq: iout.local}, {ev: iev.local}],
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "equilibrator").is_dir()
                 and any((iev.local / "equilibrator").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # MEASURED, not budgeted -- `research/fabfos/benchmarks/direction_rescue/SHARD_COST.md`
    # has the run this comes from. The declaration it replaces (4 cpus, 32 GB, 12 hours)
    # was none of those things and was wrong in both directions at once.
    #
    #   cpus       == SHARDS. Each shard is one single-threaded pass with OMP capped at
    #              one above, so more buys nothing and fewer oversubscribes.
    #   GB(48)     16 x 2.372 GB of fixed per-process footprint, plus room for the merge's
    #              concatenation. The peak does NOT fall as the partition narrows, so this
    #              must track SHARDS -- raising the count without raising this is how a
    #              fan-out gets OOM-killed at the moment it starts paying off.
    #   hours=2    23 s of startup plus ~48 ms/reaction over 83,795 reactions is ~68
    #              minutes serially and ~4 minutes across sixteen. The margin is for a
    #              cluster filesystem re-hashing the 1.34 GB pooch cache cold, which every
    #              shard waits on and only one of them pays for.
    resources=Resources(cpus=SHARDS, memory=Size.GB(48), duration=Duration(hours=2)),
)
