"""Direction member lane: dGbyG -- the learned dGr'0, with the wildcard guard.

The direction ensemble's second thermodynamic member, and until now the one that never
ran. It was deferred for a stated reason -- "it needs a third image" -- and that reason
was almost right for the wrong cause, which is worth writing down because it is why the
deferral outlived its justification:

  * The recorded reason was numpy. equilibrator-cache 0.7.1 floors numpy at 2 and torch
    2.2.1 is a numpy 1.x C-API build, so the AAM lane and the direction lane cannot
    share an image. True, and irrelevant here: dGbyG's torch 2.8 is itself a numpy 2
    build, and `envs/build-refs-equilibrator.yml` carries both thermodynamic members
    together and resolves.
  * The actual reason is PYTHON. dGbyG's source uses a same-quote nested f-string,
    legal only from 3.12, and the eQuilibrator stack is pinned at 3.11 here. Under 3.11
    it installs and then raises SyntaxError at import -- which `dir_drive`'s
    unavailable-member guard did not catch, because SyntaxError is not ImportError. So
    the member did not degrade to a missing vote; it took the step down with it.

Hence `env::dgbyg.env`: python 3.12, torch 2.8, no equilibrator packages. The two
thermodynamic members share nothing but the universe they are asked about.

THE GUARD IS THE POINT OF THE LANE. dGbyG silently accepts R-group wildcards -- rdkit
reports `*` as atomic number 0, which one-hots to a perfectly valid feature vector, so
the model returns a confident number for a compound it never trained on ('*C(=O)O' ->
-360 +/- 26). Any reaction carrying a wildcard in any participant ABSTAINS here.
Returning that number would be strictly worse than silence, and the image's build-time
assertion checks the guard can still see a wildcard at all.

CORRELATED WITH THE eQUILIBRATOR MEMBER, both being TECRDB-fitted, which is why the
combiner floors their fused uncertainty rather than counting two independent votes. The
independent member of this ensemble is MetaCyc, and it comes from the curated lane.

SHARDED, BECAUSE IT SCALES AND THE SERIAL COST WAS A GUESS. The declaration used to say
twenty-four hours with a comment admitting the figure had never been measured. It has been
now: 0.249 s per reaction alone, 0.286 s under sixteen-way contention, at a flat 2.15 GB
per process that does not grow with concurrency -- 83,795 reactions is about 5.8 hours in
one process and about fifty minutes across eight. There is no shared state to contend for,
only a 20 s startup per process spent parsing the compound table, so the scaling is nearly
linear and the only reason not to shard was that nobody had tried.

THE SHARD COUNT IS A PROPERTY OF THE HOST, not of the method. Eight was sized for a
shared 16-core box the eQuilibrator member had to fit on beside this one; under a
scheduler each lane gets its own allocation and the count follows what a node will grant.
It is twenty now. The partition is content-addressed -- crc32 of the MNXR, `aam_shard`'s,
the same one the mapper lanes use -- so the count changes what runs concurrently and not
what any reaction is asked. Coverage held at 29.9% of the universe against the trial
sample's 33.2%, and it is a property of the model, not of the fan-out.

No sidecar: Indigo needs one because it hangs inside a compiled search having written
nothing, whereas this is a bounded forward pass that showed no stalls across the trial.
What stands in for it is `dir_drive merge`, which is handed the universe and refuses
unless the shards reconstitute it exactly.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::dgbyg.env"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))

bakelib   = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_db    = model.AddProduct(lib.GetType("interm::direction_member_dgbyg"))
ev        = model.AddProduct(lib.GetType("evidence::tool_output"))

# ONE constant, read by both the fan-out and the merge's --expect. Written twice is how a
# resume silently re-partitions the universe.
#
# TWENTY, because the host changed. Eight was sized for chamois, a SHARED 16-core box the
# eQuilibrator member had to fit on beside this one. Under SLURM the lane gets its own
# allocation and the only ceiling is what a node will grant, so the measured near-linear
# scaling is worth taking: fifty minutes at eight, about twenty at twenty. The partition
# is content-addressed (crc32 of the MNXR), not positional, so a table written at eight
# and one written at twenty are the same table -- only `merge --expect` has to agree with
# the fan-out, which is why this is one constant and not two.
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
    # LD_LIBRARY_PATH is not optional here and is not cargo: rdkit in this env needs the
    # env's own newer libstdc++, not the host's, and without it the import fails inside a
    # C extension with a message that names a symbol rather than a library.
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
        # THE RAW OUTPUT IS PART OF THE RESULT, not a diagnostic nicety. This used to
        # pass on the table alone, arguing that an evidence directory lost after a
        # twelve-hour run was not worth failing over. It is: the copy happens seconds
        # after the tool finished, in the same command, so an absence is not the lane
        # being busy -- it is something going wrong that a green lane would hide, and the
        # tool's own output is the only record of what it actually said.
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "dgbyg").is_dir()
                 and any((iev.local / "dgbyg").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # MEASURED, not budgeted. cpus == SHARDS: each shard is one single-threaded forward
    # pass (OMP is capped at one above), so more buys nothing and fewer oversubscribes.
    # Memory TRACKS SHARDS and must -- the footprint is flat per process at an observed
    # 2.15 GB and does not fall as the partition narrows, so raising the shard count
    # without raising this is how a fan-out gets OOM-killed at the exact moment it starts
    # paying off. Twenty at 2.15 plus room for the merge's concatenation.
    resources=Resources(cpus=SHARDS, memory=Size.GB(56), duration=Duration(hours=2)),
)
