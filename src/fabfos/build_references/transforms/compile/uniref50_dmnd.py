"""R4 -- the DIAMOND database the uniref lane searches.

Separate from the fetch so a DIAMOND version bump rebuilds the index without
re-downloading 8.8 GB. `diamond makedb` reads the gzip directly, so the acquisition
never has to be expanded to ~60 GB on disk.

THE RELEASE DIRECTORY IS RESOLVED, NOT NAMED. `fabfos_data::uniref` is the whole
source folder and the release inside it moves -- `current_release/` rolls forward
under the acquire step, so hardcoding `2026_02` here would silently keep building an
index of whichever snapshot was current when this file was written.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::diamond.env"))
source = model.AddRequirement(lib.GetType("fabfos_data::uniref"))
db     = model.AddProduct(lib.GetType("ref::uniref50_diamond_db"))

FASTA = "uniref50.fasta.gz"


def protocol(context: ExecutionContext):
    isrc = context.Input(source)
    idb  = context.Output(db)

    # -d takes a PREFIX and appends .dmnd, so the database is built beside the product
    # and moved onto it.
    _cmd = f"""
        set -e
        N=$(find {isrc.container} -mindepth 1 -maxdepth 1 -type d | wc -l)
        if [ "$N" -ne 1 ]; then
            echo "[uniref] expected exactly one release under {isrc.container}, found $N." \\
                 'Which UniRef50 release to index is a decision, and an index carries no' \\
                 'record of which one it came from' >&2
            exit 1
        fi
        REL=$(find {isrc.container} -mindepth 1 -maxdepth 1 -type d)
        echo "[uniref] indexing $(basename $REL)/{FASTA}"

        diamond makedb --in $REL/{FASTA} -d uniref50 --threads ${{SLURM_CPUS_PER_TASK:-8}}
        mv uniref50.dmnd {idb.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    size = idb.local.stat().st_size if idb.local.exists() else 0
    Log.Info(f"uniref50.dmnd {size/1e9:.1f} GB")
    # A DIAMOND database of UniRef50 is tens of GB. A few hundred MB means makedb read a
    # truncated download and exited zero, and the uniref lane would then quietly find
    # nothing for most ORFs.
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=size > 1_000_000_000,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # NOT labels=["local"]. That label is right for `acquire/` -- a download needs the
    # login node's network -- and copying it here is what pinned every compile to the
    # login node under the slurm preset: `xlocalx` sets `executor = 'local'`, whose pool
    # slurm.nf declares as 8 cores / 8 GB, and Nextflow's local executor REFUSES a
    # process asking for more rather than queueing it. It also sets
    # errorStrategy='ignore' with no retry, so the refusal is silent and the workflow
    # goes green with the reference absent. Nothing in this transform touches the
    # network; it belongs on a compute node.
    resources=Resources(cpus=8, memory=Size.GB(64), duration=Duration(hours=12)),
)
