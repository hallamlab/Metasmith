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
    context.ExecWithEnv(env=image, cmd=_cmd)

    size = idb.local.stat().st_size if idb.local.exists() else 0
    Log.Info(f"uniref50.dmnd {size/1e9:.1f} GB")
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=size > 1_000_000_000,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=8, memory=Size.GB(64), duration=Duration(hours=12)),
)
