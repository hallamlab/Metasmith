# Antonio's step 21. It requires the augmented database as a product rather than
# the shipped one, which is what orders add_to_db before this by data dependency
# instead of by hoping the scheduler agrees.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::iphop.env"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
db     = model.AddRequirement(lib.GetType("viromics::iphop_augmented_db"))

out_genus  = model.AddProduct(lib.GetType("viromics::host_prediction_genus"))
out_genome = model.AddProduct(lib.GetType("viromics::host_prediction_genome"))
out_detail = model.AddProduct(lib.GetType("viromics::host_prediction_detail"))

# 90 is iPHoP's own default and the paper's cut, and it is hardcoded rather than
# exposed because the tool interpolates it into two of the three output names
# (`Host_prediction_to_genus_m90.csv`). A transform that let it vary would have
# to glob for its own products; one that pins it can name them.
MIN_SCORE = 90


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    idb = context.Input(db)
    threads = context.params.get("cpus", 8)

    # Read from a copy in the work dir, which the cluster presets put on node-local disk.
    # blastn memory-maps the 170 GB host genome database, and the job's memory limit
    # counts page cache, so against Lustre it re-reads the volumes: on fir it read 226 GB
    # in 4 h, at 3 MB/s by the end, and was still on the first of iPHoP's tools.
    # WIsH reads another 111 GB of models.
    local_db = Path("iphop_db")
    out_dir = Path("iphop_out")
    _cmd = f"""
        cp -rH {idb.container} {local_db}
        iphop predict --fa_file {ifrozen.container} --out_dir {out_dir} \
            --db_dir {local_db} -t {threads} -m {MIN_SCORE}
        rm -rf {local_db}
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    produced = {
        out_genus:  out_dir/f"Host_prediction_to_genus_m{MIN_SCORE}.csv",
        out_genome: out_dir/f"Host_prediction_to_genome_m{MIN_SCORE}.csv",
        out_detail: out_dir/"Detailed_output_by_tool.csv",
    }
    missing = [p.name for p in produced.values() if not p.exists()]
    assert not missing, (
        f"iphop predict wrote no {missing}; {out_dir} holds "
        f"{sorted(q.name for q in out_dir.iterdir()) if out_dir.is_dir() else 'nothing'}"
    )

    outs = {}
    for product, src in produced.items():
        o = context.Output(product)
        src.rename(o.local)
        outs[product] = o

    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={
        out_genus: f"Host_prediction_to_genus_m{MIN_SCORE}.csv",
        out_genome: f"Host_prediction_to_genome_m{MIN_SCORE}.csv",
        out_detail: "Detailed_output_by_tool.csv",
    },
    resources=Resources(cpus=16, memory=Size.GB(128), duration=Duration(hours=24)),
)
