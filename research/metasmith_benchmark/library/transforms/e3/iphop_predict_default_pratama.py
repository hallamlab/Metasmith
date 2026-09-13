# Pratama runs `iphop predict` twice (reproduction_map B12). The standard iphop_predict.py is the
# augmented-database pass. This is the other one, against the shipped database, so a host call can be
# told apart from one made only because of this survey's own MAGs.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::iphop.env"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
db     = model.AddRequirement(lib.GetType("ref::iphop_db"))
out_genus  = model.AddProduct(lib.GetType("e3::host_prediction_genus_default"))
out_genome = model.AddProduct(lib.GetType("e3::host_prediction_genome_default"))

# iPHoP interpolates the cut into its output names, so it is pinned rather than exposed.
MIN_SCORE = 90


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    idb = context.Input(db)
    threads = context.params.get("cpus", 8)
    out_dir = Path("iphop_out")
    context.ExecWithEnv(env=image, cmd=f"""
        iphop predict --fa_file {ifrozen.container} --out_dir {out_dir} --db_dir {idb.container} \
            -t {threads} -m {MIN_SCORE}
    """)
    produced = {out_genus: out_dir / f"Host_prediction_to_genus_m{MIN_SCORE}.csv",
                out_genome: out_dir / f"Host_prediction_to_genome_m{MIN_SCORE}.csv"}
    missing = [p.name for p in produced.values() if not p.exists()]
    assert not missing, f"iphop predict wrote no {missing}"
    outs = {}
    for product, src in produced.items():
        outs[product] = context.Output(product)
        src.rename(outs[product].local)
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=all(o.local.exists() for o in outs.values()))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    resources=Resources(cpus=16, memory=Size.GB(128), duration=Duration(hours=24)),
)
