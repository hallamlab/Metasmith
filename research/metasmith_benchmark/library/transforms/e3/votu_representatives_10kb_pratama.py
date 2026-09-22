# Pratama restricts DRAM-v to vOTUs of at least 10 kb (reproduction_map B10). Pratama's manual
# curation before that cut has no equivalent here.
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::seqkit.env"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
votus  = model.AddRequirement(lib.GetType("viromics::votu_cluster_table"), parents={frozen})
out    = model.AddProduct(lib.GetType("e3::votu_representatives_10kb"))


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    ivotus = context.Input(votus)
    o = context.Output(out)
    # Column 1 of MMseqs2's membership table is the representative.
    reps = sorted({line.split("\t")[0] for line in open(ivotus.local) if line.strip()})
    with open("representatives.txt", "w") as f:
        f.write("\n".join(reps) + "\n")
    Log.Info(f"{len(reps)} vOTUs")
    context.ExecWithEnv(env=image, cmd=f"""
        seqkit grep -f representatives.txt {ifrozen.container} | seqkit seq -m 10000 > {o.container}
    """)
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=2)),
)
