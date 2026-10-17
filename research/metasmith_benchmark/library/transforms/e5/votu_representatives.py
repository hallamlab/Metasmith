# Every vOTU's representative, cut from the curated set at >=5 kb -- the paper's published
# catalogue is exactly "all vOTUs with length >= 5 kb", before the island filter removes some
# of them. Distinct from votu_representatives_10kb_pratama, which cuts a >=10 kb slice of the
# raw frozen set for DRAM-v and is out of E3's scope.
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::seqkit.env"))
curated = model.AddRequirement(lib.GetType("e3::curated_candidate_virus"))
votus   = model.AddRequirement(lib.GetType("viromics::votu_cluster_table"), parents={curated})
out     = model.AddProduct(lib.GetType("e3::votu_representatives"))

MIN_LENGTH_BP = 5_000


def protocol(context: ExecutionContext):
    icurated = context.Input(curated)
    ivotus = context.Input(votus)
    o = context.Output(out)
    # Column 1 of MMseqs2's membership table is the representative.
    reps = sorted({line.split("\t")[0] for line in open(ivotus.local) if line.strip()})
    with open("representatives.txt", "w") as f:
        f.write("\n".join(reps) + "\n")
    Log.Info(f"{len(reps)} vOTUs")
    context.ExecWithEnv(env=image, cmd=f"""
        seqkit grep -f representatives.txt {icurated.container} | seqkit seq -m {MIN_LENGTH_BP} > {o.container}
    """)
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=curated,
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=2)),
)
