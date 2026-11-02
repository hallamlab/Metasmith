# Pratama's vOTU definition (reproduction_map B6): `mmseqs easy-cluster --min-seq-id 0.95 -c 0.8`,
# with no --cov-mode, so MMseqs2's default of 0. The standard library's vOTU table uses cov-mode 1.
#
# Clusters the curated, host-trimmed set (Filtering 2 + CheckV trimming), not the raw frozen
# pool -- trimming precedes clustering in the paper, so a contig's post-trim length and
# sequence are what MMseqs2 sees.
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::mmseqs2.env"))
curated = model.AddRequirement(lib.GetType("e3::curated_candidate_virus"))
out     = model.AddProduct(lib.GetType("viromics::votu_cluster_table"))


def protocol(context: ExecutionContext):
    icurated = context.Input(curated)
    o = context.Output(out)
    threads = context.params.get("cpus", 16)
    context.ExecWithEnv(env=image, cmd=f"""
        mkdir -p mmseqs_tmp
        mmseqs easy-cluster {icurated.container} cl mmseqs_tmp --min-seq-id 0.95 -c 0.8 --threads {threads}
    """)
    context.LocalShell(f"cp cl_cluster.tsv {o.local}")
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=curated,
    output_signature={out: "votu_membership.tsv"},
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=6)),
)
