# Pratama's vOTU definition (reproduction_map B6): `mmseqs easy-cluster --min-seq-id 0.95 -c 0.8`,
# with no --cov-mode, so MMseqs2's default of 0. The standard library's vOTU table uses cov-mode 1.
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::mmseqs2.env"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
out    = model.AddProduct(lib.GetType("viromics::votu_cluster_table"))


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    o = context.Output(out)
    threads = context.params.get("cpus", 16)
    context.ExecWithEnv(env=image, cmd=f"""
        mkdir -p mmseqs_tmp
        mmseqs easy-cluster {ifrozen.container} cl mmseqs_tmp --min-seq-id 0.95 -c 0.8 --threads {threads}
    """)
    context.LocalShell(f"cp cl_cluster.tsv {o.local}")
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={out: "votu_membership.tsv"},
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=6)),
)
