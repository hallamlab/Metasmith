# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# Antonio's step 10: the same 95/80 clustering at coverage mode 1. This table IS
# the vOTU definition -- there is no representative FASTA downstream of it, and a
# vOTU-level abundance is this membership joined to per-sample contig coverage
# through viromics::candidate_call_provenance.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
out    = model.AddProduct(lib.GetType("viromics::votu_cluster_table"))


def protocol(context: ExecutionContext):
    context.Input(frozen)
    o = context.Output(out)
    context.external_shell.Exec(f"touch {o.external}")
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={out: "votu_membership.tsv"},
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=6)),
)
