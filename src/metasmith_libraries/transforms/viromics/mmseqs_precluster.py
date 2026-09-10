# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# Antonio's step 6: MMseqs2 easy-cluster at 95% identity over 80% of the shorter
# sequence, coverage mode 0. A sibling file rather than a second instance of one
# transform, because the two clusterings differ only in a flag and a transform in
# this library is one invocation with its parameters fixed -- two files keep the
# flag visible in the DAG and let the two runs schedule in parallel.
#
# It emits membership only. The representative FASTA MMseqs2 also writes is
# deliberately not a product: consuming it would restart the filter chain, and it
# carries two traps besides (a trailing space appended to every header, and 19
# sidecar files beside it).
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
out    = model.AddProduct(lib.GetType("viromics::precluster_table"))


def protocol(context: ExecutionContext):
    context.Input(frozen)
    o = context.Output(out)
    context.external_shell.Exec(f"touch {o.external}")
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={out: "precluster_membership.tsv"},
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=6)),
)
