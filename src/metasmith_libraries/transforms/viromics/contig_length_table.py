# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# Antonio's step 5 cut the pooled FASTA at 1 kb and steps 11 and 13 cut it again
# at 5 kb and 10 kb, so each later tool saw a different set. Here the same numbers
# are one column of one table: seqkit stats per frozen contig, and every size cut
# is a predicate over it.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
out    = model.AddProduct(lib.GetType("viromics::contig_length_table"))


def protocol(context: ExecutionContext):
    context.Input(frozen)
    o = context.Output(out)
    context.external_shell.Exec(f"touch {o.external}")
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={out: "contig_lengths.tsv"},
    resources=Resources(cpus=2, memory=Size.GB(4), duration=Duration(hours=1)),
)
